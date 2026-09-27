// Contract tests (Go): canonical JSON Schemas vs. golden fixtures.
//
// For every message schema under contracts/schemas: valid fixtures validate, invalid fixtures are
// rejected (format assertions enabled), every schema has both kinds of fixtures, and valid fixtures
// survive a decode -> encode -> decode round trip. Decimal strings and safe integers are checked for
// exactness without ever passing through float64.
package contract

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	baseID         = "https://contracts.trading-terminal.invalid/schemas/"
	maxSafeInteger = int64(9007199254740991)
)

var (
	decimalString = regexp.MustCompile(`^-?[0-9]+\.[0-9]+$`)
	rootKeywords  = []string{"type", "allOf", "oneOf", "anyOf", "$ref"}
)

// noFetchLoader refuses every external retrieval: all schemas must be registered up front.
type noFetchLoader struct{}

func (noFetchLoader) Load(url string) (any, error) {
	return nil, errors.New("schema retrieval is disabled: " + url)
}

type suite struct {
	schemasDir  string
	fixturesDir string
	docs        map[string]map[string]any // relative path -> schema document
	compiler    *jsonschema.Compiler
}

func dirFromEnv(t *testing.T, key, fallback string) string {
	t.Helper()
	dir := os.Getenv(key)
	if dir == "" {
		dir = fallback
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatalf("resolve %s: %v", key, err)
	}
	return abs
}

func decodeFile(t *testing.T, path string) any {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	v, err := jsonschema.UnmarshalJSON(f) // numbers decode as json.Number, never float64
	if err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return v
}

func newSuite(t *testing.T) *suite {
	t.Helper()
	s := &suite{
		schemasDir:  filepath.Join(dirFromEnv(t, "CONTRACTS_DIR", "../../../contracts"), "schemas"),
		fixturesDir: dirFromEnv(t, "FIXTURES_DIR", "../fixtures"),
		docs:        map[string]map[string]any{},
		compiler:    jsonschema.NewCompiler(),
	}
	s.compiler.DefaultDraft(jsonschema.Draft2020)
	s.compiler.AssertFormat()
	s.compiler.UseLoader(noFetchLoader{})

	err := filepath.WalkDir(s.schemasDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".schema.json") {
			return err
		}
		rel, _ := filepath.Rel(s.schemasDir, path)
		rel = filepath.ToSlash(rel)
		doc, ok := decodeFile(t, path).(map[string]any)
		if !ok {
			t.Fatalf("%s: schema is not an object", rel)
		}
		if id := doc["$id"]; id != baseID+rel {
			t.Errorf("%s: $id %v does not match its location", rel, id)
		}
		s.docs[rel] = doc
		return s.compiler.AddResource(baseID+rel, doc)
	})
	if err != nil {
		t.Fatalf("load schemas: %v", err)
	}
	if len(s.docs) == 0 {
		t.Fatalf("no schemas found under %s", s.schemasDir)
	}
	return s
}

func (s *suite) messageSchemas() []string {
	var out []string
	for rel, doc := range s.docs {
		for _, k := range rootKeywords {
			if _, ok := doc[k]; ok {
				out = append(out, rel)
				break
			}
		}
	}
	sort.Strings(out)
	return out
}

func (s *suite) fixtures(rel, kind string) []string {
	dir := filepath.Join(s.fixturesDir, strings.TrimSuffix(rel, ".schema.json"), kind)
	matches, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	sort.Strings(matches)
	return matches
}

func (s *suite) compile(t *testing.T, rel string) *jsonschema.Schema {
	t.Helper()
	sch, err := s.compiler.Compile(baseID + rel)
	if err != nil {
		t.Fatalf("compile %s: %v", rel, err)
	}
	return sch
}

func TestAllSchemasCompile(t *testing.T) {
	s := newSuite(t)
	for rel := range s.docs {
		s.compile(t, rel)
	}
}

func TestEveryMessageSchemaHasFixtures(t *testing.T) {
	s := newSuite(t)
	for _, rel := range s.messageSchemas() {
		if len(s.fixtures(rel, "valid")) == 0 || len(s.fixtures(rel, "invalid")) == 0 {
			t.Errorf("%s: needs at least one valid and one invalid fixture", rel)
		}
	}
}

func TestValidFixturesValidate(t *testing.T) {
	s := newSuite(t)
	for _, rel := range s.messageSchemas() {
		sch := s.compile(t, rel)
		for _, path := range s.fixtures(rel, "valid") {
			if err := sch.Validate(decodeFile(t, path)); err != nil {
				t.Errorf("%s: %s should be valid: %v", rel, filepath.Base(path), err)
			}
		}
	}
}

func TestInvalidFixturesAreRejected(t *testing.T) {
	s := newSuite(t)
	for _, rel := range s.messageSchemas() {
		sch := s.compile(t, rel)
		for _, path := range s.fixtures(rel, "invalid") {
			if err := sch.Validate(decodeFile(t, path)); err == nil {
				t.Errorf("%s: %s should be rejected", rel, filepath.Base(path))
			}
		}
	}
}

func TestRoundTripPreservesValidFixtures(t *testing.T) {
	s := newSuite(t)
	for _, rel := range s.messageSchemas() {
		sch := s.compile(t, rel)
		for _, path := range s.fixtures(rel, "valid") {
			original := decodeFile(t, path)
			encoded, err := json.Marshal(original)
			if err != nil {
				t.Fatalf("encode %s: %v", path, err)
			}
			again, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
			if err != nil {
				t.Fatalf("re-decode %s: %v", path, err)
			}
			if !reflect.DeepEqual(original, again) {
				t.Errorf("%s: round trip changed the document", path)
			}
			if err := sch.Validate(again); err != nil {
				t.Errorf("%s: re-encoded document is invalid: %v", path, err)
			}
		}
	}
}

func collectStrings(v any, out *[]string) {
	switch x := v.(type) {
	case string:
		*out = append(*out, x)
	case map[string]any:
		for _, e := range x {
			collectStrings(e, out)
		}
	case []any:
		for _, e := range x {
			collectStrings(e, out)
		}
	}
}

func TestDecimalStringsAreExact(t *testing.T) {
	s := newSuite(t)
	count := 0
	for _, rel := range s.messageSchemas() {
		for _, path := range s.fixtures(rel, "valid") {
			var values []string
			collectStrings(decodeFile(t, path), &values)
			for _, v := range values {
				if !decimalString.MatchString(v) {
					continue
				}
				count++
				r, ok := new(big.Rat).SetString(v)
				if !ok {
					t.Errorf("%s: %q is not a decimal", path, v)
					continue
				}
				scale := len(v) - strings.IndexByte(v, '.') - 1
				if got := r.FloatString(scale); got != v {
					t.Errorf("%s: decimal %q became %q", path, v, got)
				}
			}
		}
	}
	if count == 0 {
		t.Fatal("no decimal strings found in fixtures")
	}
}

func TestTypedQuoteKeepsExactValues(t *testing.T) {
	s := newSuite(t)
	raw, err := os.ReadFile(filepath.Join(s.fixturesDir, "stream/quote/valid/max-safe-volume.json"))
	if err != nil {
		t.Fatal(err)
	}
	var q struct {
		Last   *string `json:"last"`
		Volume *int64  `json:"volume"`
	}
	if err := json.Unmarshal(raw, &q); err != nil {
		t.Fatal(err)
	}
	if q.Volume == nil || *q.Volume != maxSafeInteger {
		t.Errorf("volume = %v, want %d", q.Volume, maxSafeInteger)
	}
	if q.Last == nil || *q.Last != "184.23" {
		t.Errorf("last = %v, want decimal string 184.23", q.Last)
	}
}
