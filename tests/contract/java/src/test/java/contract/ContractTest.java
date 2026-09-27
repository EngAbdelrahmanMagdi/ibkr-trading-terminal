package contract;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.SpecificationVersion;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Contract tests (Java): canonical JSON Schemas vs. golden fixtures.
 *
 * <p>For every message schema under contracts/schemas: valid fixtures validate, invalid fixtures are rejected (format
 * assertions enabled), every schema has both kinds of fixtures, and valid fixtures survive a parse, serialize, parse
 * round trip. Decimal strings convert to {@link BigDecimal} and back unchanged, and safe integers stay exact.
 */
class ContractTest {

  private static final String BASE_ID = "https://contracts.trading-terminal.invalid/schemas/";
  private static final List<String> ROOT_KEYWORDS = List.of("type", "allOf", "oneOf", "anyOf", "$ref");
  private static final Pattern DECIMAL_STRING = Pattern.compile("^-?[0-9]+\\.[0-9]+$");
  private static final long MAX_SAFE_INTEGER = 9007199254740991L;

  private static final Path CONTRACTS = dir("CONTRACTS_DIR", "../../../contracts");
  private static final Path FIXTURES = dir("FIXTURES_DIR", "../fixtures");
  private static final Path SCHEMAS_DIR = CONTRACTS.resolve("schemas");

  private static final JsonMapper MAPPER = JsonMapper.builder()
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
      .build();

  /** Relative schema path -> schema document. */
  private static final Map<String, JsonNode> SCHEMAS = loadSchemas();

  private static final SchemaRegistry REGISTRY = SchemaRegistry.withDefaultDialect(
      SpecificationVersion.DRAFT_2020_12,
      builder -> builder
          .schemaRegistryConfig(SchemaRegistryConfig.builder().formatAssertionsEnabled(true).build())
          // Every schema is registered up front under its $id; nothing is retrieved remotely.
          .schemas(schemaSources()));

  private static Path dir(String env, String fallback) {
    String value = System.getenv(env);
    return Path.of(value != null && !value.isBlank() ? value : fallback).toAbsolutePath().normalize();
  }

  private static String read(Path path) {
    try {
      return Files.readString(path, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static List<Path> list(Path dir, String suffix) {
    if (!Files.isDirectory(dir)) {
      return List.of();
    }
    try (Stream<Path> files = Files.walk(dir)) {
      return files.filter(p -> p.toString().endsWith(suffix)).sorted().toList();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Map<String, JsonNode> loadSchemas() {
    Map<String, JsonNode> schemas = new LinkedHashMap<>();
    for (Path path : list(SCHEMAS_DIR, ".schema.json")) {
      String rel = SCHEMAS_DIR.relativize(path).toString().replace('\\', '/');
      schemas.put(rel, MAPPER.readTree(read(path)));
    }
    return schemas;
  }

  private static Map<String, String> schemaSources() {
    Map<String, String> sources = new LinkedHashMap<>();
    SCHEMAS.keySet().forEach(rel -> sources.put(BASE_ID + rel, read(SCHEMAS_DIR.resolve(rel))));
    return sources;
  }

  private static List<String> messageSchemas() {
    return SCHEMAS.entrySet().stream()
        .filter(e -> ROOT_KEYWORDS.stream().anyMatch(k -> e.getValue().has(k)))
        .map(Map.Entry::getKey)
        .toList();
  }

  private static List<Path> fixtures(String rel, String kind) {
    return list(FIXTURES.resolve(rel.substring(0, rel.length() - ".schema.json".length())).resolve(kind), ".json");
  }

  private static Schema schema(String rel) {
    return REGISTRY.getSchema(SchemaLocation.of(BASE_ID + rel));
  }

  private static boolean isValid(String rel, String json) {
    return schema(rel).validate(json, InputFormat.JSON).isEmpty();
  }

  private static void strings(JsonNode node, List<String> out) {
    if (node.isString()) {
      out.add(node.stringValue());
    }
    for (JsonNode child : node) {
      strings(child, out);
    }
  }

  @Test
  void schemasArePresentAndIdsMatchLocations() {
    assertFalse(SCHEMAS.isEmpty(), "no schemas under " + SCHEMAS_DIR);
    SCHEMAS.forEach((rel, doc) -> {
      assertEquals("https://json-schema.org/draft/2020-12/schema", doc.get("$schema").stringValue(), rel);
      assertEquals(BASE_ID + rel, doc.get("$id").stringValue(), rel);
    });
  }

  @Test
  void formatAssertionsAreActive() {
    assertFalse(isValid("stream/heartbeat.schema.json", "{\"type\":\"heartbeat\",\"timestamp\":\"2026-13-45T25:61:61Z\"}"));
    assertFalse(isValid("watchlist/watchlist.schema.json", "{\"id\":\"not-a-uuid\",\"items\":[]}"));
  }

  @Test
  void everyMessageSchemaHasValidAndInvalidFixtures() {
    for (String rel : messageSchemas()) {
      assertFalse(fixtures(rel, "valid").isEmpty(), rel + ": no valid fixtures");
      assertFalse(fixtures(rel, "invalid").isEmpty(), rel + ": no invalid fixtures");
    }
  }

  @Test
  void validFixturesValidate() {
    List<String> failures = new ArrayList<>();
    for (String rel : messageSchemas()) {
      for (Path path : fixtures(rel, "valid")) {
        var errors = schema(rel).validate(read(path), InputFormat.JSON);
        if (!errors.isEmpty()) {
          failures.add(rel + ": " + path.getFileName() + " -> " + errors);
        }
      }
    }
    assertTrue(failures.isEmpty(), String.join("\n", failures));
  }

  @Test
  void invalidFixturesAreRejected() {
    List<String> failures = new ArrayList<>();
    for (String rel : messageSchemas()) {
      for (Path path : fixtures(rel, "invalid")) {
        if (isValid(rel, read(path))) {
          failures.add(rel + ": " + path.getFileName() + " was accepted");
        }
      }
    }
    assertTrue(failures.isEmpty(), String.join("\n", failures));
  }

  @Test
  void roundTripPreservesValidFixtures() {
    for (String rel : messageSchemas()) {
      for (Path path : fixtures(rel, "valid")) {
        JsonNode original = MAPPER.readTree(read(path));
        String serialized = MAPPER.writeValueAsString(original);
        assertEquals(original, MAPPER.readTree(serialized), path.toString());
        assertTrue(isValid(rel, serialized), path + ": re-serialized document is invalid");
      }
    }
  }

  @Test
  void decimalStringsAreExact() {
    int count = 0;
    for (String rel : messageSchemas()) {
      for (Path path : fixtures(rel, "valid")) {
        List<String> values = new ArrayList<>();
        strings(MAPPER.readTree(read(path)), values);
        for (String value : values) {
          if (DECIMAL_STRING.matcher(value).matches()) {
            count++;
            assertEquals(value, new BigDecimal(value).toPlainString(), path + ": " + value);
          }
        }
      }
    }
    if (count == 0) {
      fail("no decimal strings found in fixtures");
    }
  }

  @Test
  void maxSafeIntegerIsExact() {
    JsonNode quote = MAPPER.readTree(read(FIXTURES.resolve("stream/quote/valid/max-safe-volume.json")));
    assertTrue(quote.get("volume").isIntegralNumber());
    assertEquals(MAX_SAFE_INTEGER, quote.get("volume").longValue());
  }
}
