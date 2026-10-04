package admission

import (
	"testing"
	"time"
)

func TestBoundedIndependentAdmission(t *testing.T) {
	l := New(1, time.Minute)
	now := time.Unix(100, 0)
	if ok, _ := l.Admit("peer", "submit", 1, 1, now); !ok {
		t.Fatal("initial admission")
	}
	if ok, capacity := l.Admit("peer", "submit", 1, 1, now); ok || capacity {
		t.Fatal("must rate limit")
	}
	if ok, _ := l.Admit("peer", "cancel", 1, 1, now); !ok {
		t.Fatal("cancel budget must be independent")
	}
	if ok, capacity := l.Admit("other", "submit", 1, 1, now); ok || !capacity {
		t.Fatal("must bound identities")
	}
	if ok, _ := l.Admit("other", "submit", 1, 1, now.Add(time.Minute)); !ok {
		t.Fatal("idle identity must expire")
	}
}
