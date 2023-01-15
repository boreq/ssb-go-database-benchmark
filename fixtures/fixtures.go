package fixtures

import (
	"math/rand"
	"os"
	"testing"
)

func Directory(t testing.TB) string {
	name, err := os.MkdirTemp("", "scuttlego-test")
	if err != nil {
		t.Fatal(err)
	}

	cleanup := func() {
		err := os.RemoveAll(name)
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(cleanup)

	return name
}

func RandomBytes(n int) []byte {
	r := make([]byte, n)
	_, err := rand.Read(r)
	if err != nil {
		panic(err)
	}
	return r
}
