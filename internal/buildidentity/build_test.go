package buildidentity

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestBuildIdentityEmbedding(t *testing.T) {
	for _, build := range []string{"dev", "test-build-123-dirty"} {
		t.Run(build, func(t *testing.T) {
			for _, binary := range []string{"c3-broker", "c3-claude-adapter"} {
				t.Run(binary, func(t *testing.T) {
					dest := filepath.Join(t.TempDir(), binary)
					args := []string{"build", "-o", dest}
					if build != "dev" {
						args = append(args, "-ldflags", "-s -w -X "+Symbol+"="+build)
					}
					args = append(args, "../../cmd/"+binary)
					if out, err := exec.Command("go", args...).CombinedOutput(); err != nil {
						t.Fatalf("build: %v\n%s", err, out)
					}
					got, err := Read(dest)
					if err != nil || got.Build != build {
						t.Fatalf("%+v %v", got, err)
					}
					if binary == "c3-claude-adapter" && got.Contract == "" {
						t.Fatal("missing embedded contract")
					}
				})
			}
		})
	}
}
func TestBuildIdentityUnreadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "adapter")
	if _, err := Read(path); err == nil {
		t.Fatal("missing accepted")
	}
	if err := os.WriteFile(path, []byte("not an adapter"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(path); err == nil {
		t.Fatal("arbitrary file accepted")
	}
}
