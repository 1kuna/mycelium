package skills

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// Every file in the skill folder ships; a pattern that drops a reference would
// install a guide with dead links.
func TestEmbeddedSkillHasEveryFile(t *testing.T) {
	var want, got int
	if err := filepath.WalkDir("mycelium", func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			want++
			if _, err := Mycelium.ReadFile(filepath.ToSlash(p)); err != nil {
				t.Errorf("%s not embedded: %v", p, err)
			}
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	_ = fs.WalkDir(Mycelium, ".", func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			got++
		}
		return err
	})
	if _, err := os.Stat("mycelium/SKILL.md"); err != nil || got != want || want < 2 {
		t.Fatalf("embedded %d files, folder has %d: %v", got, want, err)
	}
}
