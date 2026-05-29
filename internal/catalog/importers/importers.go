package importers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Draft struct {
	Source   string
	Importer string
	Path     string
	Name     string
	Size     int64
}

func Import(ctx context.Context, source string) (Draft, error) {
	if err := ctx.Err(); err != nil {
		return Draft{}, err
	}
	switch {
	case strings.HasPrefix(source, "hf://"):
		return Draft{}, fmt.Errorf("hf importer is not implemented in Phase 3")
	case strings.HasPrefix(source, "oci://"):
		return Draft{}, fmt.Errorf("oci importer is not implemented in Phase 3")
	case strings.HasPrefix(source, "file://"):
		return importLocal(ctx, strings.TrimPrefix(source, "file://"))
	default:
		return importLocal(ctx, source)
	}
}

func importLocal(ctx context.Context, path string) (Draft, error) {
	if err := ctx.Err(); err != nil {
		return Draft{}, err
	}
	clean, err := filepath.Abs(path)
	if err != nil {
		return Draft{}, err
	}
	info, err := os.Stat(clean)
	if err != nil {
		return Draft{}, err
	}
	if info.IsDir() {
		return Draft{}, fmt.Errorf("local model source %q is a directory", clean)
	}
	return Draft{
		Source:   path,
		Importer: "local",
		Path:     clean,
		Name:     filepath.Base(clean),
		Size:     info.Size(),
	}, nil
}
