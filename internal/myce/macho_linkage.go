package myce

import (
	"bufio"
	"context"
	"debug/macho"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"unicode"
)

const machoLibraryMissing = "payload_library_missing"

// MachOLinkage names payload-authored bytes, never an absolute worker workdir.
// Library is the install name embedded in a load command, not an OS error.
type MachOLinkage struct {
	Binary  string `json:"binary"`
	Library string `json:"library"`
}

func (d MachOLinkage) String() string {
	return fmt.Sprintf("packaged Mach-O %q requires missing non-system library %q; bundle and relink it or choose a compatible environment", d.Binary, d.Library)
}

func safeMachOLinkage(d *MachOLinkage) *MachOLinkage {
	if d == nil || !safeRelative(d.Binary) || !strings.HasPrefix(d.Library, "/") || path.Clean(d.Library) != d.Library {
		return nil
	}
	for _, s := range []string{d.Binary, d.Library} {
		if strings.ContainsAny(s, "\\") || strings.ContainsFunc(s, unicode.IsControl) {
			return nil
		}
	}
	copy := *d
	return &copy
}

// MachOLinkageCandidate selects packaged runtime code without opening data,
// archives or model weights. Executable bits catch script-launched helpers;
// dylib/so names catch transitive libraries and interpreter extension modules.
func MachOLinkageCandidate(name string, mode fs.FileMode) bool {
	return mode.IsRegular() && (mode.Perm()&0111 != 0 || strings.HasSuffix(name, ".dylib") || strings.HasSuffix(name, ".so") || strings.Contains(path.Base(name), ".so."))
}

// InspectMachOLinkage is read-only and uses metadata plus ReaderAt load-command
// reads, never symbol tables or binary contents. arch empty inspects every slice.
// Only confirmed ENOENT is a missing dependency; permissions/unknown stay unknown.
func InspectMachOLinkage(ctx context.Context, root, arch string) ([]MachOLinkage, error) {
	var findings []MachOLinkage
	err := filepath.WalkDir(root, func(file string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, file)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if rel == ".git" || rel == filepath.Join(".mycelium", "receipts") {
				return filepath.SkipDir
			}
			return nil
		}
		// Never follow symlinks to read files outside the selected payload.
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !MachOLinkageCandidate(rel, info.Mode()) {
			return nil
		}
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		findings = append(findings, InspectMachOFile(f, info.Size(), filepath.ToSlash(rel), arch)...)
		return f.Close()
	})
	return findings, err
}

// InspectMachOFile also lets source readers inspect an immutable payload member.
func InspectMachOFile(r io.ReaderAt, size int64, name, arch string) []MachOLinkage {
	var findings []MachOLinkage
	for _, library := range machoLibraries(r, size, arch) {
		if !strings.HasPrefix(library, "/") || strings.HasPrefix(library, "/usr/lib/") || strings.HasPrefix(library, "/System/Library/") {
			continue
		}
		if _, err := os.Stat(library); !errors.Is(err, fs.ErrNotExist) {
			continue
		}
		d := MachOLinkage{Binary: name, Library: library}
		if safeMachOLinkage(&d) != nil && !slices.Contains(findings, d) {
			findings = append(findings, d)
		}
	}
	return findings
}

// debug/macho.NewFile eagerly reads symbol/string tables. This narrow parser
// uses its ABI types/constants but reads only headers and dylib load commands,
// so a stripped helper and a multi-GB debug binary cost the same. Every offset
// and command length is bounded by the actual file/slice, not a guessed cap.
func machoLibraries(r io.ReaderAt, size int64, arch string) []string {
	var header [32]byte
	if size < 4 {
		return nil
	}
	if _, err := r.ReadAt(header[:4], 0); err != nil {
		return nil
	}
	magic := binary.BigEndian.Uint32(header[:4])
	if magic == macho.MagicFat || magic == 0xcafebabf {
		if size < 8 {
			return nil
		}
		if _, err := r.ReadAt(header[:8], 0); err != nil {
			return nil
		}
		n := int64(binary.BigEndian.Uint32(header[4:8]))
		stride := int64(20)
		if magic == 0xcafebabf {
			stride = 32
		}
		if n > (size-8)/stride {
			return nil
		}
		var libraries []string
		for i := int64(0); i < n; i++ {
			if _, err := r.ReadAt(header[:stride], 8+i*stride); err != nil {
				return nil
			}
			cpu := macho.Cpu(binary.BigEndian.Uint32(header[:4]))
			if !machoArchMatches(cpu, arch) {
				continue
			}
			offset, length := uint64(binary.BigEndian.Uint32(header[8:12])), uint64(binary.BigEndian.Uint32(header[12:16]))
			if stride == 32 {
				offset, length = binary.BigEndian.Uint64(header[8:16]), binary.BigEndian.Uint64(header[16:24])
			}
			if offset > uint64(size) || length > uint64(size)-offset {
				return nil
			}
			libraries = append(libraries, machoThinLibraries(io.NewSectionReader(r, int64(offset), int64(length)), int64(length), arch)...)
		}
		return libraries
	}
	return machoThinLibraries(r, size, arch)
}

func machoArchMatches(cpu macho.Cpu, arch string) bool {
	return arch == "" || arch == "arm64" && cpu == macho.CpuArm64 || arch == "amd64" && cpu == macho.CpuAmd64
}

func machoThinLibraries(r io.ReaderAt, size int64, arch string) []string {
	var h [28]byte
	if size < int64(len(h)) {
		return nil
	}
	if _, err := r.ReadAt(h[:], 0); err != nil {
		return nil
	}
	var order binary.ByteOrder = binary.BigEndian
	magic := order.Uint32(h[:4])
	if magic != macho.Magic32 && magic != macho.Magic64 {
		order = binary.LittleEndian
		magic = order.Uint32(h[:4])
		if magic != macho.Magic32 && magic != macho.Magic64 {
			return nil
		}
	}
	if !machoArchMatches(macho.Cpu(order.Uint32(h[4:8])), arch) {
		return nil
	}
	kind := macho.Type(order.Uint32(h[12:16]))
	if kind != macho.TypeExec && kind != macho.TypeDylib && kind != macho.TypeBundle {
		return nil
	}
	offset := int64(28)
	if magic == macho.Magic64 {
		offset = 32
	}
	n, length := int64(order.Uint32(h[16:20])), int64(order.Uint32(h[20:24]))
	if offset > size || length > size-offset || n > length/8 {
		return nil
	}
	end := offset + length
	var libraries []string
	for i := int64(0); i < n; i++ {
		var cmd [24]byte
		if offset > end-8 {
			return nil
		}
		if _, err := r.ReadAt(cmd[:8], offset); err != nil {
			return nil
		}
		code, length := order.Uint32(cmd[:4]), int64(order.Uint32(cmd[4:8]))
		if length < 8 || length > end-offset {
			return nil
		}
		// LC_LOAD_DYLIB, LC_REEXPORT_DYLIB, LC_LOAD_UPWARD_DYLIB.
		// LC_ID_DYLIB isn't a dependency; LC_LOAD_WEAK_DYLIB may be absent.
		if code == uint32(macho.LoadCmdDylib) || code == 0x8000001f || code == 0x80000023 {
			if length < int64(binary.Size(macho.DylibCmd{})) {
				return nil
			}
			if _, err := r.ReadAt(cmd[:], offset); err != nil {
				return nil
			}
			start := int64(order.Uint32(cmd[8:12]))
			if start < 24 || start >= length {
				return nil
			}
			// Bound the buffered reader to this command; do not touch symbols/data.
			name, err := bufio.NewReader(io.NewSectionReader(r, offset+start, length-start)).ReadString(0)
			if err != nil {
				return nil
			}
			libraries = append(libraries, strings.TrimSuffix(name, "\x00"))
		}
		offset += length
	}
	return libraries
}

func checkPayloadMachOLinkage(ctx context.Context, target ExecutionTarget, root string) error {
	if target.OS != "darwin" || root == "" {
		return nil
	}
	findings, _ := InspectMachOLinkage(ctx, root, target.Arch)
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(findings) == 0 {
		return nil
	}
	return &runtimeError{diagnostic: RuntimeCause{Operation: "launch", Class: machoLibraryMissing, Linkage: &findings[0]}}
}
