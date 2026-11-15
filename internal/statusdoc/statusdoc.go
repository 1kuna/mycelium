// Package statusdoc renders the status field inventory from its public Go types.
package statusdoc

//go:generate go run ../../cmd/statusdoc -root ../..

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"mycelium/internal/myce"
)

// Fields returns a deterministic inventory, visiting each reachable object once.
// It describes declared JSON fields; projection/redaction and presence are runtime
// behavior, documented separately rather than inferred from zero values.
func Fields() string {
	objects := map[reflect.Type]bool{}
	var wire func(reflect.Type) string
	wire = func(t reflect.Type) string {
		if t == reflect.TypeFor[json.RawMessage]() {
			return "JSON value"
		}
		if t == reflect.TypeFor[time.Time]() {
			return "timestamp"
		}
		if t.Implements(reflect.TypeFor[encoding.TextMarshaler]()) {
			return "string"
		}
		switch t.Kind() {
		case reflect.Pointer:
			return wire(t.Elem())
		case reflect.Slice, reflect.Array:
			if t.Elem().Kind() == reflect.Uint8 {
				return "base64 string"
			}
			return "array of " + wire(t.Elem())
		case reflect.Map:
			return "map of " + wire(t.Elem())
		case reflect.Struct:
			objects[t] = true
			return "[" + t.Name() + "](#fields-" + strings.ToLower(t.Name()) + ")"
		case reflect.String:
			return "string"
		case reflect.Bool:
			return "boolean"
		case reflect.Interface:
			return "JSON value"
		default:
			return "number"
		}
	}
	var fields func(reflect.Type) []string
	fields = func(t reflect.Type) []string {
		var out []string
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			tag := strings.Split(f.Tag.Get("json"), ",")
			if tag[0] == "-" {
				continue
			}
			if f.Anonymous && tag[0] == "" {
				embedded := f.Type
				if embedded.Kind() == reflect.Pointer {
					embedded = embedded.Elem()
				}
				if embedded.Kind() == reflect.Struct {
					out = append(out, fields(embedded)...)
					continue
				}
			}
			name := tag[0]
			if name == "" {
				name = f.Name
			}
			optional := ""
			for _, option := range tag[1:] {
				if option == "omitempty" || option == "omitzero" {
					optional = "?"
				}
			}
			out = append(out, "`"+name+optional+"`: "+wire(f.Type))
		}
		return out
	}
	for _, root := range []reflect.Type{reflect.TypeFor[myce.CompactStatus](), reflect.TypeFor[myce.Snapshot](), reflect.TypeFor[myce.StatusJobsPage]()} {
		wire(root)
	}
	rows := map[string]string{}
	for {
		before := len(rows)
		// fields discovers more objects; process a stable list on each pass.
		var pending []reflect.Type
		for t := range objects {
			if _, done := rows[t.Name()]; !done {
				pending = append(pending, t)
			}
		}
		for _, t := range pending {
			rows[t.Name()] = strings.Join(fields(t), "; ")
		}
		if len(rows) == before {
			break
		}
	}
	var names []string
	for name := range rows {
		names = append(names, name)
	}
	sort.Strings(names)
	var out strings.Builder
	out.WriteString("<!-- BEGIN GENERATED STATUS FIELDS -->\n\n")
	out.WriteString("Generated from `CompactStatus`, `Snapshot` and `StatusJobsPage`, including\nall reachable objects. Run `go generate ./internal/statusdoc` after changing\nthe types. Quick checks reject drift. `?` marks `omitempty`/`omitzero`, not\nguaranteed presence: status projection can redact or omit values. Arrays and\nmaps link to their element types; timestamps are JSON time strings.\n\n")
	out.WriteString("| Go object | Declared JSON fields and value types |\n| --- | --- |\n")
	for _, name := range names {
		fmt.Fprintf(&out, "| <a id=\"fields-%s\"></a>**%s** | %s |\n", strings.ToLower(name), name, rows[name])
	}
	out.WriteString("\n<!-- END GENERATED STATUS FIELDS -->")
	return out.String()
}
