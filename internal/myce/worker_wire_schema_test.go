package myce

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	generate "github.com/invopop/jsonschema"
	validate "github.com/santhosh-tekuri/jsonschema/v6"
)

func generatedWorkerSchema(t *testing.T, value any) []byte {
	t.Helper()
	r := generate.Reflector{Anonymous: true, AllowAdditionalProperties: true, Lookup: func(typ reflect.Type) generate.ID {
		if typ == reflect.TypeOf(Job{}) {
			return "job-v1.schema.json"
		}
		return ""
	}}
	data := wireJSON(t, r.Reflect(value))
	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatal(err)
	}
	// Match encoding/json's nil collections, as in the Job schema generator.
	var nullable func(any)
	nullable = func(value any) {
		switch v := value.(type) {
		case map[string]any:
			if v["type"] == "array" {
				v["type"] = []string{"array", "null"}
			}
			if v["type"] == "object" {
				if _, ok := v["additionalProperties"].(map[string]any); ok {
					v["type"] = []string{"object", "null"}
				}
			}
			for _, child := range v {
				nullable(child)
			}
		case []any:
			for _, child := range v {
				nullable(child)
			}
		}
	}
	nullable(schema)
	name := reflect.TypeOf(value).Name()
	schema["title"] = "Mycelium worker " + name + " v1"
	schema["description"] = "Generated from Go JSON types by TestWorkerWireSchemas. Uses X-Myce-Start-Gate: host-launch-v3. Checkin/Reply negotiate optional attempt_protocol keyed-attempt-v1; omission retains current one-attempt compatibility. Keyed reports and directives require (id, attempt, session); assignments reference the unchanged job-v1 schema. M1 limits admission to one attempt per worker, not collection size. Cross-field key equality is enforced by the runtime. Omission is never keyed release proof."
	defs := schema["$defs"].(map[string]any)
	for _, definition := range []string{"Checkin", "Reply"} {
		if def, ok := defs[definition].(map[string]any); ok {
			props := def["properties"].(map[string]any)
			props["attempt_protocol"].(map[string]any)["const"] = keyedAttemptProtocol
			def["allOf"] = []any{map[string]any{
				"if":   map[string]any{"required": []string{"attempts"}},
				"then": map[string]any{"required": []string{"attempt_protocol"}},
			}}
		}
	}
	for _, definition := range []string{"AttemptReport", "AttemptDirective", "Result", "WorkerEvent"} {
		if def, ok := defs[definition].(map[string]any); ok {
			props := def["properties"].(map[string]any)
			for _, field := range []string{"id", "session", "machine"} {
				if prop, ok := props[field].(map[string]any); ok {
					prop["minLength"] = 1
				}
			}
			props["attempt"].(map[string]any)["minimum"] = 1
			if definition == "WorkerEvent" {
				// Empty long-poll replies carry no attempt, only the cursor.
				props["id"].(map[string]any)["minLength"] = 0
				props["attempt"].(map[string]any)["minimum"] = 0
				def["allOf"] = []any{map[string]any{
					"if":   map[string]any{"properties": map[string]any{"id": map[string]any{"minLength": 1}}, "required": []string{"id"}},
					"then": map[string]any{"required": []string{"session"}, "properties": map[string]any{"attempt": map[string]any{"minimum": 1}}},
				}}
			}
		}
	}
	return wireJSON(t, schema)
}

func compileWorkerWireSchema(t *testing.T, name string) *validate.Schema {
	t.Helper()
	compiler := validate.NewCompiler()
	compiler.AssertFormat()
	for _, resource := range []string{"job-v1.schema.json", name} {
		data, err := os.ReadFile(filepath.Join(schemaRoot, resource))
		if err != nil {
			t.Fatal(err)
		}
		var schema any
		if err = json.Unmarshal(data, &schema); err != nil {
			t.Fatal(err)
		}
		if err = compiler.AddResource("https://schemas.example/"+resource, schema); err != nil {
			t.Fatal(err)
		}
	}
	schema, err := compiler.Compile("https://schemas.example/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func TestWorkerWireSchemas(t *testing.T) {
	for name, value := range map[string]any{
		"worker-checkin-v1.schema.json": Checkin{},
		"worker-reply-v1.schema.json":   Reply{},
		"worker-result-v1.schema.json":  Result{},
		"worker-event-v1.schema.json":   WorkerEvent{},
	} {
		t.Run(name, func(t *testing.T) {
			generated := generatedWorkerSchema(t, value)
			path := filepath.Join(schemaRoot, name)
			if os.Getenv("MYCE_UPDATE_SCHEMAS") == "1" {
				if err := os.WriteFile(path, generated, 0644); err != nil {
					t.Fatal(err)
				}
			}
			actual, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(actual, generated) {
				t.Fatalf("worker schema drift: regenerate with MYCE_UPDATE_SCHEMAS=1 go test ./internal/myce -run '^TestWorkerWireSchemas$': %v", err)
			}
			compileWorkerWireSchema(t, name)
		})
	}
}

func TestAttemptProtocolWireSchemas(t *testing.T) {
	checkinSchema := compileWorkerWireSchema(t, "worker-checkin-v1.schema.json")
	replySchema := compileWorkerWireSchema(t, "worker-reply-v1.schema.json")
	resultSchema := compileWorkerWireSchema(t, "worker-result-v1.schema.json")
	eventSchema := compileWorkerWireSchema(t, "worker-event-v1.schema.json")
	q, clock := fixture(t)
	submit(t, q, spec("experiment"))
	m := machine("wire", "fake")
	for _, protocol := range []string{"", keyedAttemptProtocol} {
		c := Checkin{Machine: m, AttemptProtocol: protocol}
		validateWireValue(t, checkinSchema, c)
		reply, err := q.Checkin(c)
		if err != nil {
			t.Fatal(err)
		}
		validateWireValue(t, replySchema, reply)
		j := reply.Job
		if protocol != "" {
			j = reply.Attempts[0].Job
		}
		a := Execution{ID: j.ID, Attempt: j.Attempt, Progress: 4}
		if protocol == "" {
			c.Running = &a
		} else {
			c.Attempts = []AttemptReport{{Execution: a, Session: m.Session, Started: true}}
		}
		validateWireValue(t, checkinSchema, c)
		reply, err = q.Checkin(c)
		if err != nil {
			t.Fatal(err)
		}
		validateWireValue(t, replySchema, reply)
		validateWireValue(t, resultSchema, result(j, true))
		deadline := clock.Now().Add(10)
		validateWireValue(t, eventSchema, WorkerEvent{Execution: a, Session: m.Session, StopAt: &deadline})
	}
	validateWireValue(t, eventSchema, WorkerEvent{Cursor: "idle"})
	// Schemas must reject missing attempt keys rather than only describe them.
	for name, schema := range map[string]*validate.Schema{"result": resultSchema, "event": eventSchema} {
		if err := schema.Validate(map[string]any{"id": "job", "attempt": 1, "machine": m.ID, "success": true, "cursor": "stop", "grace_seconds": 0, "kill": true}); err == nil {
			t.Fatalf("%s schema accepted a missing session", name)
		}
	}
}
