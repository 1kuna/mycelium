package myce

import (
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestPlatformEncodingParity(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  Platform
		valid bool
	}{
		{`"cuda"`, Platform{Required: []string{"cuda"}}, true},
		{`"any"`, Platform{Any: true}, true},
		{`["xpu","metal"]`, Platform{Required: []string{"xpu", "metal"}}, true},
		{`{"required":["fake"]}`, Platform{Required: []string{"fake"}}, true},
		{`{"preferred":["metal"]}`, Platform{Preferred: []string{"metal"}}, true},
		{`{"any":true}`, Platform{Any: true}, true},
		{`null`, Platform{}, false}, {`true`, Platform{}, false}, {`42`, Platform{}, false},
		{`"unknown"`, Platform{}, false}, {`[]`, Platform{}, false}, {`[1]`, Platform{}, false},
		{`["unknown"]`, Platform{}, false}, {`{}`, Platform{}, false},
		{`{"required":"cuda"}`, Platform{}, false}, {`{"any":"true"}`, Platform{}, false},
		{`{"any":false}`, Platform{}, false}, {`{"any":true,"required":["cuda"]}`, Platform{}, false},
		{`{"any":true,"secret":123}`, Platform{}, false}, {`{"any":true,"any":true}`, Platform{}, false},
	} {
		t.Run(tc.input, func(t *testing.T) {
			for name, decode := range map[string]func([]byte, any) error{"json": json.Unmarshal, "yaml": yaml.Unmarshal} {
				var got Platform
				err := decode([]byte(tc.input), &got)
				if err == nil {
					s := spec("experiment")
					s.Needs.Platform = got
					err = s.Validate()
				}
				if (err == nil) != tc.valid {
					t.Fatalf("%s: valid=%v err=%v", name, tc.valid, err)
				}
				if tc.valid && !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("%s: got %#v", name, got)
				}
			}
		})
	}
}

func TestPlatformSubmissionParity(t *testing.T) {
	for _, input := range []string{`"fake"`, `["fake"]`, `"any"`, `{"required":["fake"]}`, `{"preferred":["fake"]}`, `{"any":true}`} {
		t.Run(input, func(t *testing.T) {
			q, _ := fixture(t)
			data, _ := json.Marshal(spec("experiment"))
			var fields map[string]any
			json.Unmarshal(data, &fields)
			fields["needs"].(map[string]any)["platform"] = json.RawMessage(input)
			data, _ = json.Marshal(fields)
			r := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(string(data)))
			r.Header.Set("Authorization", "Bearer test")
			w := httptest.NewRecorder()
			API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, r)
			if w.Code != 200 {
				t.Fatalf("submit: %d %s", w.Code, w.Body.String())
			}
			var fromYAML Spec
			if err := yaml.Unmarshal(data, &fromYAML); err != nil {
				t.Fatal(err)
			}
			if err := fromYAML.Validate(); err != nil {
				t.Fatal(err)
			}
			if _, err := q.Submit(fromYAML); err != nil {
				t.Fatal(err)
			}
		})
	}
}
