package view

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu    sync.Mutex
	now   time.Time
	waits chan time.Duration
	fire  chan time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC), waits: make(chan time.Duration, 16), fire: make(chan time.Time)}
}
func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}
func (c *fakeClock) After(d time.Duration) <-chan time.Time { c.waits <- d; return c.fire }

// elapse waits for the poller to ask for a wait, checks it, and lets it pass.
func (c *fakeClock) elapse(t *testing.T, want func(time.Duration) bool, what string) time.Duration {
	t.Helper()
	select {
	case d := <-c.waits:
		if !want(d) {
			t.Fatalf("%s: poller waited %s", what, d)
		}
		c.advance(d)
		c.fire <- c.Now()
		return d
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: poller never waited", what)
		return 0
	}
}

// A redirect from the queue is never followed: the only request stays the
// default status read.
func TestUpstreamNeverFollowsRedirects(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	queue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.URL.RequestURI())
		mu.Unlock()
		http.Redirect(w, r, "/api/status?detail=full", http.StatusFound)
	}))
	defer queue.Close()
	u := UpstreamReader{URL: queue.URL, Token: "t", Clock: realClock{}, HTTP: &http.Client{Timeout: 5 * time.Second}}
	if _, err := u.read(context.Background()); err == nil {
		t.Fatal("a redirect must be an error, not a followed read")
	}
	if u.HTTP.CheckRedirect != nil {
		t.Fatal("the caller's client must not be modified")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0] != "/api/status" {
		t.Fatalf("upstream requests %v, want only /api/status", seen)
	}
}

func waitFrame(t *testing.T, ch <-chan []byte) Frame {
	t.Helper()
	select {
	case data := <-ch:
		var f Frame
		if err := json.Unmarshal(data, &f); err != nil {
			t.Fatal(err)
		}
		return f
	case <-time.After(5 * time.Second):
		t.Fatal("no frame")
		return Frame{}
	}
}

// End to end against a fake queue: the view only ever sends GET /api/status
// with no query (never detail=full or history), the browser cannot mutate
// anything, and other origins are refused.
func TestViewIsReadOnlyEndToEnd(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	queue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.RequestURI()+" auth="+fmt.Sprint(r.Header.Get("Authorization") == "Bearer secret-token")+fmt.Sprintf(" body=%d", r.ContentLength))
		mu.Unlock()
		fmt.Fprint(w, `{"now":"2026-11-06T12:00:00Z","machines":[{"id":"m1","last_seen":"2026-11-06T12:00:00Z"}],"jobs":[]}`)
	}))
	defer queue.Close()
	p := NewPoller(UpstreamReader{URL: queue.URL, Token: "secret-token", UserAgent: "test"}, false, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = p.Run(ctx) }()
	view := httptest.NewServer(Handler(p, "127.0.0.1:0"))
	defer view.Close()

	for _, tc := range []struct{ method, path string }{{"POST", "/events"}, {"DELETE", "/api/jobs"}} {
		method, path := tc.method, tc.path
		req, _ := http.NewRequest(method, view.URL+path, strings.NewReader(`{}`))
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: status %d, want 405", method, path, res.StatusCode)
		}
	}
	res, err := http.Get(view.URL + "/api/status")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("the view must not proxy queue routes; GET /api/status gave %d", res.StatusCode)
	}
	req, _ := http.NewRequest("GET", view.URL+"/events", nil)
	req.Host = "attacker.example"
	if res, err = http.DefaultClient.Do(req); err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Errorf("non-loopback Host on a loopback view: status %d, want 403", res.StatusCode)
	}
	for _, path := range []string{"/", "/data.js", "/scene.js", "/view.js", "/view.css"} {
		res, err := http.Get(view.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 200 || !strings.Contains(res.Header.Get("Content-Security-Policy"), "default-src 'none'") {
			t.Errorf("%s: status %d csp %q", path, res.StatusCode, res.Header.Get("Content-Security-Policy"))
		}
	}

	events, err := http.Get(view.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer events.Body.Close()
	sc := bufio.NewScanner(events.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	got := false
	for sc.Scan() {
		if data, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
			var f Frame
			if err := json.Unmarshal([]byte(data), &f); err != nil || len(f.Machines) != 1 || f.Upstream.State != "ok" {
				t.Fatalf("frame %q: %v", data, err)
			}
			got = true
			break
		}
	}
	if !got {
		t.Fatal("no frame on the event stream")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 {
		t.Fatal("queue saw no reads")
	}
	for _, s := range seen {
		if s != "GET /api/status auth=true body=0" {
			t.Errorf("unexpected upstream request %q", s)
		}
	}
}

// Every string in an anonymized frame is a generated label, a fixed
// vocabulary word or a timestamp; labels are stable and distinct.
func TestAnonymizeIsCompleteAndStable(t *testing.T) {
	secret := []string{"render-farm-7", "acme-billing", "customer-x-tier", "10.1.2.3", "/home/someone", "host-ab12", "sess-9", "quarterly-close", "ghost-box"}
	doc := func(now time.Time) []byte {
		return []byte(fmt.Sprintf(`{"now":%q,"machines":[
		  {"id":"render-farm-7","host_id":"host-ab12","session":"sess-9","last_seen":%q,"shared_host":{"interactive_reserve_gb":2},
		   "inventory_reason":"/home/someone","devices":[{"id":"render-farm-7/cuda:0","platform":"cuda","identity":"10.1.2.3","total_memory_gb":24,"free_memory_gb":8}]},
		  {"id":"ghost-box","gone":true,"last_seen":%q}],
		 "jobs":[
		  {"id":"job-acme-1","state":"running","machine":"render-farm-7","reason":"waiting on render-farm-7 at 10.1.2.3",
		   "priority_policy":{"tier":"customer-x-tier","index":0},
		   "spec":{"name":"quarterly-close","project":"acme-billing","class":"production","needs":{"platform":{"required":["weird-accel"]}}}},
		  {"id":"job-acme-2","state":"mystery_state","spec":{"name":"quarterly-close","project":"acme-billing"}}]}`, now.Format(time.RFC3339), now.Format(time.RFC3339), now.Format(time.RFC3339)))
	}
	clock := newFakeClock()
	a, proj := newAnonymizer(), newProjector()
	var frames []Frame
	for i := 0; i < 2; i++ {
		d, err := decodeStatus(doc(clock.Now()))
		if err != nil {
			t.Fatal(err)
		}
		f := proj.project(d, time.Second)
		a.frame(&f)
		frames = append(frames, f)
		clock.advance(time.Second)
	}
	data, _ := json.Marshal(frames)
	for _, s := range secret {
		if strings.Contains(string(data), s) {
			t.Fatalf("anonymized frame leaks %q: %s", s, data)
		}
	}
	allowed := regexp.MustCompile(`^(hypha-\d+|project-[a-z]+|job-\d+|j\d+|tier-\d+|production|(cuda|metal|xpu|rocm|cpu|fake|gpu):\d+|cuda|metal|xpu|rocm|cpu|fake|other|queued|running|stopping|paused|succeeded|failed|cancelled|ok|connecting|\d{4}-\d\d-\d\dT[\d:.]+Z)$`)
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case string:
			if x != "" && !allowed.MatchString(x) {
				t.Errorf("anonymized frame carries free-form string %q", x)
			}
		case []any:
			for _, e := range x {
				walk(e)
			}
		case map[string]any:
			for _, e := range x {
				walk(e)
			}
		}
	}
	var generic any
	_ = json.Unmarshal(data, &generic)
	walk(generic)
	f0, f1 := frames[0], frames[1]
	if f0.Machines[0].ID != f1.Machines[0].ID || f0.Jobs[0].ID != f1.Jobs[0].ID || f0.Jobs[0].Project != f1.Jobs[0].Project {
		t.Fatalf("labels must be stable across frames: %+v vs %+v", f0, f1)
	}
	if f0.Jobs[0].Machine != f0.Machines[0].ID {
		t.Fatalf("a job's machine label must match the machine's label")
	}
	if f0.Jobs[0].ID == f0.Jobs[1].ID || f0.Jobs[0].Name != f0.Jobs[1].Name {
		t.Fatalf("distinct jobs keep distinct IDs; the same real name keeps one label: %+v", f0.Jobs)
	}
	if !f0.Anonymized || f0.Jobs[0].Tier != "production" || f0.Jobs[1].State != "other" || f0.Jobs[0].Needs.Platform != "other" {
		t.Fatalf("vocabulary handling: %+v", f0.Jobs)
	}
	if len(f0.Machines) != 1 {
		t.Fatalf("a machine already gone when the view opened is not drawn: %+v", f0.Machines)
	}
}

func TestMissingAndMistypedFieldsDegrade(t *testing.T) {
	for name, body := range map[string]string{
		"empty":    `{}`,
		"mistyped": `{"now":"2026-11-06T12:00:00Z","machines":[{"id":"m","effective_cpu_cores":"lots","devices":{"x":1},"host_health":[]}],"jobs":[{"id":"j","state":"running","attempt":"two","spec":{"needs":{"platform":42}},"application_progress":{"completed":5,"total":0}}]}`,
	} {
		d, err := decodeStatus([]byte(body))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		f := newProjector().project(d, 0)
		if f.Machines == nil || f.Jobs == nil {
			t.Fatalf("%s: frame lists must be empty arrays, not null", name)
		}
		data, _ := json.Marshal(f)
		if strings.Contains(string(data), "null") {
			t.Fatalf("%s: frame has nulls: %s", name, data)
		}
		for _, j := range f.Jobs {
			if j.Name == "" || j.Progress != nil {
				t.Fatalf("%s: job without a name must fall back to its ID, and unknown progress stays absent: %+v", name, j)
			}
		}
	}
	if _, err := decodeStatus([]byte(`{"machines":`)); err == nil {
		t.Fatal("truncated JSON must be an error, not an empty fleet")
	}
}
