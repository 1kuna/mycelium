package myce

import (
	"fmt"
	"html/template"
	"math"
	"sort"
	"strings"
	"time"
)

// statusBefore consumes the scheduler projection, never recreates its policy.
func statusBefore(a, b *Job) bool {
	if rank(a.Spec.Priority) != rank(b.Spec.Priority) {
		return rank(a.Spec.Priority) < rank(b.Spec.Priority)
	}
	active := func(j *Job) int {
		if j.State == "running" || j.State == "stopping" {
			return 0
		}
		if j.State == "queued" {
			return 1
		}
		return 2
	}
	if active(a) != active(b) {
		return active(a) < active(b)
	}
	if a.QueueOrder != b.QueueOrder {
		if a.QueueOrder == 0 {
			return false
		}
		if b.QueueOrder == 0 {
			return true
		}
		return a.QueueOrder < b.QueueOrder
	}
	if !a.Queued.Equal(b.Queued) {
		return a.Queued.Before(b.Queued)
	}
	return a.ID < b.ID
}
func ShortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
func JobState(j *Job) string {
	state := displayState(j.State)
	if j.Attempt > 0 {
		state += fmt.Sprintf(" · attempt %d", j.Attempt)
	}
	return state
}
func JobAge(j *Job, now time.Time) string {
	start := j.StartedAt
	if start == nil {
		start = j.AssignedAt
	}
	if j.State == "queued" {
		since := j.WaitingSince
		if since.IsZero() {
			since = j.Queued
		}
		if since.IsZero() {
			return ""
		}
		return "waited " + elapsed(since, now)
	}
	if start == nil {
		return ""
	}
	if j.FinishedAt != nil {
		return "duration " + elapsed(*start, *j.FinishedAt)
	}
	if j.State == "running" || j.State == "stopping" {
		return "running for " + elapsed(*start, now)
	}
	return ""
}
func elapsed(start, end time.Time) string {
	return (max(0, end.Sub(start)) / time.Second * time.Second).String()
}

func activeJobs(jobs []*Job, priority string) []*Job {
	out := make([]*Job, 0, len(jobs))
	for _, job := range jobs {
		if job != nil && job.Spec.Priority == priority && !terminal(job.State) {
			out = append(out, job)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return statusBefore(out[i], out[j]) })
	return out
}

func machineJobs(jobs []*Job, machine string) []*Job {
	out := make([]*Job, 0, 1)
	for _, job := range jobs {
		if job != nil && job.Machine == machine && (job.State == "running" || job.State == "stopping") {
			out = append(out, job)
		}
	}
	return out
}

func machineModels(slots []ModelSlot, machine string) []ModelSlot {
	out := make([]ModelSlot, 0)
	for _, slot := range slots {
		if slot.Machine == machine {
			out = append(out, slot)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}

func machinePins(pins []ModelPin, machine string) []string {
	seen := make(map[string]bool)
	out := make([]string, 0)
	for _, pin := range pins {
		if pin.Assigned != machine && pin.Machine != machine {
			continue
		}
		if !seen[pin.Model] {
			seen[pin.Model] = true
			out = append(out, pin.Model)
		}
	}
	sort.Strings(out)
	return out
}

func failedJobs(jobs []*Job) []*Job {
	out := make([]*Job, 0)
	for _, job := range jobs {
		if job != nil && job.State == "failed" {
			out = append(out, job)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Queued.After(out[j].Queued) })
	if len(out) > 5 {
		out = out[:5]
	}
	return out
}

func recentFinishedJobs(jobs []*Job) []*Job {
	out := make([]*Job, 0)
	for _, job := range jobs {
		if job != nil && terminal(job.State) {
			out = append(out, job)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Queued.After(out[j].Queued) })
	if len(out) > 5 {
		out = out[:5]
	}
	return out
}

func lostMachines(machines []*Machine) []*Machine {
	out := make([]*Machine, 0)
	for _, machine := range machines {
		if machine != nil && machine.Gone {
			out = append(out, machine)
		}
	}
	return out
}

func checkpointWarnings(machines []*Machine) []*Machine {
	out := make([]*Machine, 0)
	for _, machine := range machines {
		if machine != nil && machine.CheckpointUnavailable {
			out = append(out, machine)
		}
	}
	return out
}

func bytesLabel(n int64) string {
	if n <= 0 {
		return "0 B"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	exp := int(math.Log(float64(n)) / math.Log(unit))
	if exp > 4 {
		exp = 4
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/math.Pow(unit, float64(exp)), "KMGT"[exp-1])
}

func copyPercent(done, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return math.Min(100, math.Max(0, float64(done)*100/float64(total)))
}

func displayState(state string) string {
	if state == "" {
		return "unknown"
	}
	return strings.ReplaceAll(state, "_", " ")
}

func stringsList(values ...string) []string { return values }

var statusPage = template.Must(template.New("status").Funcs(template.FuncMap{
	"activeJobs":         activeJobs,
	"shortID":            ShortID,
	"jobState":           JobState,
	"jobAge":             JobAge,
	"bytes":              bytesLabel,
	"checkpointWarnings": checkpointWarnings,
	"copyPercent":        copyPercent,
	"displayState":       displayState,
	"failedJobs":         failedJobs,
	"list":               stringsList,
	"lostMachines":       lostMachines,
	"machineJobs":        machineJobs,
	"machineModels":      machineModels,
	"machinePins":        machinePins,
	"recentFinishedJobs": recentFinishedJobs,
}).Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark">
<title>Mycelium status</title>
<style>
:root{color-scheme:light;--bg:#f5f7f6;--surface:#fff;--surface-2:#edf1ef;--ink:#17221d;--muted:#63716a;--line:#d7dfda;--accent:#176b52;--accent-soft:#dcefe8;--good:#18704f;--warn:#9b6410;--warn-soft:#fff4d9;--bad:#a43e3e;--bad-soft:#fde9e7;--shadow:0 1px 2px rgba(22,37,29,.06)}
*{box-sizing:border-box}html{background:var(--bg)}body{margin:0;color:var(--ink);background:var(--bg);font:14px/1.45 ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}code{font:12px/1.4 ui-monospace,SFMono-Regular,Consolas,monospace;color:var(--muted);overflow-wrap:anywhere}h1,h2,h3,p{margin-top:0}h1{margin-bottom:2px;font-size:22px;line-height:1.2;font-weight:680}h2{margin-bottom:12px;font-size:16px}h3{font-size:14px;margin-bottom:2px}.shell{width:min(1180px,100%);margin:0 auto;padding:20px}.topbar{display:flex;align-items:flex-start;justify-content:space-between;gap:20px;margin-bottom:18px}.eyebrow,.label{color:var(--muted);font-size:11px;font-weight:650;text-transform:uppercase}.eyebrow{margin-bottom:4px}.refresh-state{display:flex;align-items:center;gap:7px;color:var(--muted);font-size:12px;white-space:nowrap}.refresh-dot{width:7px;height:7px;border-radius:50%;background:var(--good)}.refresh-state[data-error="true"] .refresh-dot{background:var(--bad)}.refresh-state[data-error="true"]{color:var(--bad)}section{margin-top:22px}.section-head{display:flex;align-items:baseline;justify-content:space-between;gap:12px}.section-head p{color:var(--muted);font-size:12px}.alerts{display:grid;gap:8px;margin:0 0 18px}.alert{border:1px solid #ead8a6;border-radius:6px;background:var(--warn-soft);padding:10px 12px;color:#684711}.alert.bad{border-color:#efc9c5;background:var(--bad-soft);color:#782d2d}.fleet{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,320px),1fr));gap:12px}.card{min-width:0;border:1px solid var(--line);border-radius:7px;background:var(--surface);box-shadow:var(--shadow)}.machine{padding:14px}.card-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:13px}.identity{min-width:0}.identity h3{overflow-wrap:anywhere}.platform{color:var(--muted);font-size:12px}.badge{display:inline-flex;align-items:center;gap:5px;border-radius:999px;padding:3px 8px;background:var(--accent-soft);color:var(--good);font-size:11px;font-weight:650}.badge:before{content:"";width:6px;height:6px;border-radius:50%;background:currentColor}.badge.offline{background:var(--bad-soft);color:var(--bad)}.badge.leaving{background:var(--warn-soft);color:var(--warn)}.metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:1px;border:1px solid var(--line);border-radius:6px;overflow:hidden;background:var(--line);margin-bottom:13px}.metric{min-width:0;padding:8px;background:var(--surface-2)}.metric strong{display:block;margin-top:2px;font-size:15px;font-variant-numeric:tabular-nums}.detail{display:grid;grid-template-columns:96px minmax(0,1fr);gap:7px 10px;padding:7px 0;border-top:1px solid var(--line)}.detail:first-of-type{border-top:0}.value{min-width:0;overflow-wrap:anywhere}.chips{display:flex;flex-wrap:wrap;gap:5px}.chip{max-width:100%;border:1px solid var(--line);border-radius:4px;background:var(--surface-2);padding:2px 6px;font-size:12px;overflow-wrap:anywhere}.muted,.empty-inline{color:var(--muted)}.progress-list{display:grid;gap:8px}.progress-row{font-size:12px}.progress-copy{display:flex;justify-content:space-between;gap:8px;margin-bottom:4px}.track{height:5px;border-radius:4px;background:var(--surface-2);overflow:hidden}.fill{height:100%;background:var(--accent);width:var(--progress)}.queue{display:grid;gap:10px}.priority{overflow:hidden}.priority-head{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:10px 12px;background:var(--surface-2);border-bottom:1px solid var(--line)}.priority-head h3{margin:0;text-transform:capitalize}.count{color:var(--muted);font-size:12px}.job{display:grid;grid-template-columns:minmax(180px,1.6fr) minmax(90px,.7fr) minmax(90px,.7fr) minmax(180px,1.3fr);gap:12px;padding:11px 12px;border-bottom:1px solid var(--line);align-items:start}.job:last-child{border-bottom:0}.job-name{font-weight:650;overflow-wrap:anywhere}.state{font-weight:600;text-transform:capitalize}.reason{color:var(--muted);overflow-wrap:anywhere}.reason.important{color:var(--ink)}.usage-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,300px),1fr));gap:12px}.chart-card{padding:13px}.chart-meta{display:flex;align-items:baseline;justify-content:space-between;gap:10px;margin-bottom:8px}.chart-meta strong{font-size:17px;font-variant-numeric:tabular-nums}.chart-wrap{height:110px}.chart{display:block;width:100%;height:100%;overflow:visible}.chart .grid{stroke:var(--line);stroke-width:1}.chart .bar{fill:var(--accent)}.chart text{fill:var(--muted);font:9px ui-sans-serif,system-ui}.chart-empty{display:flex;align-items:center;justify-content:center;height:100%;border:1px dashed var(--line);border-radius:5px;color:var(--muted);font-size:12px}.samples{display:none}.empty{border:1px dashed var(--line);border-radius:7px;padding:18px;color:var(--muted);background:rgba(255,255,255,.4)}
.detail .label{min-width:0;overflow-wrap:anywhere}
@media(max-width:720px){.shell{padding:15px}.topbar{margin-bottom:14px}.job{grid-template-columns:minmax(0,1fr) auto}.job .machine-cell,.job .reason{grid-column:1/-1}.detail{grid-template-columns:96px minmax(0,1fr)}.metrics{grid-template-columns:repeat(3,minmax(0,1fr))}.metric{padding:7px}.metric strong{font-size:13px}}
@media(max-width:390px){.topbar{display:block}.refresh-state{margin-top:8px}.metrics{grid-template-columns:1fr}.job{grid-template-columns:1fr}.job .state{grid-row:auto}.job .machine-cell,.job .reason{grid-column:auto}}
@media(prefers-color-scheme:dark){:root{color-scheme:dark;--bg:#101512;--surface:#171e1a;--surface-2:#202923;--ink:#e8efeb;--muted:#9caaa2;--line:#344139;--accent:#63c6a1;--accent-soft:#183e31;--good:#71d2ad;--warn:#edbd64;--warn-soft:#3c2e16;--bad:#f08d86;--bad-soft:#40211f;--shadow:none}.alert{border-color:#614c27;color:#f2d393}.alert.bad{border-color:#693634;color:#f4aaa5}.empty{background:rgba(23,30,26,.5)}}
</style>
</head>
<body>
<main id="status-root" class="shell">
  <header class="topbar">
    <div><div class="eyebrow">Fleet overview</div><h1>Mycelium</h1><p class="muted">GPU work across the shared queue</p></div>
    <div class="refresh-state" data-refresh-state role="status" aria-live="polite"><span class="refresh-dot"></span><span data-refresh-copy>Live · every 5s</span></div>
  </header>

  {{$failed := failedJobs .Jobs}}{{$lost := lostMachines .Machines}}{{$nas := checkpointWarnings .Machines}}
  {{if or .Warning .HostHolds $failed $lost $nas}}<div class="alerts" aria-label="Recent problems">
    {{range .HostHolds}}<div class="alert" role="status">{{.}}</div>{{end}}
    {{if .Warning}}<div class="alert" role="alert">{{.Warning}}</div>{{end}}
    {{range $nas}}<div class="alert" role="alert"><strong>NAS unavailable on {{.ID}}.</strong> New checkpointed work will wait until access returns.</div>{{end}}
    {{range $lost}}<div class="alert bad" role="alert"><strong>{{.ID}} is offline.</strong> Its work may be reassigned.</div>{{end}}
    {{range $failed}}<div class="alert bad" role="alert"><strong>{{.Spec.Name}} failed.</strong> {{if .Reason}}{{.Reason}}{{else}}Check its logs for details.{{end}}</div>{{end}}
  </div>{{end}}

  <section aria-labelledby="fleet-title">
    <div class="section-head"><h2 id="fleet-title">Machines</h2><p>{{len .Machines}} in fleet</p></div>
    {{if .Machines}}<div class="fleet">
    {{range .Machines}}{{$machine := .}}{{$jobs := machineJobs $.Jobs .ID}}{{$models := machineModels $.LoadedModels .ID}}{{$pins := machinePins $.Pins .ID}}
      <article class="card machine">
        <div class="card-head"><div class="identity"><h3>{{.ID}}</h3><div class="platform">{{if .GPU}}{{.GPU}}{{else}}platform unknown{{end}}{{if .Device}} · {{.Device}}{{end}}</div></div>
          {{if .Gone}}<span class="badge offline">Offline</span>{{else if .Leaving}}<span class="badge leaving">Leaving</span>{{else}}<span class="badge">Online</span>{{end}}
        </div>
        <div class="metrics" aria-label="GPU memory">
          <div class="metric"><span class="label">Used</span><strong>{{printf "%.1f" .MemoryUsedGB}} GB</strong></div>
          <div class="metric"><span class="label">Free</span><strong>{{printf "%.1f" .FreeMemoryGB}} GB</strong></div>
	          <div class="metric"><span class="label">Reserved</span><strong>{{printf "%.1f" .ReservedMemoryGB}} GB</strong></div>
        </div>
        {{if .MemoryError}}<div class="alert bad">Memory unavailable: {{.MemoryError}}</div>{{end}}
		{{if .CacheError}}<div class="alert bad">Model cache unavailable: {{.CacheError}}</div>{{end}}
        <div class="detail"><div class="label">Current</div><div class="value">{{range $jobs}}<div><strong>{{.Spec.Name}}</strong> · {{jobState .}}{{if .Progress}} · progress {{.Progress}}{{end}}{{with jobAge . $.Now}} · {{.}}{{end}}</div>{{else}}<span class="empty-inline">Idle</span>{{end}}</div></div>
		<div class="detail"><div class="label">Loaded</div><div class="value chips">{{range $models}}<span class="chip">{{.Model}} · {{displayState .State}} · {{.InFlight}} calls{{if .Error}} · {{.Error}}{{end}}</span>{{else}}<span class="empty-inline">None</span>{{end}}</div></div>
        <div class="detail"><div class="label">Pinned</div><div class="value chips">{{range $pins}}<span class="chip">{{.}}</span>{{else}}<span class="empty-inline">None</span>{{end}}</div></div>
        <div class="detail"><div class="label">Cached</div><div class="value chips">{{range .CachedModels}}<span class="chip">{{.}}</span>{{else}}<span class="empty-inline">None</span>{{end}}</div></div>
        <div class="detail"><div class="label">Environments</div><div class="value chips">{{range .Environments}}<span class="chip">{{.}}</span>{{else}}<span class="empty-inline">None ready</span>{{end}}</div></div>
        {{if or .CopyReports .EnvironmentReports}}<div class="detail"><div class="label">Activity</div><div class="value progress-list">
          {{range .CopyReports}}<div class="progress-row"><div class="progress-copy"><span>Copy {{.Model}} · {{displayState .State}}</span><span>{{bytes .Bytes}} / {{bytes .Total}}</span></div><progress max="100" value="{{printf "%.1f" (copyPercent .Bytes .Total)}}">{{printf "%.0f" (copyPercent .Bytes .Total)}}%</progress>{{if .Error}}<div class="muted">{{.Error}}</div>{{end}}</div>{{end}}
          {{range .EnvironmentReports}}<div class="progress-row"><div class="progress-copy"><span>Build {{.Key}}</span><span>{{displayState .State}}{{if .BuildSeconds}} · {{printf "%.0f" .BuildSeconds}}s{{end}}</span></div>{{if .Error}}<div class="muted">{{.Error}}</div>{{end}}</div>{{end}}
        </div></div>{{end}}
      </article>
    {{end}}</div>{{else}}<div class="empty">No machines have joined the fleet.</div>{{end}}
  </section>

  <section aria-labelledby="queue-title">
    <div class="section-head"><h2 id="queue-title">Queue</h2><p>Grouped by scheduling priority</p></div>
    <div class="queue">
    {{range $priority := (list "production" "deadline" "experiment")}}{{$jobs := activeJobs $.Jobs $priority}}
      <div class="card priority"><div class="priority-head"><h3>{{$priority}}</h3><span class="count">{{len $jobs}} active</span></div>
      {{range $jobs}}<div class="job"><div><div class="job-name">{{.Spec.Name}}</div><code title="{{.ID}}">{{shortID .ID}}</code></div><div><div class="state">{{jobState .}}</div>{{with jobAge . $.Now}}<div class="muted">{{.}}</div>{{end}}</div><div class="machine-cell">{{if .Machine}}{{.Machine}}{{else}}Unassigned{{end}}</div><div class="reason {{if .Reason}}important{{end}}">{{if .Reason}}{{.Reason}}{{else if eq .State "running"}}Running now{{else}}Waiting for a compatible machine{{end}}</div></div>
      {{else}}<div class="job"><span class="muted">No {{$priority}} work.</span></div>{{end}}</div>
    {{end}}
	{{$finished := recentFinishedJobs .Jobs}}{{if $finished}}<div class="card priority"><div class="priority-head"><h3>Recent outcomes</h3><span class="count">{{len $finished}} shown</span></div>
	  {{range $finished}}<div class="job"><div><div class="job-name">{{.Spec.Name}}</div><code title="{{.ID}}">{{shortID .ID}}</code></div><div><div class="state">{{jobState .}}</div>{{with jobAge . $.Now}}<div class="muted">{{.}}</div>{{end}}</div><div class="machine-cell">{{if .Machine}}{{.Machine}}{{else}}Unassigned{{end}}</div><div class="reason">{{.Reason}}</div></div>{{end}}
	</div>{{end}}
    </div>
  </section>

  <section aria-labelledby="usage-title">
    <div class="section-head"><h2 id="usage-title">Last 7 days</h2><p>Observed usage samples</p></div>
    <div class="samples" aria-hidden="true">{{range .Usage.Daily}}<i data-day="{{.Date}}" data-project="{{.Project}}" data-machine="{{.Machine}}" data-gpu-hours="{{.GPUHours}}" data-busy-percent="{{.BusyPercent}}"></i>{{end}}</div>
    {{if or .Usage.Projects .Usage.Machines}}<div class="usage-grid">
	      {{range .Usage.Projects}}<article class="card chart-card" data-chart-kind="project" data-chart-name="{{.Name}}"><div class="chart-meta"><div><h3>{{.Name}}</h3><span class="muted">Project GPU time</span></div><strong>{{printf "%.1f" .GPUHours}}h</strong></div><div class="chart-wrap"><svg class="chart" viewBox="0 0 280 105" role="img" aria-label="Seven day usage for {{.Name}}"></svg></div></article>{{end}}
	      {{range .Usage.Machines}}<article class="card chart-card" data-chart-kind="machine" data-chart-name="{{.Name}}"><div class="chart-meta"><div><h3>{{.Name}}</h3><span class="muted">Machine busy</span></div><strong>{{printf "%.0f" .BusyPercent}}%</strong></div><div class="chart-wrap"><svg class="chart" viewBox="0 0 280 105" role="img" aria-label="Seven day usage for {{.Name}}"></svg></div></article>{{end}}
    </div>{{else}}<div class="empty">No usage samples yet. Charts appear after the first recorded interval.</div>{{end}}
  </section>
</main>
<script type="module">
(() => {
  const ns = "http://www.w3.org/2000/svg";
  function drawCharts(root) {
    const samples = [...root.querySelectorAll(".samples i")].map((el) => ({day:el.dataset.day,project:el.dataset.project,machine:el.dataset.machine,gpuHours:Number(el.dataset.gpuHours),busyPercent:Number(el.dataset.busyPercent)}));
    root.querySelectorAll("[data-chart-kind]").forEach((card) => {
      const kind = card.dataset.chartKind, name = card.dataset.chartName;
      const values = samples.filter((s) => s[kind] === name).slice(-7);
      const wrap = card.querySelector(".chart-wrap");
      if (!values.length) { wrap.innerHTML = '<div class="chart-empty">No samples for this period</div>'; return; }
      const metric = kind === "machine" ? "busyPercent" : "gpuHours";
      const ceiling = kind === "machine" ? 100 : Math.max(1, ...values.map((v) => v[metric]));
	      const svg = document.createElementNS(ns,"svg"); svg.setAttribute("class","chart"); svg.setAttribute("viewBox","0 0 280 105"); svg.setAttribute("role","img"); svg.setAttribute("aria-label",name + ", seven day " + (kind === "machine" ? "busy percent" : "GPU hours"));
      [15,45,75].forEach((y) => { const line=document.createElementNS(ns,"line"); line.setAttribute("class","grid"); line.setAttribute("x1","4"); line.setAttribute("x2","276"); line.setAttribute("y1",y); line.setAttribute("y2",y); svg.append(line); });
      const slot = 272 / values.length;
	      values.forEach((v,i) => { const h=Math.max(1,Math.min(78,(v[metric]/ceiling)*78)); const rect=document.createElementNS(ns,"rect"); rect.setAttribute("class","bar"); rect.setAttribute("x",String(5+i*slot)); rect.setAttribute("y",String(84-h)); rect.setAttribute("width",String(Math.max(5,slot-7))); rect.setAttribute("height",String(h)); rect.setAttribute("rx","2"); const title=document.createElementNS(ns,"title"); title.textContent=v.day + ": " + v[metric].toFixed(1) + (kind === "machine" ? "%" : " GPU-hours"); rect.append(title); svg.append(rect); const label=document.createElementNS(ns,"text"); label.setAttribute("x",String(5+i*slot+(slot-7)/2)); label.setAttribute("y","100"); label.setAttribute("text-anchor","middle"); const parsed=new Date(v.day); label.textContent=Number.isNaN(parsed.valueOf()) ? v.day : parsed.toLocaleDateString(undefined,{weekday:"short",timeZone:"UTC"}); svg.append(label); });
      wrap.replaceChildren(svg);
    });
  }
  drawCharts(document);
  let refreshing = false;
  async function refresh() {
    if (refreshing || document.hidden) return;
    refreshing = true;
    try {
      const response = await fetch("/", {cache:"no-store", credentials:"same-origin"});
	      if (!response.ok) throw new Error("HTTP " + response.status);
      const doc = new DOMParser().parseFromString(await response.text(), "text/html");
      const next = doc.querySelector("#status-root"), current = document.querySelector("#status-root");
      if (!next || !current) throw new Error("status fragment missing");
      current.replaceWith(next); drawCharts(document);
    } catch (error) {
      const state=document.querySelector("[data-refresh-state]"); if (state) { state.dataset.error="true"; const copy=state.querySelector("[data-refresh-copy]"); if (copy) copy.textContent="Refresh paused"; }
      console.warn("Mycelium status refresh failed", error);
    } finally { refreshing = false; }
  }
  window.setInterval(refresh, 5000);
})();
</script>
</body>
</html>`))
