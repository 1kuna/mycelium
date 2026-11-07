/* Data layer for the live view: one event stream from `myce view`, frames
 * diffed into events. Nothing here draws; nothing here talks to the queue.
 * Frame fields are documented in internal/view/frame.go. */
(function (g) {
  'use strict';
  // Health marks for smoke tests and curious humans: frames received, script errors.
  var root = document.documentElement;
  root.dataset.frames = '0'; root.dataset.errors = '0';
  function fault(msg) { root.dataset.errors = String(+root.dataset.errors + 1); root.dataset.lastError = String(msg).slice(0, 200); }
  g.addEventListener('error', function (e) { fault(e.message || 'error'); });
  g.addEventListener('unhandledrejection', function (e) { fault(e.reason || 'rejection'); });
  document.addEventListener('securitypolicyviolation', function (e) { fault('csp ' + e.violatedDirective); });

  function connect(onFrame, onState) {
    // ?frames=N stops listening after N frames: a still picture for screenshots and smoke tests.
    var limit = parseInt(new URLSearchParams(location.search).get('frames') || '0', 10);
    var es = new EventSource('/events');
    es.onmessage = function (m) {
      var f;
      try { f = JSON.parse(m.data); } catch (e) { fault('bad frame'); return; }
      root.dataset.frames = String(+root.dataset.frames + 1);
      if (limit && +root.dataset.frames >= limit) es.close();
      onFrame(normalize(f));
    };
    es.onerror = function () { onState && onState('reconnecting'); };
    es.onopen = function () { onState && onState('open'); };
    return es;
  }

  // Defensive defaults so a sparse or older frame never throws while drawing.
  function normalize(f) {
    f.machines = (f.machines || []).filter(function (m) { return m && m.id; });
    f.jobs = (f.jobs || []).filter(function (j) { return j && j.id; });
    f.upstream = f.upstream || { state: 'connecting' };
    f.machines.forEach(function (m) { m.devices = m.devices || []; });
    f.jobs.forEach(function (j) { j.needs = j.needs || {}; j.name = j.name || j.id; j.project = j.project || ''; });
    return f;
  }

  function active(j) { return j.state === 'running' || j.state === 'stopping'; }
  function online(m) { return !m.gone && m.fresh !== false; }

  // The order the scheduler would pick from: tier, then queue position.
  function queueOrder(f) {
    return f.jobs.filter(function (j) { return j.state === 'queued'; }).sort(function (a, b) {
      return (b.production - a.production) || ((a.tier_index || 0) - (b.tier_index || 0)) || ((a.queue_order || 0) - (b.queue_order || 0));
    });
  }

  function counts(f) {
    var c = { running: 0, waiting: 0, done: 0, offline: 0, machines: f.machines.length };
    f.jobs.forEach(function (j) {
      if (active(j)) c.running++;
      else if (j.state === 'queued') c.waiting++;
      else if (j.state === 'succeeded') c.done++;
    });
    f.machines.forEach(function (m) { if (!online(m)) c.offline++; });
    return c;
  }

  function clamp(x) { return x < 0 ? 0 : x > 1 ? 1 : x || 0; }

  // How full a machine is with Mycelium work, 0..1: the most-held of GPU
  // memory, host memory and CPU reserved by running jobs. Status reports
  // memory held, not GPU utilisation.
  function load(m) {
    if (!online(m)) return 0;
    var cpu = m.effective_cpu_cores ? clamp(m.cpu_in_use_cores / m.effective_cpu_cores) : 0;
    var mem = m.total_memory_gb ? clamp(m.host_memory_in_use_gb / m.total_memory_gb) : 0;
    var gpu = 0, total = 0, held = 0;
    m.devices.forEach(function (d) { if (d.total_memory_gb > 0) { total += d.total_memory_gb; held += d.total_memory_gb - (d.free_memory_gb || 0); } });
    if (total) gpu = clamp(held / total);
    return Math.max(cpu, mem, gpu);
  }

  // Events derived from two consecutive frames.
  function diff(prev, next) {
    var ev = [];
    if (!prev) return ev;
    var pj = {}, pm = {};
    prev.jobs.forEach(function (j) { pj[j.id] = j; });
    prev.machines.forEach(function (m) { pm[m.id] = m; });
    next.machines.forEach(function (m) {
      var p = pm[m.id];
      if (!p) { ev.push({ type: 'machine-join', machine: m.id }); return; }
      if (!p.gone && m.gone) ev.push({ type: 'machine-lost', machine: m.id });
      else if (online(p) && !online(m)) ev.push({ type: 'machine-silent', machine: m.id });
      else if (!online(p) && online(m)) ev.push({ type: 'machine-back', machine: m.id });
    });
    next.jobs.forEach(function (j) {
      var p = pj[j.id];
      if (!p) {
        if (j.state === 'queued') ev.push({ type: j.production ? 'production-arrive' : 'arrive', job: j });
        else if (active(j)) ev.push({ type: 'place', job: j, machine: j.machine });
        return;
      }
      if (p.state === j.state && (p.machine || '') === (j.machine || '')) return;
      if (j.state === 'running' && p.state !== 'stopping') ev.push({ type: j.attempt > 1 || p.losses !== j.losses || p.preemption_count !== j.preemption_count ? 'resume' : 'place', job: j, machine: j.machine, from: p.machine });
      else if (j.state === 'stopping') ev.push({ type: 'preempt', job: j, machine: j.machine });
      else if (j.state === 'queued' && p.machine) ev.push({ type: (j.losses > p.losses ? 'requeue-lost' : 'requeue-preempted'), job: j, machine: p.machine });
      else if (j.state === 'succeeded') ev.push({ type: 'finish', job: j, machine: p.machine });
      else if (j.state === 'failed' || j.state === 'cancelled') ev.push({ type: 'fail', job: j, machine: p.machine });
    });
    return ev;
  }

  // One plain line per event for the ticker; hot lines are the story beats.
  function line(e) {
    var n = e.job && e.job.name;
    switch (e.type) {
      case 'production-arrive': return [n + ' is production · jumps to the front of the queue', true];
      case 'preempt': return [n + ' pauses on ' + e.machine + ' to make room', true];
      case 'machine-silent': return [e.machine + ' went quiet', true];
      case 'machine-lost': return [e.machine + ' dropped out · its jobs go back to the queue', true];
      case 'machine-back': return [e.machine + ' is back', true];
      case 'machine-join': return [e.machine + ' joined', true];
      case 'requeue-lost': return [n + ' goes back to the queue', true];
      case 'resume': return [n + ' resumes on ' + e.machine + (e.job.resumable ? ' from its checkpoint' : ''), true];
      case 'place': return [n + ' → ' + e.machine, false];
      case 'arrive': return [n + ' joins the queue', false];
      case 'finish': return [n + ' done', false];
      case 'fail': return [n + ' stopped (' + e.job.state + ')', true];
      default: return null;
    }
  }

  function clock(iso) {
    var d = new Date(iso);
    if (isNaN(d)) return '--:--:--';
    function p(x) { return (x < 10 ? '0' : '') + x; }
    return p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds());
  }

  g.MyceData = { connect: connect, normalize: normalize, diff: diff, queueOrder: queueOrder, counts: counts, load: load, line: line, clock: clock, active: active, online: online };
})(window);
