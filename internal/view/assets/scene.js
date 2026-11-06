/* The fleet as a fungal network. Waiting jobs gather as beads in the middle;
 * a thread (hypha) runs to each machine. A machine is a circle sized by its
 * capacity that fills from the bottom as it gets busier, with its running
 * jobs inside. Beads travel the threads when work is placed or returned; a
 * machine that drops out drains, its thread withers, and it shows a dashed x.
 * Layout is automatic for any number of machines. */
(function (g) {
  'use strict';
  var W = 1280, C = { x: 640, y: 370 };
  var L = { key: '', nodes: {} };
  var flights = [], buds = {}, beads = {}, slot = {}, pops = [], level = {}, grow = {}, life = {}, seen = false;

  function rng(s) { return function () { s = (s * 16807) % 2147483647; return (s - 1) / 2147483646; }; }
  function hash(str) { var h = 2166136261; for (var i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 16777619); } return (h >>> 0) % 2147483646 + 1; }
  function hypha(a, b, seed) {
    var r = rng(seed), n = 64, pts = [], dx = b.x - a.x, dy = b.y - a.y, len = Math.hypot(dx, dy) || 1, nx = -dy / len, ny = dx / len;
    var f1 = 1 + r() * 2, f2 = 3 + r() * 3, p1 = r() * 6, p2 = r() * 6, amp = Math.min(38, len * 0.08) + r() * 14;
    for (var i = 0; i <= n; i++) {
      var u = i / n, off = Math.sin(Math.PI * u) * amp * (Math.sin(u * Math.PI * f1 + p1) * 0.75 + Math.sin(u * Math.PI * f2 + p2) * 0.25);
      pts.push({ x: a.x + dx * u + nx * off, y: a.y + dy * u + ny * off });
    }
    return pts;
  }
  function capacity(m) {
    var c = m.total_memory_gb || 0;
    if (!m.unified_memory) m.devices.forEach(function (d) { if (!d.unified_memory) c += d.total_memory_gb || 0; });
    return c || 1;
  }

  // One ellipse up to 12 machines, two beyond; sizes relative to the largest machine.
  function layout(f) {
    var ids = f.machines.map(function (m) { return m.id; }), key = ids.join('|');
    if (key === L.key) return;
    var n = ids.length, rings = n > 12 ? [Math.ceil(n * 0.4), n - Math.ceil(n * 0.4)] : [n];
    var radii = n > 12 ? [[230, 128], [470, 250]] : [[390, 215]];
    var maxCap = 1; f.machines.forEach(function (m) { maxCap = Math.max(maxCap, capacity(m)); });
    var nodes = {}, i = 0;
    rings.forEach(function (count, ri) {
      var circ = Math.PI * (radii[ri][0] + radii[ri][1]), rmax = Math.min(58, circ / count * 0.36), rmin = Math.max(12, rmax * 0.45);
      var start = count === 1 ? 0 : -Math.PI / 2 + Math.PI / count + ri * Math.PI / count / 2;
      for (var k = 0; k < count; k++, i++) {
        var m = f.machines[i], a = start + 2 * Math.PI * k / count;
        var p = { x: C.x + Math.cos(a) * radii[ri][0], y: C.y + Math.sin(a) * radii[ri][1], a: a, compact: n > 12 };
        p.r = rmin + (rmax - rmin) * Math.sqrt(capacity(m) / maxCap);
        var old = L.nodes[m.id];
        p.hy = old && old.x === p.x && old.y === p.y ? old.hy : hypha(C, p, hash(m.id));
        nodes[m.id] = p;
      }
    });
    L = { key: key, nodes: nodes };
  }
  function at(pts, u) { u = Math.max(0, Math.min(1, u)); var f = u * (pts.length - 1), i = Math.min(pts.length - 2, Math.floor(f)), t = f - i; return { x: pts[i].x + (pts[i + 1].x - pts[i].x) * t, y: pts[i].y + (pts[i + 1].y - pts[i].y) * t }; }
  function lerp(a, b, u) { return { x: a.x + (b.x - a.x) * u, y: a.y + (b.y - a.y) * u }; }
  function ease(t) { return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2; }
  function easeOut(t) { return 1 - Math.pow(1 - t, 3); }
  function smooth(map, k, target, rate, dt) { if (!(k in map)) map[k] = target; map[k] += (target - map[k]) * (1 - Math.exp(-rate * dt)); return map[k]; }

  function alloc(mid, id) { var used = {}; for (var k in slot) if (slot[k].m === mid) used[slot[k].k] = 1; var s = 0; while (used[s]) s++; slot[id] = { m: mid, k: s }; }
  function inAt(nd, id) { var k = slot[id].k, a = k * 2.39996 - Math.PI / 2, d = k ? Math.min(nd.r - 8, 6 + Math.sqrt(k) * Math.min(17, nd.r * 0.36)) : 0; return { x: nd.x + Math.cos(a) * d, y: nd.y + Math.sin(a) * d }; }
  function bed(i) { var a = i * 2.39996, r = i ? 13 + Math.sqrt(i) * 14 : 0; return { x: C.x + Math.cos(a) * r, y: C.y + Math.sin(a) * r * 0.85 }; }
  var BED_MAX = 48; // beyond this the counts line carries the number; the bed shows "+N"

  function onFrame(f, events, api) {
    layout(f);
    var now = performance.now();
    if (!seen) { // first picture: place running work without animation
      seen = true;
      f.jobs.forEach(function (j) { if (api.D.active(j) && L.nodes[j.machine]) { alloc(j.machine, j.id); buds[j.id] = { born: -1e9 }; } });
      return;
    }
    events.forEach(function (e) {
      var j = e.job, nd = e.machine && L.nodes[e.machine];
      if (e.type === 'arrive' || e.type === 'production-arrive') beads[j.id] = e.type === 'production-arrive' ? { x: C.x, y: -20 } : { x: C.x + (Math.random() - 0.5) * 600, y: Math.random() < 0.5 ? -20 : 740 };
      else if ((e.type === 'place' || e.type === 'resume') && nd) {
        var b = beads[j.id] || C; alloc(e.machine, j.id);
        flights.push({ j: j, out: true, m: e.machine, sx: b.x, sy: b.y, born: now, dur: api.RM ? 1 : 1100 }); delete beads[j.id];
      } else if (e.type === 'requeue-lost' || e.type === 'requeue-preempted') {
        var from = buds[j.id] || nd || C;
        if (nd) flights.push({ j: j, out: false, m: e.machine, sx: from.x, sy: from.y, born: now, dur: api.RM ? 1 : 1300 });
        delete buds[j.id]; delete slot[j.id];
      } else if (e.type === 'finish' || e.type === 'fail') {
        if (buds[j.id] && buds[j.id].x != null) pops.push({ x: buds[j.id].x, y: buds[j.id].y, c: e.type === 'finish' ? api.color(j) : api.C.LOSS, born: now });
        delete buds[j.id]; delete slot[j.id];
      }
    });
    // Jobs that vanished from the frame (e.g. history trimmed) release their slots.
    var live = {}; f.jobs.forEach(function (j) { live[j.id] = j; });
    Object.keys(slot).forEach(function (id) { var j = live[id]; if (!j || !api.D.active(j) || j.machine !== slot[id].m) { delete slot[id]; delete buds[id]; } });
    Object.keys(beads).forEach(function (id) { if (!live[id] || live[id].state !== 'queued') delete beads[id]; });
  }

  function draw(f, dt, t, api) {
    var ctx = api.ctx, P = api.C, D = api.D, now = performance.now(), RM = api.RM;
    layout(f);
    ctx.lineCap = 'round'; ctx.lineJoin = 'round';
    var flying = {}; flights.forEach(function (fl) { flying[fl.j.id] = 1; });

    // threads
    f.machines.forEach(function (m) {
      var nd = L.nodes[m.id], on = D.online(m), gr = smooth(grow, m.id, on ? 1 : 0.3, 2.75, dt);
      var n = Math.max(2, Math.floor(nd.hy.length * gr));
      ctx.lineWidth = 1.8; ctx.strokeStyle = 'rgba(232,223,204,' + (0.16 + 0.36 * gr).toFixed(3) + ')';
      if (!on) ctx.setLineDash([2, 6]);
      ctx.beginPath(); ctx.moveTo(nd.hy[0].x, nd.hy[0].y); for (var i = 1; i < n; i++) ctx.lineTo(nd.hy[i].x, nd.hy[i].y); ctx.stroke();
      ctx.setLineDash([]);
    });

    // waiting beads in the middle, front of the queue at the centre
    var q = D.queueOrder(f);
    q.forEach(function (j, i) {
      if (flying[j.id] || i >= BED_MAX) return;
      var tg = bed(i), b = beads[j.id] || (beads[j.id] = { x: tg.x, y: tg.y }), k = 1 - Math.exp(-(RM ? 40 : 4.5) * dt);
      b.x += (tg.x - b.x) * k; b.y += (tg.y - b.y) * k; bead(ctx, b.x, b.y, j, t, api);
    });
    if (q.length > BED_MAX) api.text('+' + (q.length - BED_MAX) + ' more', C.x, C.y + (13 + Math.sqrt(BED_MAX) * 14) * 0.85 + 24, '12px ' + P.MONO, P.DIM, 'center');

    // machines
    f.machines.forEach(function (m) {
      var nd = L.nodes[m.id], on = D.online(m), lf = smooth(life, m.id, on ? 1 : 0, 5, dt);
      var load = smooth(level, m.id, D.load(m), 5, dt);
      ctx.fillStyle = P.BG; ctx.beginPath(); ctx.arc(nd.x, nd.y, nd.r, 0, Math.PI * 2); ctx.fill();
      if (load > 0.003) {
        var lvl = nd.y + nd.r - 2 * nd.r * load, wav = RM ? 0 : Math.min(2, nd.r * 0.05) * Math.min(1, load * 6), x, step = Math.max(2, nd.r / 12);
        ctx.save(); ctx.beginPath(); ctx.arc(nd.x, nd.y, nd.r - 1, 0, Math.PI * 2); ctx.clip();
        ctx.beginPath(); ctx.moveTo(nd.x - nd.r, nd.y + nd.r);
        for (x = -nd.r; x <= nd.r + step; x += step) ctx.lineTo(nd.x + x, lvl + Math.sin(x / 11 + t * 2.2) * wav);
        ctx.lineTo(nd.x + nd.r, nd.y + nd.r); ctx.closePath(); ctx.fillStyle = 'rgba(232,223,204,0.17)'; ctx.fill();
        ctx.beginPath();
        for (x = -nd.r; x <= nd.r + step; x += step) { var yy = lvl + Math.sin(x / 11 + t * 2.2) * wav; x === -nd.r ? ctx.moveTo(nd.x + x, yy) : ctx.lineTo(nd.x + x, yy); }
        ctx.strokeStyle = 'rgba(232,223,204,0.45)'; ctx.lineWidth = 1.2; ctx.stroke();
        ctx.restore();
      }
      ctx.beginPath(); ctx.arc(nd.x, nd.y, nd.r, 0, Math.PI * 2);
      ctx.lineWidth = 1.6; ctx.strokeStyle = 'rgba(232,223,204,' + (0.25 + 0.6 * lf).toFixed(3) + ')';
      if (!on) ctx.setLineDash([3, 4]); ctx.stroke(); ctx.setLineDash([]);
      f.jobs.forEach(function (j) {
        if (!D.active(j) || j.machine !== m.id || flying[j.id]) return;
        if (!slot[j.id]) alloc(m.id, j.id);
        var p = inAt(nd, j.id), b = buds[j.id] || (buds[j.id] = { born: now });
        var gr = RM ? 1 : Math.min(1, Math.max(0, (now - b.born) / 450)), e = easeOut(gr);
        b.x = nd.x + (p.x - nd.x) * e; b.y = nd.y + (p.y - nd.y) * e;
        ctx.globalAlpha = j.state === 'stopping' ? 0.35 + 0.35 * Math.abs(Math.sin(t * 9)) : 1;
        bead(ctx, b.x, b.y, j, t, api, nd.r * 0.3); ctx.globalAlpha = 1;
      });
      if (!on) api.text('×', nd.x, nd.y + nd.r * 0.2, Math.round(Math.max(14, nd.r * 0.6)) + 'px ' + P.SERIF, P.MUTED, 'center');
      label(ctx, m, nd, on, load, api);
    });

    flights = flights.filter(function (fl) {
      var nd = L.nodes[fl.m]; if (!nd) return false;
      var u = Math.min(1, (now - fl.born) / fl.dur), e = ease(u), p;
      if (fl.out) p = e < 0.12 ? lerp({ x: fl.sx, y: fl.sy }, C, e / 0.12) : at(nd.hy, (e - 0.12) / 0.88);
      else { p = e < 0.88 ? at(nd.hy, 1 - e / 0.88) : C; if (e < 0.08) p = lerp({ x: fl.sx, y: fl.sy }, p, e / 0.08); }
      bead(ctx, p.x, p.y, fl.j, t, api);
      if (u >= 1) {
        if (!fl.out) beads[fl.j.id] = { x: C.x, y: C.y };
        else if (buds[fl.j.id] == null) buds[fl.j.id] = { born: now };
      }
      return u < 1;
    });
    pops = pops.filter(function (s) {
      var a = (now - s.born) / 700; if (a > 1) return false;
      ctx.strokeStyle = s.c; ctx.globalAlpha = 1 - a; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(s.x, s.y, 5 + a * 16, 0, Math.PI * 2); ctx.stroke(); ctx.globalAlpha = 1; return true;
    });
  }

  function bead(ctx, x, y, j, t, api, cap) {
    var r = Math.min(cap || 12, api.size(j) * 0.95);
    ctx.fillStyle = api.color(j); ctx.beginPath(); ctx.arc(x, y, r, 0, Math.PI * 2); ctx.fill();
    if (j.production) {
      var pl = api.RM ? 0 : Math.sin(t * 6) * 1.5; ctx.strokeStyle = api.C.PROD; ctx.lineWidth = 1.5;
      ctx.beginPath(); ctx.arc(x, y, r + 5 + pl, 0, Math.PI * 2); ctx.stroke();
      ctx.globalAlpha *= 0.45; ctx.beginPath(); ctx.arc(x, y, r + 10 + pl, 0, Math.PI * 2); ctx.stroke(); ctx.globalAlpha = 1;
    }
  }

  // Name outside the node, away from its thread, with how full it is.
  function label(ctx, m, nd, on, load, api) {
    var P = api.C, cs = Math.cos(nd.a), sn = Math.sin(nd.a), d = nd.r + 16, side = Math.abs(cs) > 0.35;
    var nf = (nd.compact ? 11 : 13) + 'px ' + P.MONO, pct = on ? '  ' + Math.round(load * 100) + '%' : '';
    ctx.font = nf; var wn = ctx.measureText(m.id).width, wp = ctx.measureText(pct).width;
    var lx = nd.x + cs * d, ly = nd.y + sn * d + (side ? 5 : sn > 0 ? 12 : -2);
    var x0 = side ? (cs > 0 ? lx : lx - wn - wp) : lx - (wn + wp) / 2;
    x0 = Math.max(8, Math.min(W - 8 - wn - wp, x0));
    api.text(m.id, x0, ly, nf, on ? P.MUTED : P.LOSS, 'left');
    if (pct) api.text(pct, x0 + wn, ly, nf, P.DIM, 'left');
  }

  g.MyceScene = { onFrame: onFrame, draw: draw };
})(window);
