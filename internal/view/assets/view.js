/* Live view renderer. Frames come from MyceData (data.js); everything that
 * moves is derived by diffing consecutive frames, never from the scheduler. */
(function () {
  'use strict';
  var D = window.MyceData;
  var W = 1280, H = 720;
  var BG = '#12100c', BONE = '#e8dfcc', MUTED = 'rgba(232,223,204,0.55)', DIM = 'rgba(232,223,204,0.3)';
  var PROD = '#ffcf4a', AMBER = '#ffb000', LOSS = '#e0805f';
  var PIGMENTS = ['#9cc4ab', '#86a0dc', '#d27556', '#c39bd3', '#7fc7c9', '#b8c46a', '#e08fa0', '#a7a29a'];
  var SERIF = '"Iowan Old Style", "Palatino Linotype", Palatino, "URW Palladio L", P052, "TeX Gyre Pagella", Georgia, "DejaVu Serif", serif';
  var MONO = 'ui-monospace, "SF Mono", Menlo, "JetBrains Mono", "DejaVu Sans Mono", "Liberation Mono", Consolas, monospace';
  var RM = window.matchMedia && matchMedia('(prefers-reduced-motion: reduce)').matches;

  // ---- stage: 1280x720 units scaled to the window, crisp on any DPR ----
  var canvas = document.getElementById('stage'), ctx = canvas.getContext('2d');
  var S = { s: 1, ox: 0, oy: 0, dpr: 1 };
  function resize() {
    S.dpr = Math.min(3, window.devicePixelRatio || 1);
    canvas.width = Math.round(innerWidth * S.dpr); canvas.height = Math.round(innerHeight * S.dpr);
    canvas.style.width = innerWidth + 'px'; canvas.style.height = innerHeight + 'px';
    S.s = Math.min(innerWidth / W, innerHeight / H); S.ox = (innerWidth - W * S.s) / 2; S.oy = (innerHeight - H * S.s) / 2;
  }
  addEventListener('resize', resize); resize();
  function begin() {
    ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.fillStyle = BG; ctx.fillRect(0, 0, canvas.width, canvas.height);
    ctx.setTransform(S.dpr * S.s, 0, 0, S.dpr * S.s, S.dpr * S.ox, S.dpr * S.oy);
  }
  function text(s, x, y, font, color, align) { ctx.font = font; ctx.fillStyle = color; ctx.textAlign = align || 'left'; ctx.fillText(s, x, y); }

  // ---- shared state ----
  var frame = null, link = 'connecting', pig = {}, pigN = 0;
  function color(j) { if (j.production) return PROD; if (!(j.project in pig)) pig[j.project] = PIGMENTS[pigN++ % PIGMENTS.length]; return pig[j.project]; }
  function size(j) { return 5 + Math.sqrt((j.needs.memory_gb || 0) + (j.needs.host_memory_gb || 0)) * 0.8; }

  // ---- one-line ticker: story beats replace it at once, routine lines when it has been up a while ----
  var tick = null;
  function pushTicker(events, f) {
    var now = performance.now();
    events.forEach(function (e) {
      var l = D.line(e); if (!l) return;
      var age = tick ? now - tick.born : 1e9;
      if (l[1] ? !(tick && tick.hot && age < 900) : age > (tick && tick.hot ? 3200 : 1400))
        tick = { text: l[0], hot: l[1], born: now, time: D.clock(f.now) };
    });
  }
  function drawTicker(x, y) {
    var font = '14px ' + MONO, now = performance.now();
    ctx.font = font; var cw = ctx.measureText('0').width;
    if (!tick) { text(statusLine() || 'watching…', x, y, font, DIM); return; }
    var reveal = RM ? 1e9 : Math.floor((now - tick.born) / 1000 * 90), body = tick.text.slice(0, reveal), bx = x + cw * 10;
    text(tick.time, x, y, font, DIM);
    text(body, bx, y, font, tick.hot ? AMBER : 'rgba(255,176,0,0.72)');
    if (!RM && Math.floor(now / 300) % 2 === 0) { ctx.fillStyle = AMBER; ctx.fillRect(bx + cw * body.length + 3, y - 12, cw * 0.85, 15); }
  }

  // Where the data comes from, in a few words, only when it is not plainly live.
  function statusLine() {
    if (link === 'reconnecting') return 'view server unreachable · retrying';
    if (!frame) return 'connecting to the queue…';
    var u = frame.upstream || {};
    if (u.state === 'rate_limited') return 'queue busy · next look in ' + Math.ceil((u.retry_after_ms || u.next_poll_ms || 0) / 1000) + ' s';
    if (u.state === 'unreachable' || u.state === 'error') return 'queue unreachable · retrying in ' + Math.ceil((u.next_poll_ms || 0) / 1000) + ' s';
    if (u.state === 'connecting') return 'connecting to the queue…';
    return '';
  }

  // "mycelium   8 running · 3 waiting · 7 done · 1 offline"
  function drawCounts(x, y) {
    text('mycelium', x, y, 'italic 26px ' + SERIF, BONE);
    ctx.font = 'italic 26px ' + SERIF; var w = ctx.measureText('mycelium').width + 22;
    if (!frame) return;
    var c = D.counts(frame), font = '13px ' + MONO;
    var parts = [[c.running + ' running', MUTED], [c.waiting + ' waiting', MUTED], [c.done + ' done', MUTED]];
    if (c.offline) parts.push([c.offline + ' offline', LOSS]);
    var st = statusLine(); if (st && tick) parts.push([st, LOSS]);
    ctx.font = font;
    parts.forEach(function (p, i) {
      if (i) { text('·', x + w, y - 1, font, DIM); w += ctx.measureText('·').width + 10; }
      text(p[0], x + w, y - 1, font, p[1]); w += ctx.measureText(p[0]).width + 10;
    });
  }

  // ---- scene: the fleet picture (set by the chosen look) ----
  var Scene = window.MyceScene;
  if (!Scene) throw new Error('scene.js did not load');

  function onFrame(f) {
    var events = D.diff(frame, f);
    frame = f;
    pushTicker(events, f);
    Scene.onFrame(f, events, api);
  }
  var api = { ctx: ctx, text: text, color: color, size: size, RM: RM, D: D,
    C: { BG: BG, BONE: BONE, MUTED: MUTED, DIM: DIM, PROD: PROD, AMBER: AMBER, LOSS: LOSS, SERIF: SERIF, MONO: MONO } };

  var last = performance.now();
  function loop(now) {
    requestAnimationFrame(loop);
    var dt = Math.min(0.1, (now - last) / 1000); last = now;
    begin();
    if (frame) Scene.draw(frame, dt, now / 1000, api);
    drawCounts(40, 52);
    drawTicker(40, 694);
  }
  D.connect(onFrame, function (s) { link = s; });
  requestAnimationFrame(loop);
  document.documentElement.dataset.ready = '1';
})();
