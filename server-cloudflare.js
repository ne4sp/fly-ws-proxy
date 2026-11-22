import { connect } from "cloudflare:sockets";

const KEY = "CHANGE-ME"; // замените CHANGE-ME на свой ключ или задайте такую переменную KEY в настройках
const NAT64 = "2a01:4f9:c010:3f02:64::,2602:fc59:b0:64::";
const TIMEOUT = 5000; // самое оптимальное число после множественных проверок, но можете опустить значение
const STAGGER = 150;
const PARALLEL = 5;
const ROUTE_TTL = 600000;

const routes = new Map();
const dns = new Map();
let lastGood = null;

const enc = new TextEncoder();

function save(map, key, value) {
  if (map.size > 2000) map.clear();
  map.set(key, value);
}

function fresh(map, key) {
  const v = map.get(key);
  return v && v.until > Date.now() ? v : null;
}

function quiet(f) {
  try {
    const p = f();
    if (p && p.catch) p.catch(() => {});
  } catch (e) {}
}

function splitList(s) {
  return String(s).split(",").map((x) => x.trim()).filter(Boolean);
}

function normalizeIPv6Literal(host) {
  const value = String(host).trim();
  return value.startsWith("[") && value.endsWith("]")
    ? value.slice(1, -1)
    : value;
}

function isIPv6Literal(host) {
  const value = normalizeIPv6Literal(host);
  if (!value.includes(":") || value.includes("[") || value.includes("]")) return false;
  if (value.includes("%")) return false;

  const doubleColonCount = value.split("::").length - 1;
  if (doubleColonCount > 1) return false;

  const ipParts = value.split("::");
  if (doubleColonCount === 0 && ipParts.length !== 1) return false;

  const countGroups = (part) => {
    if (!part) return 0;
    const groups = part.split(":");
    let count = 0;
    for (const group of groups) {
      if (/^([0-9a-f]{1,4})$/i.test(group)) {
        count += 1;
      } else if (/^\d{1,3}(?:\.\d{1,3}){3}$/.test(group)) {
        const octets = group.split(".").map(Number);
        if (octets.some((octet) => octet > 255)) return -1;
        count += 2;
      } else {
        return -1;
      }
    }
    return count;
  };

  const leftCount = countGroups(ipParts[0]);
  if (leftCount < 0) return false;

  if (doubleColonCount === 0) return leftCount === 8;

  const rightCount = ipParts.length === 2 ? countGroups(ipParts[1]) : -1;
  if (rightCount < 0) return false;
  return leftCount + rightCount < 8;
}

async function resolve4(host) {
  if (isIPv6Literal(host) || /^\d{1,3}(\.\d{1,3}){3}$/.test(host)) return host;
  const hit = fresh(dns, host);
  if (hit) return hit.ip;
  const r = await fetch("https://cloudflare-dns.com/dns-query?type=A&name=" + encodeURIComponent(host), {
    headers: { accept: "application/dns-json" },
  });
  const a = ((await r.json()).Answer || []).find((x) => x.type === 1);
  if (!a) throw new Error("no A record");
  save(dns, host, { ip: a.data, until: Date.now() + Math.min(Math.max(a.TTL || 60, 60), 600) * 1000 });
  return a.data;
}

function toNat64(prefix, ip) {
  const [a, b, c, d] = ip.split(".").map(Number);
  if (prefix.split(":").length > 7) prefix = prefix.replace(/::$/, ":");
  return "[" + prefix + ((a << 8) | b).toString(16) + ":" + ((c << 8) | d).toString(16) + "]";
}

async function probe(target, host) {
  const t0 = Date.now();
  let s, timer;
  try {
    s = connect({ hostname: target, port: 80 });
    const r = s.readable.getReader();
    const w = s.writable.getWriter();
    const res = await Promise.race([
      w.write(enc.encode("HEAD / HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n")).then(() => r.read()),
      new Promise((_, rej) => { timer = setTimeout(() => rej(new Error("timeout")), TIMEOUT); }),
    ]);
    const line = res.done ? "closed without data" : new TextDecoder().decode(res.value).split("\r\n")[0];
    return { target, ms: Date.now() - t0, result: line };
  } catch (e) {
    return { target, ms: Date.now() - t0, error: String(e) };
  } finally {
    clearTimeout(timer);
    quiet(() => s.close());
  }
}

async function diagnose(host, prefixes) {
  const normalizedHost = normalizeIPv6Literal(host);
  const isV6 = isIPv6Literal(normalizedHost);
  const [direct, ip] = await Promise.all([
    probe(normalizedHost, normalizedHost),
    isV6
      ? Promise.resolve(normalizedHost)
      : resolve4(normalizedHost).catch((e) => ({ error: String(e) })),
  ]);
  if (typeof ip !== "string") return [direct, { resolve: ip.error }];
  const out = [];
  out.push(direct, isV6 ? { ipv6: ip } : { ipv4: ip });
  if (isV6) {
    const route = fresh(routes, normalizedHost);
    out.push({ cache: { route: route ? route.prefix || "direct" : null, lastGood } });
    return out;
  }
  for (let i = 0; i < prefixes.length; i += 5) {
    out.push(...(await Promise.all(prefixes.slice(i, i + 5).map((p) => probe(toNat64(p, ip), host)))));
  }
  const route = fresh(routes, host);
  out.push({ cache: { route: route ? route.prefix || "direct" : null, lastGood } });
  return out;
}

function relay(ws, host, port, prefixes) {
  const normalizedHost = normalizeIPv6Literal(host);
  const isV6 = isIPv6Literal(normalizedHost);
  const route = fresh(routes, normalizedHost);
  const pending = [];
  const live = new Set();
  let winner = null, closed = false, firstAt = 0;
  let queue = [], limit = 1, staggerTimer = null, ipP = null;

  const getIp = () => {
    if (isV6) return Promise.resolve(normalizedHost);
    return ipP || (ipP = resolve4(normalizedHost));
  };
  const direct = async () => [{ target: normalizedHost, prefix: null }];
  const nat64 = async () => {
    if (!prefixes.length || isV6) return [];
    const ip = await getIp();
    const order = new Set([route && route.prefix, lastGood, ...prefixes].filter((p) => prefixes.includes(p)));
    return [...order].map((p) => ({ target: toNat64(p, ip), prefix: p }));
  };
  const stages = route && route.prefix ? [nat64, direct] : [direct, nat64];
  if (!route && prefixes.length) getIp().catch(() => {});

  const tick = setInterval(() => {
    for (const a of live) {
      const since = a.opened ? firstAt && Math.max(a.start, firstAt) : a.start;
      if (since && Date.now() - since > TIMEOUT) fail(a, "timeout");
    }
  }, 100);

  const next = () => {
    clearTimeout(staggerTimer);
    if (closed || winner) return;
    if (queue.length && live.size < limit) {
      start(queue.shift());
      if (queue.length && live.size < limit) staggerTimer = setTimeout(next, STAGGER);
    } else if (!live.size && !queue.length) nextStage();
  };

  const nextStage = () => {
    const stage = stages.shift();
    if (!stage) {
      console.log("giveup", host);
      routes.delete(host);
      return finish();
    }
    stage().then(
      (q) => {
        queue = q;
        limit = port === 443 && q.length && q[0].prefix ? PARALLEL : 1;
        next();
      },
      (e) => {
        console.log("fail", host, String(e));
        if (!closed && !winner) nextStage();
      },
    );
  };

  const start = (t) => {
    const a = { target: t.target, prefix: t.prefix, start: Date.now(), opened: false, wq: Promise.resolve() };
    live.add(a);
    try {
      a.sock = connect({ hostname: a.target, port });
      a.reader = a.sock.readable.getReader();
      a.writer = a.sock.writable.getWriter();
      a.sock.opened.then(() => { a.opened = true; }, (e) => fail(a, String(e)));
    } catch (e) {
      return queueMicrotask(() => fail(a, String(e)));
    }
    for (const b of pending) write(a, b);
    read(a);
  };

  const write = (a, bytes) => {
    a.wq = a.wq.then(() => a.writer.write(bytes)).catch((e) => (a === winner ? finish() : fail(a, String(e))));
  };

  const read = async (a) => {
    let why = "closed without data";
    try {
      for (;;) {
        const { value, done } = await a.reader.read();
        if (done) break;
        if (!value || !value.byteLength) continue;
        if (!winner && !closed) win(a);
        if (winner !== a) return;
        ws.send(value);
      }
    } catch (e) {
      why = String(e);
    }
    a === winner ? finish() : fail(a, why);
  };

  const kill = (a) => {
    quiet(() => a.reader.cancel());
    quiet(() => a.writer.abort());
    quiet(() => a.sock.close());
  };

  const win = (a) => {
    winner = a;
    clearInterval(tick);
    clearTimeout(staggerTimer);
    live.delete(a);
    for (const o of live) kill(o);
    live.clear();
    pending.length = 0;
    save(routes, host, { prefix: a.prefix, until: Date.now() + ROUTE_TTL });
    if (a.prefix) lastGood = a.prefix;
    console.log("ok", host, a.target);
  };

  const fail = (a, why) => {
    if (!live.delete(a)) return;
    console.log("fail", a.target, why);
    kill(a);
    next();
  };

  const finish = () => {
    if (closed) return;
    closed = true;
    clearInterval(tick);
    clearTimeout(staggerTimer);
    for (const a of live) kill(a);
    live.clear();
    if (winner) kill(winner);
    pending.length = 0;
    quiet(() => ws.close(1000, "done"));
  };

  ws.binaryType = "arraybuffer";
  ws.addEventListener("message", (e) => {
    if (closed) return;
    const bytes = typeof e.data === "string" ? enc.encode(e.data) : new Uint8Array(e.data);
    if (winner) return write(winner, bytes);
    if (!firstAt) firstAt = Date.now();
    pending.push(bytes);
    for (const a of live) write(a, bytes);
  });
  ws.addEventListener("close", finish);
  ws.addEventListener("error", finish);

  nextStage();
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const arg = (name) => url.searchParams.get(name) || "";
    const key = env.KEY || KEY;
    const authed = !key.toUpperCase().startsWith("CHANGE-ME") && arg("k") === key;
    const prefixes = splitList(env.NAT64 ?? NAT64);

    if ((request.headers.get("Upgrade") || "").toLowerCase() !== "websocket") {
      if (!authed || !arg("test")) return new Response("Not found", { status: 404 });
      const custom = splitList(arg("nat64")).map((p) => p.replace(/^\[|\]$|\/96$/g, ""));
      const valid = custom.filter((p) => /^[0-9a-f:]{2,40}$/i.test(p));
      const report = await diagnose(arg("test").toLowerCase(), arg("nat64") ? valid.slice(0, 20) : prefixes);
      if (valid.length < custom.length) report.push({ skipped: custom.filter((p) => !valid.includes(p)) });
      return new Response(JSON.stringify(report, null, 2), { headers: { "content-type": "application/json" } });
    }

    if (!authed) return new Response("Forbidden", { status: 403 });
    const host = normalizeIPv6Literal(arg("h").toLowerCase());
    const port = parseInt(arg("p") || "443", 10);
    if (!/^[a-z0-9.\-:]{1,253}$/.test(host) || !(port > 0 && port < 65536) || port === 25) {
      return new Response("Bad Request", { status: 400 });
    }
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    relay(server, host, port, prefixes);
    return new Response(null, { status: 101, webSocket: client });
  },
};