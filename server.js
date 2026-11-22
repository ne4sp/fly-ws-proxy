import http from "node:http";
import net from "node:net";
import { WebSocketServer } from "ws";

// ==================== КОНФИГУРАЦИЯ ====================
const KEY = process.env.KEY || "CHANGE-ME";
const NAT64 = process.env.NAT64 || "2a01:4f9:c010:3f02:64::,2602:fc59:b0:64::";
const TIMEOUT = 5000;
const STAGGER = 150;
const PARALLEL = 5;
const ROUTE_TTL = 600000;
const PORT = parseInt(process.env.PORT || "8080", 10);

// ==================== УТИЛИТЫ ====================
const routes = new Map();
const dns = new Map();
let lastGood = null;

function save(map, key, value) {
  if (map.size > 2000) map.clear();
  map.set(key, value);
}

function fresh(map, key) {
  const v = map.get(key);
  return v && v.until > Date.now() ? v : null;
}

function splitList(s) {
  return String(s).split(",").map((x) => x.trim()).filter(Boolean);
}

async function resolve4(host) {
  if (net.isIPv4(host) || net.isIPv6(host)) return host;
  const hit = fresh(dns, host);
  if (hit) return hit.ip;
  const r = await fetch(
    "https://cloudflare-dns.com/dns-query?type=A&name=" + encodeURIComponent(host),
    { headers: { accept: "application/dns-json" } }
  );
  const a = ((await r.json()).Answer || []).find((x) => x.type === 1);
  if (!a) throw new Error("no A record");
  save(dns, host, {
    ip: a.data,
    until: Date.now() + Math.min(Math.max(a.TTL || 60, 60), 600) * 1000,
  });
  return a.data;
}

function toNat64(prefix, ip) {
  const [a, b, c, d] = ip.split(".").map(Number);
  if (prefix.split(":").length > 7) prefix = prefix.replace(/::$/, ":");
  return prefix + ((a << 8) | b).toString(16) + ":" + ((c << 8) | d).toString(16);
}

// ==================== ПРОБИРОВАНИЕ ЧЕРЕЗ NET ====================
function probe(target, host) {
  return new Promise((resolve) => {
    const t0 = Date.now();
    let done = false;
    const sock = net.connect({ host: target, port: 80 });
    const finish = (result) => {
      if (done) return;
      done = true;
      try { sock.destroy(); } catch {}
      resolve({ target, ms: Date.now() - t0, ...result });
    };
    sock.setTimeout(TIMEOUT);
    sock.on("connect", () => {
      sock.write(`HEAD / HTTP/1.1\r\nHost: ${host}\r\nConnection: close\r\n\r\n`);
    });
    sock.on("data", (buf) => {
      const line = buf.toString("utf8").split("\r\n")[0];
      finish({ result: line });
    });
    sock.on("timeout", () => finish({ error: "timeout" }));
    sock.on("error", (e) => finish({ error: String(e) }));
    sock.on("close", () => finish({ result: "closed without data" }));
  });
}

async function diagnose(host, prefixes) {
  const isV6 = net.isIPv6(host);
  const [direct, ip] = await Promise.all([
    probe(host, host),
    isV6
      ? Promise.resolve(host)
      : resolve4(host).catch((e) => ({ error: String(e) })),
  ]);
  if (typeof ip !== "string") return [direct, { resolve: ip.error }];
  const out = [direct, isV6 ? { ipv6: ip } : { ipv4: ip }];
  if (isV6) {
    const route = fresh(routes, host);
    out.push({ cache: { route: route ? route.prefix || "direct" : null, lastGood } });
    return out;
  }
  for (let i = 0; i < prefixes.length; i += 5) {
    out.push(
      ...(await Promise.all(
        prefixes.slice(i, i + 5).map((p) => probe(toNat64(p, ip), host))
      ))
    );
  }
  const route = fresh(routes, host);
  out.push({ cache: { route: route ? route.prefix || "direct" : null, lastGood } });
  return out;
}

// ==================== ОСНОВНОЙ РЕТРАНСЛЯТОР ====================
function relay(ws, host, port, prefixes) {
  const route = fresh(routes, host);
  const pending = [];
  const live = new Set();
  let winner = null, closed = false;
  let queue = [], limit = 1, staggerTimer = null, ipP = null;
  let tickInterval = null;
  let isAlive = true;
  const isV6 = net.isIPv6(host);
  const MAX_PENDING_BYTES = 1024 * 1024;
  let pendingBytes = 0;

  const getIp = () => {
    if (isV6) return Promise.resolve(host);
    return ipP || (ipP = resolve4(host));
  };
  const direct = async () => [{ target: host, prefix: null }];
  const nat64 = async () => {
    if (!prefixes.length || isV6) return [];
    const ip = await getIp();
    const order = new Set(
      [route && route.prefix, lastGood, ...prefixes].filter((p) => prefixes.includes(p))
    );
    return [...order].map((p) => ({ target: toNat64(p, ip), prefix: p }));
  };
  const stages = route && route.prefix ? [nat64, direct] : [direct, nat64];
  if (!route && prefixes.length) getIp().catch(() => {});

  tickInterval = setInterval(() => {
    for (const a of live) {
      if (Date.now() - a.start > TIMEOUT) fail(a, "timeout");
    }
  }, 100);

  // ⚠️ Heartbeat (требование Fly.io из PDF)
  const hbInterval = setInterval(() => {
    if (!isAlive) {
      try { ws.terminate(); } catch {}
      return finish();
    }
    isAlive = false;
    try { ws.ping(); } catch {}
  }, 30000);
  ws.on("pong", () => { isAlive = true; });

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
      }
    );
  };

  const start = (t) => {
    const a = {
      target: t.target,
      prefix: t.prefix,
      start: Date.now(),
      opened: false,
      wq: Promise.resolve(),
    };
    live.add(a);
    try {
      a.sock = net.connect({ host: a.target, port });
      a.sock.setNoDelay(true);
      a.sock.on("connect", () => { a.opened = true; });
      a.sock.on("error", (e) => fail(a, String(e)));
      a.sock.on("close", () => {
        if (a === winner) finish();
        else fail(a, "closed without data");
      });
    } catch (e) {
      return queueMicrotask(() => fail(a, String(e)));
    }
    for (const b of pending) write(a, b);
    read(a);
  };

  const write = (a, bytes) => {
    a.wq = a.wq
      .then(() => {
        if (!a.sock.destroyed) {
          const accepted = a.sock.write(bytes);
          if (!accepted) {
            const wsSocket = ws._socket;
            try { wsSocket?.pause(); } catch {}
            a.sock.once("drain", () => {
              try { wsSocket?.resume(); } catch {}
            });
          }
        }
      })
      .catch((e) => (a === winner ? finish() : fail(a, String(e))));
  };

  const read = (a) => {
    a.sock.on("data", (value) => {
      if (!winner && !closed) win(a);
      if (winner !== a) return;
      if (ws.readyState !== ws.OPEN) return finish();
      if (ws.bufferedAmount >= 1024 * 1024) a.sock.pause();
      try {
        ws.send(value, (error) => {
          if (error) return finish();
          if (ws.bufferedAmount < 256 * 1024) a.sock.resume();
        });
      } catch {
        finish();
      }
    });
  };

  const kill = (a) => {
    try { a.sock.destroy(); } catch {}
  };

  const win = (a) => {
    winner = a;
    clearInterval(tickInterval);
    clearTimeout(staggerTimer);
    live.delete(a);
    for (const o of live) kill(o);
    live.clear();
    pending.length = 0;
    pendingBytes = 0;
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
    clearInterval(tickInterval);
    clearInterval(hbInterval);
    clearTimeout(staggerTimer);
    for (const a of live) kill(a);
    live.clear();
    if (winner) kill(winner);
    pending.length = 0;
    pendingBytes = 0;
    try { ws.close(1000, "done"); } catch {}
  };

  ws.on("message", (data, isBinary) => {
    if (closed) return;
    const bytes = isBinary ? data : Buffer.from(String(data), "utf8");
    if (winner) return write(winner, bytes);
    if (pendingBytes + bytes.length > MAX_PENDING_BYTES) {
      try { ws.close(1008, "pending data limit exceeded"); } catch {}
      return;
    }
    pending.push(bytes);
    pendingBytes += bytes.length;
    for (const a of live) write(a, bytes);
  });
  ws.on("close", finish);
  ws.on("error", finish);

  nextStage();
}

// ==================== HTTP + WS СЕРВЕР ====================
const prefixes = splitList(NAT64);

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const arg = (n) => url.searchParams.get(n) || "";
  const key = process.env.KEY || KEY;
  const authed = !key.toUpperCase().startsWith("CHANGE-ME") && arg("k") === key;

  // health-check для Fly.io
  if (url.pathname === "/healthz") {
    res.writeHead(200); res.end("OK"); return;
  }
  if (!authed || !arg("test")) {
    res.writeHead(404); res.end("Not found"); return;
  }
  const custom = splitList(arg("nat64")).map((p) => p.replace(/^\[|\]$|\/96$/g, ""));
  const valid = custom.filter((p) => /^[0-9a-f:]{2,40}$/i.test(p));
  const testHost = arg("test").toLowerCase();
  const normalizedTestHost =
    testHost.startsWith("[") && testHost.endsWith("]")
      ? testHost.slice(1, -1)
      : testHost;
  const report = await diagnose(
    normalizedTestHost,
    arg("nat64") ? valid.slice(0, 20) : prefixes
  );
  if (valid.length < custom.length)
    report.push({ skipped: custom.filter((p) => !valid.includes(p)) });
  res.writeHead(200, { "content-type": "application/json" });
  res.end(JSON.stringify(report, null, 2));
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const arg = (n) => url.searchParams.get(n) || "";
  const key = process.env.KEY || KEY;
  const authed = !key.toUpperCase().startsWith("CHANGE-ME") && arg("k") === key;
  if (!authed) {
    socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
    socket.destroy();
    return;
  }
  const rawHost = arg("h").toLowerCase();
  const host =
    rawHost.startsWith("[") && rawHost.endsWith("]")
      ? rawHost.slice(1, -1)
      : rawHost;
  const port = parseInt(arg("p") || "443", 10);
  if (!/^[a-z0-9.\-:]{1,253}$/.test(host) || !(port > 0 && port < 65536) || port === 25) {
    socket.write("HTTP/1.1 400 Bad Request\r\n\r\n");
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit("connection", ws, req, host, port);
  });
});

wss.on("connection", (ws, req, host, port) => {
  relay(ws, host, port, prefixes);
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`WS proxy listening on :${PORT}`);
  if (KEY.toUpperCase().startsWith("CHANGE-ME")) {
    console.warn("⚠️  KEY не задан — авторизация отключена (все запросы будут 403)");
  }
});