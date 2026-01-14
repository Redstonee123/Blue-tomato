import crypto from "crypto";
import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import http from "http";
import { WebSocketServer } from "ws";
import { spawn } from "child_process";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

const {
  PUSH_DOMAIN,
  PLAY_DOMAIN,
  APP_NAME,
  PUSH_KEY,
  PLAY_KEY,
  TOKEN_TTL_MINUTES = "1440",
  PORT = "3001",
  PLAY_PROTOCOL = "http",
} = process.env;

const rooms = new Map();
const liveProcesses = new Map();

function requireEnv() {
  const missing = [];
  if (!PUSH_DOMAIN) missing.push("PUSH_DOMAIN");
  if (!PLAY_DOMAIN) missing.push("PLAY_DOMAIN");
  if (!APP_NAME) missing.push("APP_NAME");
  if (!PUSH_KEY) missing.push("PUSH_KEY");
  if (!PLAY_KEY) missing.push("PLAY_KEY");
  return missing;
}

function buildAuthKey(path, key, ttlMinutes) {
  const expire = Math.floor(Date.now() / 1000) + ttlMinutes * 60;
  const base = `${path}-${expire}-0-0-${key}`;
  const hash = crypto.createHash("md5").update(base).digest("hex");
  return `${expire}-0-0-${hash}`;
}

function buildUrls(streamName) {
  const ttl = parseInt(TOKEN_TTL_MINUTES, 10) || 1440;
  const pushPath = `/${APP_NAME}/${streamName}`;
  const playPathHls = `/${APP_NAME}/${streamName}.m3u8`;
  const playPathFlv = `/${APP_NAME}/${streamName}.flv`;

  const pushAuth = buildAuthKey(pushPath, PUSH_KEY, ttl);
  const playAuthHls = buildAuthKey(playPathHls, PLAY_KEY, ttl);
  const playAuthFlv = buildAuthKey(playPathFlv, PLAY_KEY, ttl);

  return {
    pushUrl: `rtmp://${PUSH_DOMAIN}${pushPath}?auth_key=${pushAuth}`,
    playHls: `${PLAY_PROTOCOL}://${PLAY_DOMAIN}${playPathHls}?auth_key=${playAuthHls}`,
    playFlv: `${PLAY_PROTOCOL}://${PLAY_DOMAIN}${playPathFlv}?auth_key=${playAuthFlv}`,
  };
}

app.get("/api/health", (_req, res) => {
  const missing = requireEnv();
  if (missing.length) {
    res.status(500).json({ ok: false, missing });
    return;
  }
  res.json({ ok: true });
});

app.post("/api/live/start", (req, res) => {
  const missing = requireEnv();
  if (missing.length) {
    res.status(500).json({ error: "missing_env", missing });
    return;
  }

  const { streamName, title = "学习中", user = "匿名用户" } = req.body || {};
  if (!streamName) {
    res.status(400).json({ error: "streamName_required" });
    return;
  }

  const urls = buildUrls(streamName);
  const room = {
    streamName,
    title,
    user,
    startedAt: Date.now(),
    urls,
  };

  rooms.set(streamName, room);
  res.json(room);
});

app.post("/api/live/stop", (req, res) => {
  const { streamName } = req.body || {};
  if (!streamName) {
    res.status(400).json({ error: "streamName_required" });
    return;
  }
  rooms.delete(streamName);
  res.json({ ok: true });
});

app.get("/api/live/list", (_req, res) => {
  res.json(Array.from(rooms.values()));
});

app.get("/api/live/urls", (req, res) => {
  const missing = requireEnv();
  if (missing.length) {
    res.status(500).json({ error: "missing_env", missing });
    return;
  }
  const { streamName } = req.query;
  if (!streamName) {
    res.status(400).json({ error: "streamName_required" });
    return;
  }
  res.json(buildUrls(streamName));
});

const port = parseInt(PORT, 10) || 3001;
const server = http.createServer(app);

const wss = new WebSocketServer({ server, path: "/api/live/ingest" });

function startFfmpeg(pushUrl) {
  const ffmpegPath = process.env.FFMPEG_PATH || "ffmpeg";
  const args = [
    "-re",
    "-i",
    "pipe:0",
    "-r",
    "25",
    "-c:v",
    "libx264",
    "-preset",
    "veryfast",
    "-tune",
    "zerolatency",
    "-g",
    "50",
    "-keyint_min",
    "50",
    "-sc_threshold",
    "0",
    "-pix_fmt",
    "yuv420p",
    "-c:a",
    "aac",
    "-ar",
    "44100",
    "-f",
    "flv",
    pushUrl,
  ];
  return spawn(ffmpegPath, args, { stdio: ["pipe", "inherit", "inherit"] });
}

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const streamName = url.searchParams.get("streamName");
  if (!streamName) {
    ws.close(1008, "streamName_required");
    return;
  }

  const urls = buildUrls(streamName);
  const existing = liveProcesses.get(streamName);
  if (existing && !existing.killed) {
    existing.stdin.end();
    existing.kill("SIGINT");
  }
  const ffmpeg = startFfmpeg(urls.pushUrl);
  liveProcesses.set(streamName, ffmpeg);

  ffmpeg.on("error", (err) => {
    console.error("ffmpeg spawn error:", err.message || err);
  });
  ffmpeg.stdin.on("error", (err) => {
    if (err && err.code !== "EPIPE" && err.code !== "EOF") {
      console.error("ffmpeg stdin error:", err.message || err);
    }
  });
  ffmpeg.on("close", (code, signal) => {
    console.log(`ffmpeg closed for ${streamName} code=${code} signal=${signal}`);
    liveProcesses.delete(streamName);
  });

  ws.on("message", (data) => {
    if (!ffmpeg.stdin.writable) return;
    try {
      ffmpeg.stdin.write(data);
    } catch (err) {
      console.error("ffmpeg write error:", err && err.message ? err.message : err);
    }
  });

  ws.on("close", () => {
    if (ffmpeg && !ffmpeg.killed) {
      ffmpeg.stdin.end();
      ffmpeg.kill("SIGINT");
    }
  });

  ws.on("error", (err) => {
    console.error("WebSocket error:", err.message || err);
    if (ffmpeg && !ffmpeg.killed) {
      ffmpeg.stdin.end();
      ffmpeg.kill("SIGINT");
    }
  });
});

server.listen(port, () => {
  console.log(`Live server running on http://localhost:${port}`);
});
