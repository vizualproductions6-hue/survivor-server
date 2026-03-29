const express = require("express");
const cors = require("cors");
const OpenAI = require("openai");
require("dotenv").config({ override: true });
const { saveGame, loadGame, storeMemory, recallMemories, deleteGame, hasSupabaseConfig } = require("./db");

const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));

const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
if (!ANTHROPIC_KEY) { console.error("Missing ANTHROPIC_API_KEY"); process.exit(1); }
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const openai = OPENAI_API_KEY ? new OpenAI({ apiKey: OPENAI_API_KEY }) : null;

const CHAT_FETCH_TIMEOUT_MS = 45000;
const CHAT_FIRST_CHUNK_TIMEOUT_MS = 20000;
const CHAT_STREAM_IDLE_TIMEOUT_MS = 20000;

// Log Supabase config on startup (no secrets)
console.log("SUPABASE_URL set:", !!process.env.SUPABASE_URL);
console.log("SUPABASE_KEY set:", !!process.env.SUPABASE_KEY);
console.log("SUPABASE_SERVICE_ROLE_KEY set:", !!process.env.SUPABASE_SERVICE_ROLE_KEY);
console.log("SUPABASE_ENABLED:", hasSupabaseConfig);
console.log("OPENAI_API_KEY set:", !!OPENAI_API_KEY);

function randomId() {
  return Math.random().toString(36).slice(2, 10);
}

function timeoutError(label, ms) {
  return new Error(`${label} timed out after ${ms}ms`);
}

async function readWithTimeout(reader, ms, label) {
  return Promise.race([
    reader.read(),
    new Promise((_, reject) => setTimeout(() => reject(timeoutError(label, ms)), ms)),
  ]);
}

function writeAnthropicErrorEvent(res, message) {
  if (res.writableEnded || res.destroyed) return;
  res.write(`data: ${JSON.stringify({
    type: "overloaded_error",
    error: { type: "overloaded_error", message },
  })}\n\n`);
}

function createUpstreamSignal(controller, ms) {
  if (typeof AbortSignal !== "undefined" && typeof AbortSignal.any === "function" && typeof AbortSignal.timeout === "function") {
    return AbortSignal.any([controller.signal, AbortSignal.timeout(ms)]);
  }
  const timer = setTimeout(() => controller.abort(timeoutError("Anthropic request", ms)), ms);
  controller.signal.addEventListener("abort", () => clearTimeout(timer), { once: true });
  return controller.signal;
}

function requireOpenAI() {
  if (!openai) {
    throw new Error("OpenAI is not configured on this server");
  }
  return openai;
}

async function createEmbeddingForText(text) {
  const input = String(text || "").trim();
  if (!input) return null;
  const client = requireOpenAI();
  const response = await client.embeddings.create({
    model: "text-embedding-3-small",
    input: input.slice(0, 8000),
  });
  return response?.data?.[0]?.embedding || null;
}

// The only route that matters — proxy to Anthropic with model routing
app.post("/api/chat", async (req, res) => {
  const { messages, system, maxTokens, callType } = req.body;
  const requestId = randomId();
  const startedAt = Date.now();

  if (!Array.isArray(messages) || !messages.length || !system) {
    return res.status(400).json({ error: "Missing messages or system prompt" });
  }

  // Model routing: Haiku for cheap stuff, Sonnet for everything else
  const SONNET = "claude-sonnet-4-6";
  const HAIKU = "claude-haiku-4-5-20251001";
  const model = (callType === "anchor" || callType === "morning" || callType === "transition")
    ? HAIKU : SONNET;

  console.log(`[/api/chat:${requestId}] start type=${callType || "game"} model=${model} msgs=${messages.length} systemChars=${String(system).length}`);

  const upstreamController = new AbortController();
  let clientClosed = false;
  let upstreamReader = null;
  let streamStarted = false;

  const onRequestAborted = () => {
    clientClosed = true;
    upstreamController.abort(new Error("Client disconnected"));
  };
  const onResponseClose = () => {
    // `req.close` fires once the request body is fully read, which is too early for
    // a streaming response. Only treat response close as a disconnect if we did not
    // finish writing the stream normally.
    if (!res.writableEnded) {
      clientClosed = true;
      upstreamController.abort(new Error("Client disconnected"));
    }
  };
  req.on("aborted", onRequestAborted);
  res.on("close", onResponseClose);

  try {
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "anthropic-version": "2023-06-01",
        "x-api-key": ANTHROPIC_KEY,
      },
      body: JSON.stringify({
        model,
        max_tokens: maxTokens || 350,
        cache_control: { type: "ephemeral" },
        system,
        messages,
        stream: true,
      }),
      signal: createUpstreamSignal(upstreamController, CHAT_FETCH_TIMEOUT_MS),
    });

    if (!response.ok) {
      const err = await response.text();
      console.error(`[/api/chat:${requestId}] upstream ${response.status}: ${err.slice(0, 300)}`);
      res.status(response.status).send(err);
      return;
    }

    if (!response.body) {
      throw new Error("Anthropic returned no response body");
    }

    const reader = response.body.getReader();
    upstreamReader = reader;
    const decoder = new TextDecoder();

    // Do not start the browser stream until Anthropic has actually produced bytes.
    const firstChunk = await readWithTimeout(reader, CHAT_FIRST_CHUNK_TIMEOUT_MS, "Anthropic first chunk");
    if (firstChunk.done || !firstChunk.value) {
      throw new Error("Anthropic stream ended before sending content");
    }

    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    if (typeof res.flushHeaders === "function") res.flushHeaders();

    streamStarted = true;
    res.write(decoder.decode(firstChunk.value, { stream: true }));
    console.log(`[/api/chat:${requestId}] first chunk in ${Date.now() - startedAt}ms`);

    while (true) {
      const { done, value } = await readWithTimeout(reader, CHAT_STREAM_IDLE_TIMEOUT_MS, "Anthropic stream");
      if (done) break;
      if (clientClosed || res.writableEnded || res.destroyed) break;
      res.write(decoder.decode(value, { stream: true }));
    }

    if (!res.writableEnded && !res.destroyed) res.end();
    console.log(`[/api/chat:${requestId}] complete in ${Date.now() - startedAt}ms`);
  } catch (e) {
    const aborted = e.name === "AbortError" || /aborted|disconnect/i.test(e.message || "");
    console.error(`[/api/chat:${requestId}] error after ${Date.now() - startedAt}ms:`, e.message);

    if (upstreamReader) {
      try { await upstreamReader.cancel(e.message); } catch (_) {}
    }

    if (clientClosed) return;

    if (streamStarted) {
      writeAnthropicErrorEvent(res, e.message || "Upstream stream failed");
      if (!res.writableEnded && !res.destroyed) res.end();
      return;
    }

    if (aborted) {
      res.status(499).json({ error: e.message || "Client disconnected" });
      return;
    }

    const timedOut = /timed out/i.test(e.message || "");
    res.status(timedOut ? 504 : 500).json({ error: e.message });
  } finally {
    req.off("aborted", onRequestAborted);
    res.off("close", onResponseClose);
  }
});

// Save/load game state
app.post("/api/save", async (req, res) => {
  const { gameId, state } = req.body;
  try {
    await saveGame(gameId, state);
    res.json({ ok: true });
  } catch (e) {
    console.error("[/api/save] error:", e.message, e.details || "");
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/load/:gameId", async (req, res) => {
  try {
    const state = await loadGame(req.params.gameId);
    res.json({ state });
  } catch (e) {
    console.error("[/api/load] error:", e.message, e.details || "");
    res.status(500).json({ error: e.message });
  }
});

// NPC memory
app.post("/api/memory/store", async (req, res) => {
  const { gameId, npcName, memory } = req.body;
  console.log("[/api/memory/store] gameId:", gameId, "npc:", npcName, "memory keys:", memory ? Object.keys(memory) : "null");
  if (!gameId || !npcName || !memory || !memory.content) {
    console.error("[/api/memory/store] missing required fields");
    return res.status(400).json({ error: "Missing gameId, npcName, or memory.content" });
  }
  try {
    const embedding = await createEmbeddingForText(memory.content);
    await storeMemory(gameId, npcName, memory, embedding);
    res.json({ ok: true });
  } catch (e) {
    console.error("[/api/memory/store] error:", e.message, e.details || "", e.hint || "");
    res.status(500).json({ error: e.message, details: e.details, hint: e.hint });
  }
});

app.post("/api/memory/recall", async (req, res) => {
  const { gameId, npcName, currentContext } = req.body;
  if (!gameId || !npcName || !String(currentContext || "").trim()) {
    return res.status(400).json({ error: "Missing gameId, npcName, or currentContext" });
  }
  try {
    const queryEmbedding = await createEmbeddingForText(currentContext);
    const memory = await recallMemories(gameId, npcName, { limit: 2, queryEmbedding, currentContext });
    res.json({ memory });
  } catch (e) {
    console.error("[/api/memory/recall:post] error:", e.message, e.details || "", e.hint || "");
    res.status(500).json({ error: e.message, details: e.details, hint: e.hint });
  }
});

app.get("/api/memory/recall/:gameId/:npcName", async (req, res) => {
  try {
    const memory = await recallMemories(req.params.gameId, req.params.npcName, { limit: 20 });
    res.json({ memory });
  } catch (e) {
    console.error("[/api/memory/recall] error:", e.message, e.details || "");
    res.status(500).json({ error: e.message });
  }
});

// Cleanup finished game data
app.delete("/api/cleanup/:gameId", async (req, res) => {
  try {
    await deleteGame(req.params.gameId);
    res.json({ ok: true });
  } catch (e) {
    console.error("[/api/cleanup] error:", e.message, e.details || "");
    res.status(500).json({ error: e.message });
  }
});

// Health check — Railway/Render uses this to know the server is alive
app.get("/", (req, res) => res.json({ status: "Survivor DM server running" }));
app.get("/api/health", (req, res) => res.json({ ok: true, anthropic: !!ANTHROPIC_KEY, openai: !!OPENAI_API_KEY }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server running on port " + PORT));
