const express = require("express");
const cors = require("cors");
require("dotenv").config({ override: true });
const { saveGame, loadGame, storeMemory, recallMemories, deleteGame } = require("./db");

const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));

const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
if (!ANTHROPIC_KEY) { console.error("Missing ANTHROPIC_API_KEY"); process.exit(1); }

// The only route that matters — proxy to Anthropic with model routing
app.post("/api/chat", async (req, res) => {
  const { messages, system, maxTokens, callType } = req.body;

  // Model routing: Haiku for cheap stuff, Sonnet for everything else
  const SONNET = "claude-sonnet-4-6";
  const HAIKU = "claude-haiku-4-5-20251001";
  const model = (callType === "anchor" || callType === "morning" || callType === "transition")
    ? HAIKU : SONNET;

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
        system,
        messages,
        stream: true,
      }),
    });

    if (!response.ok) {
      const err = await response.text();
      res.status(response.status).send(err);
      return;
    }

    // Stream the response back to the browser exactly as-is
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      res.write(decoder.decode(value, { stream: true }));
    }
    res.end();
  } catch (e) {
    console.error("API error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

// Save/load game state
app.post("/api/save", async (req, res) => {
  const { gameId, state } = req.body;
  try {
    await saveGame(gameId, state);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/load/:gameId", async (req, res) => {
  try {
    const state = await loadGame(req.params.gameId);
    res.json({ state });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// NPC memory
app.post("/api/memory/store", async (req, res) => {
  const { gameId, npcName, memory } = req.body;
  try {
    await storeMemory(gameId, npcName, memory);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/memory/recall/:gameId/:npcName", async (req, res) => {
  try {
    const memory = await recallMemories(req.params.gameId, req.params.npcName);
    res.json({ memory });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Cleanup finished game data
app.delete("/api/cleanup/:gameId", async (req, res) => {
  try {
    await deleteGame(req.params.gameId);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Health check — Railway/Render uses this to know the server is alive
app.get("/", (req, res) => res.json({ status: "Survivor DM server running" }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server running on port " + PORT));
