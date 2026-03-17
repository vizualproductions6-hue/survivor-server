const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

async function saveGame(gameId, state, meta = {}) {
  const { error } = await supabase
    .from("games")
    .upsert({
      id: gameId,
      state,
      player_name: meta.playerName || null,
      difficulty: meta.difficulty || null,
      format: meta.format || null,
      updated_at: new Date().toISOString(),
    });
  if (error) throw error;
}

async function loadGame(gameId) {
  const { data, error } = await supabase
    .from("games")
    .select("state")
    .eq("id", gameId)
    .single();
  if (error) throw error;
  return data?.state || null;
}

async function storeMemory(gameId, npcName, { day, beat, event_type, importance, content, emotional_context, involves_player }) {
  const { error } = await supabase
    .from("npc_memories")
    .insert({
      game_id: gameId,
      npc_name: npcName,
      day: day || null,
      beat: beat || null,
      event_type: event_type || null,
      importance: importance || null,
      content,
      emotional_context: emotional_context || null,
      involves_player: involves_player || false,
    });
  if (error) throw error;
}

async function recallMemories(gameId, npcName, limit = 20) {
  const { data, error } = await supabase
    .from("npc_memories")
    .select("day, beat, event_type, importance, content, emotional_context, involves_player, created_at")
    .eq("game_id", gameId)
    .eq("npc_name", npcName)
    .order("importance", { ascending: false })
    .limit(limit);
  if (error) throw error;
  return data || [];
}

async function deleteGame(gameId) {
  await supabase.from("npc_memories").delete().eq("game_id", gameId);
  await supabase.from("npc_relationships").delete().eq("game_id", gameId);
  await supabase.from("games").delete().eq("id", gameId);
}

module.exports = { saveGame, loadGame, storeMemory, recallMemories, deleteGame };
