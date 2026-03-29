const { createClient } = require("@supabase/supabase-js");

const SUPABASE_SERVER_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY;
const hasSupabaseConfig = !!(process.env.SUPABASE_URL && SUPABASE_SERVER_KEY);
const supabase = hasSupabaseConfig
  ? createClient(process.env.SUPABASE_URL, SUPABASE_SERVER_KEY)
  : null;

function requireSupabase() {
  if (!supabase) {
    throw new Error("Supabase is not configured on this server");
  }
  return supabase;
}

async function saveGame(gameId, state, meta = {}) {
  const { error } = await requireSupabase()
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
  const { data, error } = await requireSupabase()
    .from("games")
    .select("state")
    .eq("id", gameId)
    .single();
  if (error) throw error;
  return data?.state || null;
}

async function storeMemory(gameId, npcName, { day, beat, event_type, importance, content, emotional_context, involves_player }, embedding = null) {
  const client = requireSupabase();
  // Ensure game row exists (foreign key requirement) before inserting memory
  await client.from("games").upsert({ id: gameId, updated_at: new Date().toISOString() }, { onConflict: "id", ignoreDuplicates: true });

  const { error } = await client
    .from("npc_memories")
    .insert({
      game_id: gameId,
      npc_name: npcName,
      day: day || null,
      beat: beat || null,
      event_type: event_type || null,
      importance: importance || null,
      content,
      embedding: Array.isArray(embedding) ? embedding : null,
      emotional_context: emotional_context || null,
      involves_player: involves_player || false,
    });
  if (error) throw error;
}

function isRpcSignatureError(error) {
  const msg = String(error?.message || "");
  return error?.code === "PGRST202" || /function .*match_memories/i.test(msg) || /could not find/i.test(msg);
}

async function callMatchMemoriesRpc(client, gameId, npcName, queryEmbedding, limit, currentContext) {
  const payloads = [
    { query_embedding: queryEmbedding, match_count: limit, filter: { game_id: gameId, npc_name: npcName } },
    { query_embedding: queryEmbedding, match_count: limit, filter_game_id: gameId, filter_npc_name: npcName },
    { query_embedding: queryEmbedding, match_count: limit, game_id: gameId, npc_name: npcName },
    { query_embedding: queryEmbedding, match_count: limit, p_game_id: gameId, p_npc_name: npcName },
    { query_embedding: queryEmbedding, match_count: limit, game_id_filter: gameId, npc_name_filter: npcName },
    { query_embedding: queryEmbedding, match_count: limit, current_context: currentContext, game_id: gameId, npc_name: npcName },
  ];
  let lastError = null;
  for (const args of payloads) {
    const { data, error } = await client.rpc("match_memories", args);
    if (!error) return Array.isArray(data) ? data : [];
    lastError = error;
    if (!isRpcSignatureError(error)) throw error;
  }
  if (lastError) throw lastError;
  return [];
}

async function recallMemories(gameId, npcName, options = {}) {
  const client = requireSupabase();
  const limit = Number(options.limit || 20);
  const queryEmbedding = Array.isArray(options.queryEmbedding) ? options.queryEmbedding : null;
  const currentContext = String(options.currentContext || "");
  if (queryEmbedding && queryEmbedding.length) {
    const data = await callMatchMemoriesRpc(client, gameId, npcName, queryEmbedding, limit, currentContext);
    if (data.length) return data;
  }
  const { data, error } = await client
    .from("npc_memories")
    .select("day, beat, event_type, importance, content, emotional_context, involves_player, created_at")
    .eq("game_id", gameId)
    .eq("npc_name", npcName)
    .order("importance", { ascending: false })
    .order("created_at", { ascending: false })
    .limit(limit);
  if (error) throw error;
  return data || [];
}

async function deleteGame(gameId) {
  const client = requireSupabase();
  await client.from("npc_memories").delete().eq("game_id", gameId);
  await client.from("npc_relationships").delete().eq("game_id", gameId);
  await client.from("games").delete().eq("id", gameId);
}

module.exports = { saveGame, loadGame, storeMemory, recallMemories, deleteGame, hasSupabaseConfig };
