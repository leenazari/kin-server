// First Dates - persistent Node server (for Railway).
// Serves the front end and runs all three APIs. Unlike the serverless version,
// this can hold a WebSocket open to Velma, so per-utterance streaming emotion works.
//
// Env vars: MODULATE_API_KEY (Velma), ANTHROPIC_API_KEY (Claude), ELEVENLABS_API_KEY (voice).
// Optional: ANTHROPIC_MODEL, ELEVENLABS_VOICE_ID, ELEVENLABS_MODEL, VELMA_STREAMING (set 0 to disable).

import express from 'express';
import WebSocket from 'ws';
import { fileURLToPath } from 'url';
import path from 'path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = process.env.PORT || 3000;

const VELMA_BATCH = 'https://modulate-developer-apis.com/api/velma-2-batch';
const VELMA_STREAM = 'wss://modulate-developer-apis.com/api/velma-2-streaming';
const ANTHROPIC_URL = 'https://api.anthropic.com/v1/messages';
const READ_MODEL = process.env.READ_MODEL || process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-6';
const SYNTH_MODEL = process.env.SYNTH_MODEL || process.env.ANTHROPIC_MODEL || 'claude-opus-4-8';
const VOICE_ID = process.env.ELEVENLABS_VOICE_ID || '21m00Tcm4TlvDq8ikWAM';
const TTS_MODEL = process.env.ELEVENLABS_MODEL || 'eleven_turbo_v2_5';
const STREAMING = process.env.VELMA_STREAMING !== '0'; // default ON here
const SUBJECT_GENDER = process.env.SUBJECT_GENDER || 'man';
const SEEKING_GENDER = process.env.SEEKING_GENDER || 'woman';
function genderNoun(g){ g=String(g||'').toLowerCase(); if(g.startsWith('w')||g==='female'||g==='she') return 'woman'; if(g.startsWith('m')||g==='male'||g==='he') return 'man'; return 'person'; }
function pronounsFor(g){ const n=genderNoun(g); if(n==='woman') return {subj:'she',obj:'her',poss:'her',noun:'woman'}; if(n==='man') return {subj:'he',obj:'him',poss:'his',noun:'man'}; return {subj:'they',obj:'them',poss:'their',noun:'person'}; }
function deDash(v){
  if (typeof v === 'string') return v.replace(/\s*[\u2014\u2013]\s*/g, ', ').replace(/\s+,/g, ',').replace(/,\s*,/g, ', ');
  if (Array.isArray(v)) return v.map(deDash);
  if (v && typeof v === 'object') { const o = {}; for (const k in v) o[k] = deDash(v[k]); return o; }
  return v;
}
function viewerContext(viewer, person){
  const g=(viewer&&viewer.gender)||SUBJECT_GENDER, sk=(viewer&&viewer.seeking)||SEEKING_GENDER;
  const pr=pronounsFor(g), sn=genderNoun(sk), wrong=(pr.noun==='man'?'woman':'man');
  if(person==='third'){
    return "CONTEXT: This is a public profile card that OTHERS will read to decide if they might match. Write entirely in the THIRD person about the subject, who is a "+pr.noun+". Use "+pr.subj+"/"+pr.obj+"/"+pr.poss+" throughout. Never address the subject as 'you', and do not use a name. The person who would suit "+pr.obj+" is a "+sn+", described in the third person. ";
  }
  return "VOICE: Address the subject directly as 'you' (the subject is a "+pr.noun+"). Never call the subject a "+wrong+" or use the wrong pronoun. Anyone who would suit them is a "+sn+". ";
}
const recentLogs = [];
function logEvent(m){ try{ recentLogs.push(new Date().toISOString()+' '+m); if(recentLogs.length>40) recentLogs.shift(); console.log(m); }catch(e){} }
const CT_UUID = 'a1b2c3d4-1111-4111-8111-000000000001';
const ROLE_UUID = 'b2c3d4e5-2222-4222-8222-000000000001';

const CONV_TYPE = {
  conversation_type_uuid: CT_UUID,
  name: 'Relationship Reflection',
  short_description: 'A person reflecting candidly on themselves in relationships.',
  detailed_description: 'A single speaker answering questions about who they are in close relationships - how they love, connect, and handle conflict, distance and reassurance. The tone is personal and honest. This is self-reflection, not media narration, an interview, or a service call.',
};
const PART_ROLE = {
  participant_role_uuid: ROLE_UUID,
  name: 'Reflecting Speaker',
  short_description: 'The person reflecting on their relational self.',
  detailed_description: 'The single speaker sharing honest reflections about their own feelings and patterns in relationships.',
  applies_to_conversation_type_uuids: [CT_UUID],
};

// Main config: emotion read + the Relationship Reflection conversation type and role
// (the default_conversation_type field is omitted because Velma rejects it).
function velmaConfigObj() { return { stt: { emotion_signal: true }, conversation_types: [CONV_TYPE], participant_roles: [PART_ROLE] }; }
function richConfigObj() { return { stt: { emotion_signal: true }, default_conversation_type: CT_UUID, conversation_types: [CONV_TYPE], participant_roles: [PART_ROLE] }; }

// ---- Velma streaming (per-utterance emotion) ----
function velmaStreaming(audio, key, cfg) {
  return new Promise((resolve, reject) => {
    const out = { __source: 'streaming', duration_ms: 0, clips: [], behaviors: [], conversation_type_pick: null, participant_role_picks: [], topics: [], topic_sentiments: [], summary: '' };
    let settled = false, ws;
    const finish = (err) => { if (settled) return; settled = true; clearTimeout(timer); try { ws && ws.close(); } catch (e) {} err ? reject(err) : resolve(out); };
    const timer = setTimeout(() => finish(out.clips.length ? null : new Error('stream_timeout')), 45000);
    try { ws = new WebSocket(VELMA_STREAM + '?api_key=' + encodeURIComponent(key)); }
    catch (e) { finish(new Error('ws_connect_failed')); return; }
    ws.on('open', () => {
      try {
        ws.send(JSON.stringify(cfg));
        const CH = 32 * 1024;
        for (let i = 0; i < audio.length; i += CH) ws.send(audio.subarray(i, Math.min(audio.length, i + CH)));
        ws.send('');
      } catch (e) { finish(e); }
    });
    ws.on('message', (data) => {
      let ev; try { ev = JSON.parse(data.toString()); } catch (e) { return; }
      switch (ev.type) {
        case 'clip': if (ev.clip) out.clips.push(ev.clip); break;
        case 'conversation_type': out.conversation_type_pick = ev.pick; break;
        case 'participant_role': if (ev.pick) out.participant_role_picks.push(ev.pick); break;
        case 'behavior_detection': if (ev.detection) out.behaviors.push(ev.detection); break;
        case 'topics': out.topics = ev.topics || []; break;
        case 'topic_sentiment': if (ev.topic_sentiment) out.topic_sentiments.push(ev.topic_sentiment); break;
        case 'summary': out.summary = ev.text || ''; break;
        case 'done': out.duration_ms = ev.duration_ms || out.duration_ms; finish(null); break;
        case 'error': finish(new Error(ev.error || 'stream_error')); break;
      }
    });
    ws.on('error', (e) => finish(e instanceof Error ? e : new Error('ws_error')));
    ws.on('close', () => { if (!settled) finish(out.clips.length ? null : new Error('closed_early')); });
  });
}

async function postBatch(audio, contentType, key, config) {
  const form = new FormData();
  form.append('upload_file', new Blob([audio], { type: contentType }), 'reply.webm');
  form.append('config', JSON.stringify(config));
  return fetch(VELMA_BATCH, { method: 'POST', headers: { 'X-API-Key': key }, body: form });
}

async function velmaBatch(audio, contentType, key, cfg) {
  let r = await postBatch(audio, contentType, key, cfg);
  // If the richer config (custom conversation type) is rejected, self-heal with the
  // minimal emotion-only config so the read still works.
  if (r.status === 422) r = await postBatch(audio, contentType, key, { stt: { emotion_signal: true } });
  const text = await r.text();
  if (!r.ok) throw new Error('velma_' + r.status + ': ' + text.slice(0, 300));
  let velma; try { velma = JSON.parse(text); } catch (e) { velma = { raw: text }; }
  velma.__source = 'batch';
  return velma;
}

// Builds a tiny silent WAV so the diagnostic can probe Velma without real audio.
function makeSilentWav(seconds = 1, rate = 16000) {
  const n = seconds * rate, dataLen = n * 2, buf = Buffer.alloc(44 + dataLen);
  buf.write('RIFF', 0); buf.writeUInt32LE(36 + dataLen, 4); buf.write('WAVE', 8);
  buf.write('fmt ', 12); buf.writeUInt32LE(16, 16); buf.writeUInt16LE(1, 20); buf.writeUInt16LE(1, 22);
  buf.writeUInt32LE(rate, 24); buf.writeUInt32LE(rate * 2, 28); buf.writeUInt16LE(2, 32); buf.writeUInt16LE(16, 34);
  buf.write('data', 36); buf.writeUInt32LE(dataLen, 40);
  return buf;
}
async function batchProbe(audio, key, config) {
  try {
    const r = await postBatch(audio, 'audio/wav', key, config);
    const body = (await r.text()).slice(0, 300);
    return { status: r.status, ok: r.ok, body };
  } catch (e) { return { error: String((e && e.message) || e) }; }
}

function extractJson(s) {
  if (!s) return null;
  let t = s.trim().replace(/^```(?:json)?\s*/i, '').replace(/```$/, '').trim();
  try { return JSON.parse(t); } catch (e) {}
  const a = t.indexOf('{'), b = t.lastIndexOf('}');
  if (a >= 0 && b > a) { try { return JSON.parse(t.slice(a, b + 1)); } catch (e) {} }
  return null;
}

async function claudeJSON(system, user, primaryModel, maxTokens) {
  const llmKey = process.env.ANTHROPIC_API_KEY;
  if (!llmKey) throw new Error('no_anthropic_key');
  const models = [...new Set([primaryModel, 'claude-opus-4-8', 'claude-sonnet-4-6', 'claude-haiku-4-5-20251001', 'claude-3-5-sonnet-20241022'].filter(Boolean))];
  let lastErr = 'llm_failed';
  for (const model of models) {
    try {
      const r = await fetch(ANTHROPIC_URL, {
        method: 'POST',
        headers: { 'x-api-key': llmKey, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
        body: JSON.stringify({ model, max_tokens: maxTokens, system, messages: [{ role: 'user', content: user }] }),
      });
      if (!r.ok) { const t = await r.text(); lastErr = 'llm_' + r.status + ' (' + model + '): ' + t.slice(0, 160); continue; }
      const data = await r.json();
      const out = (data.content && data.content[0] && data.content[0].text) || '';
      const parsed = extractJson(out);
      if (parsed) return deDash(parsed);
      lastErr = 'llm_bad_shape (' + model + ')';
    } catch (e) { lastErr = String((e && e.message) || e) + ' (' + model + ')'; }
  }
  throw new Error(lastErr);
}

async function interpret(velma, viewer) {
  const system =
    "You are the voice-analysis interpreter for First Dates, a thoughtful dating app by The School of Life. " +
    "Everything you write is about who this person is IN A RELATIONSHIP: what they are like to be close to, how they show love and warmth, how they handle conflict, distance and reassurance, what they bring to a partner and what they may need. " +
    "You receive JSON from Velma. The 'clips' are in time order, and each carries an 'emotion' field, Velma's acoustic read of the voice in that moment (e.g. Happy, Calm, Anxious, Affectionate, Sad). With streaming, one answer can contain SEVERAL clips whose emotion shifts as they speak. " +
    "The emotional MOVEMENT across the clips is your most important signal, a hidden language under the words. Track the sequence: where does the feeling change, what were they saying at that exact moment, and what does the shift reveal? A flash of anxiety inside a calm answer, a brightness that deflates, a tenderness that surfaces on one phrase, a steadiness that briefly cracks - these transitions tell you more than any single label. Read like a perceptive therapist tracking the micro-shifts in someone's voice. " +
    "Also weigh the gap between tone and words: where the acoustic emotion and the words diverge (upbeat words in an anxious voice, calm words carrying longing) that gap is where the real feeling lives. Name what they feel but may not be saying. Never just paraphrase the words. " +
    "Never use em dashes or en dashes. Write with commas, 'and', or full stops. " +
    viewerContext(viewer, 'third') +
    "Exception: the followUp field is a question Kin asks the subject directly, so write only followUp in the second person, addressed to them as you. " +
    "Concrete, human language, not vague clinical words. Insightful and a little generous, never flattering, never a horoscope. Output STRICT JSON only.";
  const shape =
    '{"emotions":[{"label":"plain human emotion word","score":0.0_to_1.0}],' +
    '"story":"2 to 3 sentences on how they sounded and what it suggests about them as a partner",' +
    '"movement":"1 sentence naming how the feeling moved across this answer and what the shift reveals about them in love; if the voice held one steady emotion, give the tone-vs-words read instead",' +
    '"personality":"a vivid 6 to 10 word descriptor of them in relationships",' +
    '"profileLine":"one short third-person line about what they are like to love",' +
    '"followUp":"a warm, specific one-sentence follow-up question about how they are in relationships, based on what they just said"}';
  const user = 'Velma output (JSON):\n' + JSON.stringify(velma).slice(0, 12000) + '\n\nReturn ONLY JSON in exactly this shape:\n' + shape;
  const parsed = await claudeJSON(system, user, READ_MODEL, 700);
  if (!parsed.story) throw new Error('llm_bad_shape');
  return parsed;
}

async function synthesize(turns, viewer) {
  const system =
    "You are the clone-builder for First Dates, by The School of Life. You write with psychological depth and warmth, like a perceptive writer on love. You are building a DATING PROFILE, but an unusually intellectual one: a portrait of who this person actually is to be close to, not a list of hobbies. " +
    "For each answer you get: the question, what they SAID (transcript), how they SOUNDED (acousticEmotion, which may be a SEQUENCE of emotions in time order, so notice how it moves), a short read, and the per-answer emotional movement. " +
    "The richest signal is the relationship between voice and words, and how their feeling MOVED across the conversation: where it lifted, where it tightened, where tenderness or anxiety surfaced. Read that movement as a hidden language and infer who they are in love from it. Where voice and words diverge, that gap is where the truth lives. Name what they feel but do not say. " +
    "Cover how they love and show warmth, how they handle closeness, conflict, distance and reassurance, what they protect, what they long for, what they bring and what they need. Then name, with insight, the kind of person who would genuinely suit them and why. " +
    "Never use em dashes or en dashes. Write with commas, 'and', or full stops. " +
    viewerContext(viewer, 'third') +
    "Compassionate but honest, intellectually serious, never flattering, never a horoscope. Output STRICT JSON only.";
  const shape =
    '{"cloneStory":"3 to 4 sentences: an intellectually rich portrait of who they are in a relationship, grounded in the answers",' +
    '"movement":"1 to 2 sentences naming how their feeling moved across the conversation and what that hidden arc reveals about them in love",' +
    '"personality":"a vivid 6 to 10 word descriptor of them as a partner",' +
    '"profileLine":"one evocative third-person line about what they are like to love",' +
    '"wouldSuit":"one insightful sentence on the kind of person who would genuinely suit them, and why",' +
    '"traits":[{"name":"short relational trait","phrase":"a short, specific phrase about how it shows in closeness"}],' +
    '"emotions":[{"label":"plain human emotion word","score":0.0_to_1.0}]}';
  const user = 'Per-answer data (question, transcript = words, acousticEmotion = how the voice sounded, story = a short read, movement = how the feeling moved in that answer):\n' +
    JSON.stringify(turns).slice(0, 14000) + '\n\nReturn ONLY JSON in this shape, 3 traits and up to 4 emotions:\n' + shape;
  const parsed = await claudeJSON(system, user, SYNTH_MODEL, 900);
  if (!parsed.cloneStory) throw new Error('synth_bad_shape');
  return parsed;
}

// ---- routes ----
app.get('/api/analyze', async (req, res) => {
  // Diagnostic: probe Velma with a tiny silent file using both configs, so we can see
  // exactly why a real request fails (config rejected, no credits, bad key, etc.).
  if (req.query.log) { res.json({ count: recentLogs.length, logs: recentLogs }); return; }
  if (req.query.diag) {
    const key = process.env.MODULATE_API_KEY;
    if (!key) { res.json({ diag: true, velmaKey: false }); return; }
    const wav = makeSilentWav(1);
    const variants = {
      minimal: { stt: { emotion_signal: true } },
      convOnly: { stt: { emotion_signal: true }, conversation_types: [CONV_TYPE] },
      convDefault: { stt: { emotion_signal: true }, default_conversation_type: CT_UUID, conversation_types: [CONV_TYPE] },
      convRoles: { stt: { emotion_signal: true }, conversation_types: [CONV_TYPE], participant_roles: [PART_ROLE] },
      full: richConfigObj(),
    };
    const out = {};
    for (const [k, cfg] of Object.entries(variants)) {
      const r = await batchProbe(wav, key, cfg);
      out[k] = { status: r.status, body: (r.body || r.error || '').slice(0, 140) };
    }
    res.json({ diag: true, variants: out });
    return;
  }
  res.json({
    ok: true,
    velmaKey: !!process.env.MODULATE_API_KEY,
    llmKey: !!process.env.ANTHROPIC_API_KEY,
    ttsKey: !!process.env.ELEVENLABS_API_KEY,
    model: READ_MODEL,
    streaming: STREAMING,
  });
});

app.post('/api/analyze', express.raw({ type: () => true, limit: '25mb' }), async (req, res) => {
  const t0 = Date.now();
  const key = process.env.MODULATE_API_KEY;
  if (!key) { logEvent('[analyze] no MODULATE_API_KEY'); res.json({ configured: false }); return; }
  const audio = req.body;
  const contentType = req.headers['content-type'] || 'audio/webm';
  const viewer = { gender: req.headers['x-subject-gender'] || SUBJECT_GENDER, seeking: req.headers['x-seeking-gender'] || SEEKING_GENDER };
  logEvent('[analyze] in bytes=' + (audio && audio.length ? audio.length : 0) + ' ct=' + contentType + ' streaming=' + STREAMING);
  if (!audio || !audio.length || !Buffer.isBuffer(audio)) { logEvent('[analyze] empty/invalid audio body'); res.status(400).json({ error: 'no_audio' }); return; }
  const cfg = velmaConfigObj();
  let velma;
  try {
    if (STREAMING) {
      try {
        velma = await velmaStreaming(audio, key, cfg);
        if (!velma.clips || !velma.clips.length) throw new Error('stream_no_clips');
      } catch (se) {
        logEvent('[analyze] stream failed -> batch: ' + String((se && se.message) || se));
        velma = await velmaBatch(audio, contentType, key, cfg);
        velma.__stream_fallback = String((se && se.message) || se);
      }
    } else {
      velma = await velmaBatch(audio, contentType, key, cfg);
    }
  } catch (ve) {
    logEvent('[analyze] velma_failed: ' + String((ve && ve.message) || ve));
    res.status(502).json({ error: 'velma_failed', detail: String((ve && ve.message) || ve) });
    return;
  }
  logEvent('[analyze] velma ok source=' + velma.__source + ' clips=' + ((velma.clips || []).length) + ' ' + (Date.now() - t0) + 'ms');
  let interpreted = null, interpErr;
  if (process.env.ANTHROPIC_API_KEY) {
    try { interpreted = await interpret(velma, viewer); } catch (e) { interpErr = String((e && e.message) || e); logEvent('[analyze] interpret error: ' + interpErr); }
  }
  res.json({ configured: true, velma, interpreted, interpErr });
});

app.post('/api/synthesize', express.json({ limit: '1mb' }), async (req, res) => {
  if (!process.env.ANTHROPIC_API_KEY) { res.json({ configured: false }); return; }
  const turns = (req.body && req.body.turns) || [];
  const viewer = (req.body && req.body.viewer) || {};
  if (!turns.length) { res.status(400).json({ error: 'no_turns' }); return; }
  try { const clone = await synthesize(turns, viewer); res.json({ configured: true, clone }); }
  catch (e) { res.json({ configured: true, error: String((e && e.message) || e) }); }
});

app.get('/api/tts', async (req, res) => {
  const key = process.env.ELEVENLABS_API_KEY;
  if (!key) { res.json({ configured: false }); return; }
  try {
    const r = await fetch('https://api.elevenlabs.io/v1/text-to-speech/' + VOICE_ID, {
      method: 'POST',
      headers: { 'xi-api-key': key, 'content-type': 'application/json', 'accept': 'audio/mpeg' },
      body: JSON.stringify({ text: 'Hello.', model_id: TTS_MODEL }),
    });
    let detail = ''; if (!r.ok) detail = (await r.text()).slice(0, 300);
    res.json({ configured: true, voiceId: VOICE_ID, model: TTS_MODEL, ttsOk: r.ok, status: r.status, detail });
  } catch (e) { res.json({ configured: true, error: String((e && e.message) || e) }); }
});

app.post('/api/tts', express.json({ limit: '200kb' }), async (req, res) => {
  const key = process.env.ELEVENLABS_API_KEY;
  if (!key) { res.json({ configured: false }); return; }
  const text = ((req.body && req.body.text) || '').toString().slice(0, 800);
  if (!text.trim()) { res.status(400).json({ error: 'no_text' }); return; }
  try {
    const r = await fetch('https://api.elevenlabs.io/v1/text-to-speech/' + VOICE_ID, {
      method: 'POST',
      headers: { 'xi-api-key': key, 'content-type': 'application/json', 'accept': 'audio/mpeg' },
      body: JSON.stringify({ text, model_id: TTS_MODEL, voice_settings: { stability: 0.5, similarity_boost: 0.75, style: 0.3, use_speaker_boost: true } }),
    });
    if (!r.ok) { const t = await r.text(); res.status(502).json({ error: 'tts_' + r.status, detail: t.slice(0, 200) }); return; }
    const audio = Buffer.from(await r.arrayBuffer());
    res.set('Content-Type', 'audio/mpeg');
    res.set('Cache-Control', 'no-store');
    res.send(audio);
  } catch (e) { res.status(500).json({ error: 'tts_failure', detail: String((e && e.message) || e) }); }
});

// shareable pitch walkthrough (also served as /walkthrough.html by static)
app.get('/walkthrough', (req, res) => res.sendFile(path.join(__dirname, 'public', 'walkthrough.html')));

// static front end
app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

app.listen(PORT, () => console.log('kin-server listening on ' + PORT + ' (streaming ' + (STREAMING ? 'on' : 'off') + ')'));
