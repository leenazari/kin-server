# First Dates - streaming server (Railway)

A persistent Node server that serves the front end and runs all three APIs. Because
it stays running (unlike serverless), it can hold a WebSocket open to Velma, so
per-utterance, moment-to-moment emotion (streaming) works here.

## What it does

- Serves the front end from `public/index.html`.
- `GET  /api/analyze` - health (which keys are present, streaming on/off).
- `POST /api/analyze` - audio in, Velma streaming (with batch fallback) + Claude read out.
- `POST /api/synthesize` - builds the clone with Claude Opus.
- `POST /api/tts` - ElevenLabs voice (and `GET /api/tts` is a diagnostic).

## Deploy on Railway

1. Push this `kin-server` folder to a GitHub repo (its own repo is cleanest).
2. In Railway: New Project, Deploy from GitHub repo, pick the repo (or set the root
   directory to `kin-server` if it lives inside a larger repo).
3. Railway auto-detects Node and runs `npm install` then `npm start`. No build step.
4. Add Variables (Railway → your service → Variables):
   - `MODULATE_API_KEY` - your Velma key
   - `ANTHROPIC_API_KEY` - your Claude key
   - `ELEVENLABS_API_KEY` - your ElevenLabs key
   - `ELEVENLABS_VOICE_ID` - optional, the voice you chose
   - optional: `ANTHROPIC_MODEL`, `ELEVENLABS_MODEL`, and `VELMA_STREAMING` (set to `0` to force batch)
5. Railway gives you a public URL (enable a public domain under Settings → Networking
   if it is not on by default). Open it and run a conversation.

Streaming is ON by default here. After a conversation, open the debug panel: `__source`
should read `streaming` with several clips, each carrying its own emotion that can shift
across the answer. If a call ever fails, it falls back to batch automatically.

## Run locally

```bash
npm install
MODULATE_API_KEY=... ANTHROPIC_API_KEY=... ELEVENLABS_API_KEY=... npm start
# open http://localhost:3000
```

## Notes

- Keys live only on the server, never in the page.
- This server is self-contained: once it is up on Railway, use its URL for the demo. The
  Vercel deployment can stay as a no-streaming backup or be retired.
