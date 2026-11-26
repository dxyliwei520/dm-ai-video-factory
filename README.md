# DM Main Site (cccccxai.com) — AI video factory

Static frontend + FastAPI backend for an AI image/video generation site.
Published for archival/reading purposes after the site was retired.

## Stack
- `server.py` — FastAPI backend (auth, chat, credits, generation proxy)
- `index.html` — single-page frontend (vanilla JS)
- `vip_credits.py` — credit/VIP ledger helpers
- static pages: credits / pricing / privacy / terms / refund

## Running
1. `pip install -r requirements.txt`
2. Provide configuration via environment variables (see `.env.example`)
3. `python server.py`

## Not included (intentionally)
- `.env` / API keys, `users.db` (user accounts, orders, chat history), uploads.
