# Phoenix Git Sync Policy

This repository is the public source snapshot for code review and ChatGPT analysis.

Rules:

- Treat GitHub as the source of truth for code changes.
- Do not commit API keys, `.env` files, SSH keys, tokens, logs, trade records, or raw runtime data.
- Keep VPS edits read-only unless Shawn explicitly approves a deployment.
- If emergency edits happen on the VPS, sync the source files back into this repo before further work.
- Runtime data directories such as `round_runner_reports*`, `logs`, `state`, `analyst_exports`, `hermes_exports`, and backups are intentionally ignored.

Recommended flow:

1. Edit code in this repository.
2. Commit and push to GitHub.
3. Review the diff.
4. Deploy only approved files to the VPS with backups and verification.
