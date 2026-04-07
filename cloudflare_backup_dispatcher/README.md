# Macro Feishu Backup Dispatcher

This Cloudflare Worker is the free primary timer for the `daily_macro_feishu.yml` workflow.

What it does:

- Runs every day at `UTC 01:05` (`09:05` China time).
- Checks whether today's GitHub Actions run already exists.
- Skips dispatch if a healthy run is already queued, in progress, or completed successfully.
- Triggers the workflow with `workflow_dispatch` if nothing healthy ran today.

## Why this helps

GitHub Actions scheduled workflows can be delayed or dropped under load. This Worker gives you an external timer without needing your own computer to stay online.

Current production setup:

- Cloudflare Worker cron is the only scheduler.
- GitHub workflow keeps `workflow_dispatch` only.
- Daily trigger time is `UTC 01:05` (`09:05` China time).
- Production Worker URL: `https://macro-feishu-backup-dispatcher.yufennbrief2026.workers.dev`

## Files

- `src/index.js`: Worker logic
- `wrangler.toml`: Worker config and cron schedule
- `.dev.vars.example`: local dev secret example

## One-time setup

1. Install Wrangler.
2. Log in to Cloudflare with Wrangler.
3. Create a GitHub token with access to this repository.
4. Add the GitHub token as a Cloudflare Worker secret.
5. Deploy the Worker.

## Recommended GitHub token

Use a fine-grained personal access token for repository `yufenn122-boop/macro-feishu-tracker`.

Recommended permission:

- Actions: Read and write

## Deploy commands

```bash
cd macro-feishu-tracker/cloudflare_backup_dispatcher
wrangler secret put GITHUB_TOKEN
wrangler deploy
```

## Test commands

Health check:

```bash
curl https://macro-feishu-backup-dispatcher.yufennbrief2026.workers.dev/health
```

Manual run check:

```bash
curl -X POST https://macro-feishu-backup-dispatcher.yufennbrief2026.workers.dev/run
```

## Important note about duplicates

The repository is already set up so Cloudflare is the only timer and GitHub only executes manual dispatches from the Worker.

If you ever add a GitHub `schedule:` block back into `daily_macro_feishu.yml`, duplicate or delayed runs can return.
