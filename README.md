# gh-autofollow

A production-grade GitHub auto-follow library and CLI tool. Discovers users through multiple strategies, enforces rate limits, persists state in SQLite, and can run as a background scheduler with OS-level autostart on Linux, macOS, and Windows.

---

## Features

- **Multiple discovery strategies** — trending repos, followers-of-following, starred repo stargazers, topic search, and random contributor exploration
- **SQLite candidate cache** — avoids re-following the same users across restarts
- **Proactive rate-limit tracking** — reads `X-RateLimit-*` headers on every response and backs off before hitting the limit
- **Velocity guard** — per-minute, per-hour, per-day, and per-session sliding-window limits prevent follow spikes
- **Anomaly detection** — analyses run history for error spikes, rate-limit storms, high follow velocity, consecutive failures, and possible account flags
- **Account health checks** — validates token, OAuth scopes (`user:follow`), account suspension status, and following count against GitHub's 5 000 hard limit
- **Token vault** — stores your GitHub token in the OS keyring (Windows Credential Manager / macOS Keychain / libsecret) or an AES-256-GCM encrypted file
- **Background scheduler** — blocking event loop with configurable interval, PID file, and graceful SIGTERM handling
- **OS autostart** — systemd user service (Linux), launchd plist (macOS), or Task Scheduler task (Windows)
- **Dry-run mode** — simulates follows without making any API calls
- **Fully configurable** — TOML/JSON file, environment variables, or programmatic overrides
- **Structured logging** — rotating file handler with optional JSON output

---

## Installation

```bash
pip install gh-autofollow
```

For TOML config file support on Python < 3.11:

```bash
pip install "gh-autofollow[toml]"
```

For encrypted token storage:

```bash
pip install "gh-autofollow[toml]" cryptography
```

---

## Quick start

```bash
export GITHUB_TOKEN=ghp_your_token_here

# Run a single batch (follows up to 10 users)
gh-autofollow run

# Start the background scheduler (runs every hour)
gh-autofollow scheduler

# Simulate without following anyone
gh-autofollow --dry-run run
```

---

## Configuration

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `GH_AUTOFOLLOW_GITHUB_TOKEN` | — | GitHub personal access token (required) |
| `GITHUB_TOKEN` | — | Fallback token variable |
| `GH_AUTOFOLLOW_BATCH_SIZE` | `10` | Users to follow per batch run |
| `GH_AUTOFOLLOW_BATCH_INTERVAL` | `3600` | Seconds between scheduler runs |
| `GH_AUTOFOLLOW_DRY_RUN` | `false` | Set to `true` to simulate |
| `GH_AUTOFOLLOW_STRATEGIES` | `trending,followers_of_following,starred_repos,topic_search` | Comma-separated list |
| `GH_AUTOFOLLOW_VELOCITY_PER_MINUTE` | `3` | Max follows per minute |
| `GH_AUTOFOLLOW_VELOCITY_PER_HOUR` | `30` | Max follows per hour |
| `GH_AUTOFOLLOW_VELOCITY_PER_DAY` | `150` | Max follows per day |

### Config file

`gh-autofollow` auto-detects config files in the following locations:

```
~/.config/gh-autofollow/config.toml
~/.config/gh-autofollow/config.json
./gh-autofollow.toml
./gh-autofollow.json
```

Example `config.toml`:

```toml
github_token = "ghp_..."
batch_size = 15
batch_interval = 3600
dry_run = false

strategies = [
    "trending",
    "followers_of_following",
    "starred_repos",
    "topic_search",
]

topics = ["python", "rust", "open-source", "machine-learning"]
trending_languages = ["python", "typescript", "rust", "go"]

min_followers = 5
min_public_repos = 2
skip_orgs = true
skip_bots = true

[security]
velocity_per_hour = 25
velocity_per_day = 100
anomaly_abort_on_critical = true
```

---

## CLI reference

### `run` — execute a single batch now

```bash
gh-autofollow run [--batch-size N] [--dry-run]
```

### `scheduler` — start the background loop

```bash
gh-autofollow scheduler [--daemon]
```

`--daemon` performs a Unix double-fork so the process detaches from the terminal entirely.

### `discover` — populate the candidate pool without following

```bash
gh-autofollow discover
```

### `stats` — show follow statistics

```bash
gh-autofollow stats [--json]
```

```
=== gh-autofollow stats ===
  Total followed  : 142
  Candidates      : 87 pending / 310 total
  Blocked users   : 3
  Total runs      : 28

  Rate limit      : 4750/5000 core remaining
  Resets in       : 1823s
  Search remaining: 27

  Strategy breakdown:
    trending                        48
    followers_of_following          51
    starred_repos                   29
    topic_search                    14
```

### `history` — show recent batch run history

```bash
gh-autofollow history [--limit 20] [--json]
```

### `autostart` — manage OS autostart

```bash
gh-autofollow autostart install
gh-autofollow autostart remove
gh-autofollow autostart status
```

### `config` — show, validate, or save configuration

```bash
gh-autofollow config show [--json]
gh-autofollow config validate
gh-autofollow config save
```

### `db` — database maintenance

```bash
gh-autofollow db vacuum
gh-autofollow db prune [--days 7]
gh-autofollow db check
gh-autofollow db summary
```

### `blocklist` — manage the follow blocklist

```bash
gh-autofollow blocklist add someuser --reason spam
gh-autofollow blocklist list
```

### `security` — security checks and token management

```bash
# Full account health report
gh-autofollow security health [--json]

# Analyse recent runs for anomalies
gh-autofollow security anomalies

# Show current follow velocity
gh-autofollow security velocity

# Store token in OS keyring / encrypted file
gh-autofollow security token-store
gh-autofollow security token-status
gh-autofollow security token-delete
```

---

## Discovery strategies

| Strategy | Description |
|---|---|
| `trending` | Scrapes GitHub's trending page for repo owners across configured languages |
| `followers_of_following` | Fetches followers of users you already follow (second-degree connections) |
| `starred_repos` | Collects stargazers of repos you have starred (shared-interest signals) |
| `topic_search` | Searches users and repos via the GitHub search API using configured topics |
| `random_explore` | Samples contributors of popular open-source repos (Linux, VS Code, React, etc.) |

---

## Programmatic usage

```python
from gh_autofollow import AutoFollower, Config

config = Config(
    github_token="ghp_...",
    batch_size=10,
    strategies=["trending", "starred_repos"],
    dry_run=False,
)
config.validate()
config.ensure_dirs()

def on_event(event, payload):
    print(f"[{event}]", payload)

with AutoFollower(config, on_event=on_event) as af:
    record = af.run_batch()
    print(f"Followed {record.followed_count} users")
```

### Custom filters

```python
from gh_autofollow.strategies.filters import FilterPipeline

def only_active_users(candidate, config, db):
    if candidate.public_repos < 5:
        return False, "too_few_repos"
    return True, ""

pipeline = FilterPipeline()
pipeline.add_filter(only_active_users)

with AutoFollower(config, filter_pipeline=pipeline) as af:
    af.run_batch()
```

### Event callbacks

`AutoFollower` emits the following events to the `on_event` callback:

| Event | Payload keys |
|---|---|
| `authenticated` | `login` |
| `batch_start` | `run_id`, `batch_size` |
| `batch_complete` | `run_id`, `followed`, `skipped`, `errors` |
| `batch_error` | `error` |
| `follow` | `login`, `strategy` / `dry_run` |
| `rate_limit_hit` | `reset_at` |
| `max_following_reached` | `current`, `max` |
| `strategy_complete` | `strategy`, `found` |

---

## Security

### Token requirements

Generate a token at **GitHub → Settings → Developer settings → Personal access tokens** with the `user:follow` scope. Classic tokens and fine-grained tokens with the `followers` permission both work.

### Rate limits

GitHub enforces 5 000 API requests per hour for authenticated users. `gh-autofollow` tracks the `X-RateLimit-*` response headers on every call and keeps a configurable buffer (default: 100 remaining calls) before stopping the batch. The search API has a separate limit of 30 requests per minute.

### Follow velocity

To avoid triggering GitHub's abuse detection, follow actions are separated by a random jitter delay (default: 2–7 seconds). The velocity guard enforces additional sliding-window limits:

- 3 follows per minute
- 30 follows per hour
- 150 follows per day

These can be adjusted in config or via environment variables.

### Anomaly detection

Before each batch the anomaly detector inspects the last 50 run records for:

- Error rate > 30% in a single run
- 3+ rate-limit hits within 1 hour
- > 50 follows within 1 hour across all runs
- 3+ consecutive failed runs
- 2+ failed runs with high error counts and no rate-limit flag (possible account flag)

Critical anomalies pause execution until manually cleared with `mid.resume()`.

---

## Database

State is persisted in a SQLite database at:

| Platform | Default path |
|---|---|
| Linux | `~/.local/share/gh-autofollow/gh_autofollow.db` |
| macOS | `~/Library/Application Support/gh-autofollow/gh_autofollow.db` |
| Windows | `%APPDATA%\gh-autofollow\gh_autofollow.db` |

Tables: `followed_users`, `candidate_cache`, `rate_limit_log`, `run_log`, `blocked_users`, `velocity_events`, `schema_migrations`.

WAL mode is enabled by default for safe concurrent access.

---

## Development

```bash
git clone https://github.com/NgoHuuLoc0612/gh-autofollow
cd gh-autofollow
pip install -e ".[dev]"
pytest tests/ -v
```

### Running tests

```bash
# All tests
pytest

# With coverage
pytest --cov=gh_autofollow --cov-report=term-missing

# Security module only
pytest tests/test_security.py -v
```

---

## License

MIT — see [LICENSE](LICENSE) for details.
