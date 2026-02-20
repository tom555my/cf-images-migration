# Cloudflare Images Migration CLI (Bun)

CLI tool to migrate images from one Cloudflare Images account to another, or list source images.

## What it does

- Lists source images via `GET /accounts/{account_id}/images/v1`
- Exports each original image via `GET /accounts/{account_id}/images/v1/{image_id}/blob`
- Uploads into destination via `POST /accounts/{account_id}/images/v1`
- Supports `--dry-run`, concurrency, retries, and resume from a state file

## Prerequisites

- [Bun](https://bun.sh)
- Cloudflare API token for source account with Images read permissions
- Cloudflare API token for destination account with Images write permissions

## Setup

```bash
cp .env.example .env
```

Set:

- `CF_SOURCE_ACCOUNT_ID`
- `CF_SOURCE_API_TOKEN`
- `CF_DEST_ACCOUNT_ID`
- `CF_DEST_API_TOKEN`

## Usage

List source images:

```bash
bun run src/index.ts list
```

List as NDJSON:

```bash
bun run src/index.ts list --json --max-pages 2
```

Real migration (resumable):

```bash
bun run src/index.ts migrate --resume --concurrency 6 --page-size 100
```

With explicit flags:

```bash
bun run src/index.ts \
  migrate \
  --source-account-id <SOURCE_ACCOUNT_ID> \
  --source-api-token <SOURCE_TOKEN> \
  --destination-account-id <DEST_ACCOUNT_ID> \
  --destination-api-token <DEST_TOKEN>
```

## Important flags

- `--state-file <path>`: default `.context/cf-images-migration-state.jsonl`
- `--max-pages <n>`: limit how many pages are fetched when using `list`
- `--json`: emit one JSON object per image (list mode)
- `--on-conflict skip|error`: default `skip`
- `--no-preserve-id`: do not reuse source IDs in destination
- `--no-preserve-metadata`: do not copy metadata
- `--no-preserve-signed-urls`: do not copy `requireSignedURLs`

Show all options:

```bash
bun run src/index.ts --help
```

## Notes

- State is stored as JSONL and includes `success`, `skipped`, and `failed` rows.
- With `--resume`, previously `success`/`skipped` IDs are ignored.
