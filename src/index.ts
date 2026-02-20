#!/usr/bin/env bun

import { existsSync, mkdirSync, appendFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { dirname } from "node:path";

type JsonObject = Record<string, unknown>;

type CloudflareEnvelope<T> = {
  success: boolean;
  errors: Array<{ code?: number; message?: string }>;
  messages: Array<{ code?: number; message?: string }>;
  result: T;
  result_info?: {
    count?: number;
    page?: number;
    per_page?: number;
    total_count?: number;
    total_pages?: number;
  };
};

type CloudflareImage = {
  id: string;
  filename?: string;
  requireSignedURLs?: boolean;
  meta?: JsonObject;
  uploaded?: string;
};

type CloudflareListResult =
  | CloudflareImage[]
  | {
      images?: CloudflareImage[];
      continuation_token?: string;
    };

type Subcommand = "migrate" | "list";

type CliOptions = {
  command: Subcommand;
  sourceAccountId: string;
  sourceApiToken: string;
  destinationAccountId?: string;
  destinationApiToken?: string;
  pageSize: number;
  concurrency: number;
  dryRun: boolean;
  resume: boolean;
  preserveId: boolean;
  preserveMetadata: boolean;
  preserveSignedUrls: boolean;
  onConflict: "skip" | "error";
  maxRetries: number;
  requestTimeoutMs: number;
  startPage: number;
  stateFile: string;
  apiBaseUrl: string;
  listJson: boolean;
  maxPages?: number;
};

type MigrationStateRow = {
  id: string;
  status: "success" | "skipped" | "failed";
  reason?: string;
  destinationId?: string;
  timestamp: string;
};

class ApiError extends Error {
  constructor(
    message: string,
    readonly status?: number,
    readonly details?: unknown,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

class CloudflareImagesClient {
  constructor(
    private readonly accountId: string,
    private readonly apiToken: string,
    private readonly baseUrl: string,
    private readonly timeoutMs: number,
  ) {}

  async listImages(page: number, perPage: number): Promise<CloudflareEnvelope<CloudflareListResult>> {
    return this.requestJson<CloudflareListResult>("GET", `/accounts/${this.accountId}/images/v1?page=${page}&per_page=${perPage}`);
  }

  async downloadBlob(imageId: string): Promise<Blob> {
    const url = `${this.baseUrl}/accounts/${this.accountId}/images/v1/${encodeURIComponent(imageId)}/blob`;
    const response = await fetch(url, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${this.apiToken}`,
      },
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (!response.ok) {
      const message = await safeReadText(response);
      throw new ApiError(`Failed to download blob for ${imageId}: HTTP ${response.status} ${message}`, response.status, message);
    }

    return response.blob();
  }

  async uploadImage(params: {
    id?: string;
    filename?: string;
    blob: Blob;
    metadata?: JsonObject;
    requireSignedURLs?: boolean;
  }): Promise<CloudflareImage> {
    const form = new FormData();
    if (params.id) {
      form.append("id", params.id);
    }
    if (params.metadata !== undefined) {
      form.append("metadata", JSON.stringify(params.metadata));
    }
    if (params.requireSignedURLs !== undefined) {
      form.append("requireSignedURLs", String(params.requireSignedURLs));
    }

    form.append("file", params.blob, params.filename || "image");

    const envelope = await this.requestJson<CloudflareImage>("POST", `/accounts/${this.accountId}/images/v1`, form);
    return envelope.result;
  }

  private async requestJson<T>(method: string, path: string, body?: FormData): Promise<CloudflareEnvelope<T>> {
    const headers: HeadersInit = {
      Authorization: `Bearer ${this.apiToken}`,
      Accept: "application/json",
    };

    const response = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers,
      body,
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    const json = (await response.json().catch(() => null)) as CloudflareEnvelope<T> | null;

    if (!response.ok) {
      const details = json ?? (await safeReadText(response));
      throw new ApiError(`Cloudflare API request failed (${method} ${path})`, response.status, details);
    }

    if (!json) {
      throw new ApiError(`Cloudflare API returned non-JSON response for ${method} ${path}`);
    }

    if (!json.success) {
      throw new ApiError(
        `Cloudflare API returned success=false for ${method} ${path}`,
        response.status,
        json.errors,
      );
    }

    return json;
  }
}

function printHelp(): void {
  console.log(`cf-images-migrate

Migrate or list Cloudflare Images.

Usage:
  cf-images-migrate [migrate] [options]
  cf-images-migrate list [options]

Subcommands:
  migrate                 Copy images from source account to destination account (default)
  list                    List existing images from source account

Required flags (or env vars):
  --source-account-id        (CF_SOURCE_ACCOUNT_ID)
  --source-api-token         (CF_SOURCE_API_TOKEN)

Required for migrate only:
  --destination-account-id   (CF_DEST_ACCOUNT_ID)
  --destination-api-token    (CF_DEST_API_TOKEN)

Options:
  --page-size <n>            Images per API page (default: 100)
  --start-page <n>           First page to fetch from source (default: 1)
  --max-pages <n>            Max pages to list (list subcommand only)
  --max-retries <n>          Retry attempts per image operation (default: 3)
  --request-timeout-ms <n>   Timeout for each API call (default: 60000)
  --help                     Show this message

Migrate options:
  --concurrency <n>          Concurrent migrations (default: 4)
  --state-file <path>        JSONL state file path (default: .context/cf-images-migration-state.jsonl)
  --on-conflict <mode>       skip|error (default: skip)
  --dry-run                  Show what would migrate, without uploading
  --resume                   Skip IDs that already succeeded/skipped in state file
  --no-preserve-id           Do not preserve source image IDs on destination
  --no-preserve-metadata     Do not copy metadata
  --no-preserve-signed-urls  Do not copy requireSignedURLs flag

List options:
  --json                     Print one JSON object per image (NDJSON)

Examples:
  bun run src/index.ts list
  bun run src/index.ts list --json --max-pages 2
  bun run src/index.ts migrate --dry-run --resume
  bun run src/index.ts --concurrency 8 --page-size 100
`);
}

function parseArgs(argv: string[]): CliOptions {
  const args = [...argv];
  const env = process.env;

  let command: Subcommand = "migrate";
  if (args[0] === "migrate" || args[0] === "list") {
    command = args.shift() as Subcommand;
  } else if (args[0] && !args[0].startsWith("-")) {
    throw new Error(`Unknown subcommand: ${args[0]}`);
  }

  const options: Partial<CliOptions> = {
    command,
    sourceAccountId: env.CF_SOURCE_ACCOUNT_ID,
    sourceApiToken: env.CF_SOURCE_API_TOKEN,
    destinationAccountId: env.CF_DEST_ACCOUNT_ID,
    destinationApiToken: env.CF_DEST_API_TOKEN,
    pageSize: 100,
    concurrency: 4,
    dryRun: false,
    resume: false,
    preserveId: true,
    preserveMetadata: true,
    preserveSignedUrls: true,
    onConflict: "skip",
    maxRetries: 3,
    requestTimeoutMs: 60_000,
    startPage: 1,
    stateFile: ".context/cf-images-migration-state.jsonl",
    apiBaseUrl: "https://api.cloudflare.com/client/v4",
    listJson: false,
  };

  while (args.length > 0) {
    const arg = args.shift();
    if (!arg) {
      continue;
    }

    if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    }

    if (arg === "--dry-run") {
      options.dryRun = true;
      continue;
    }

    if (arg === "--resume") {
      options.resume = true;
      continue;
    }

    if (arg === "--json") {
      options.listJson = true;
      continue;
    }

    if (arg === "--no-preserve-id") {
      options.preserveId = false;
      continue;
    }

    if (arg === "--no-preserve-metadata") {
      options.preserveMetadata = false;
      continue;
    }

    if (arg === "--no-preserve-signed-urls") {
      options.preserveSignedUrls = false;
      continue;
    }

    const [flag, inlineValue] = arg.split("=", 2);
    const value = inlineValue ?? args.shift();

    if (!value) {
      throw new Error(`Missing value for ${flag}`);
    }

    switch (flag) {
      case "--source-account-id":
        options.sourceAccountId = value;
        break;
      case "--source-api-token":
        options.sourceApiToken = value;
        break;
      case "--destination-account-id":
        options.destinationAccountId = value;
        break;
      case "--destination-api-token":
        options.destinationApiToken = value;
        break;
      case "--page-size":
        options.pageSize = parsePositiveInt(value, flag);
        break;
      case "--concurrency":
        options.concurrency = parsePositiveInt(value, flag);
        break;
      case "--state-file":
        options.stateFile = value;
        break;
      case "--start-page":
        options.startPage = parsePositiveInt(value, flag);
        break;
      case "--max-pages":
        options.maxPages = parsePositiveInt(value, flag);
        break;
      case "--max-retries":
        options.maxRetries = parsePositiveInt(value, flag);
        break;
      case "--request-timeout-ms":
        options.requestTimeoutMs = parsePositiveInt(value, flag);
        break;
      case "--on-conflict":
        if (value !== "skip" && value !== "error") {
          throw new Error(`Invalid value for --on-conflict: ${value}`);
        }
        options.onConflict = value;
        break;
      default:
        throw new Error(`Unknown flag: ${flag}`);
    }
  }

  const sourceAccountId = requiredValue(
    options.sourceAccountId,
    "--source-account-id (or CF_SOURCE_ACCOUNT_ID)",
  );
  const sourceApiToken = requiredValue(
    options.sourceApiToken,
    "--source-api-token (or CF_SOURCE_API_TOKEN)",
  );

  if (command === "migrate") {
    const destinationAccountId = requiredValue(
      options.destinationAccountId,
      "--destination-account-id (or CF_DEST_ACCOUNT_ID)",
    );
    const destinationApiToken = requiredValue(
      options.destinationApiToken,
      "--destination-api-token (or CF_DEST_API_TOKEN)",
    );

    return {
      ...options,
      command,
      sourceAccountId,
      sourceApiToken,
      destinationAccountId,
      destinationApiToken,
    } as CliOptions;
  }

  return {
    ...options,
    command,
    sourceAccountId,
    sourceApiToken,
  } as CliOptions;
}

function requiredValue(value: string | undefined, label: string): string {
  if (!value) {
    throw new Error(`Missing required option: ${label}`);
  }
  return value;
}

function parsePositiveInt(value: string, flag: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid value for ${flag}: ${value}`);
  }
  return parsed;
}

async function readCompletedIds(stateFile: string): Promise<Set<string>> {
  if (!existsSync(stateFile)) {
    return new Set();
  }

  const content = await readFile(stateFile, "utf8");
  const ids = new Set<string>();

  for (const line of content.split("\n")) {
    if (!line.trim()) {
      continue;
    }

    try {
      const row = JSON.parse(line) as MigrationStateRow;
      if (row.status === "success" || row.status === "skipped") {
        ids.add(row.id);
      }
    } catch {
      // Ignore malformed rows and continue.
    }
  }

  return ids;
}

function appendStateRow(stateFile: string, row: MigrationStateRow): void {
  mkdirSync(dirname(stateFile), { recursive: true });
  appendFileSync(stateFile, `${JSON.stringify(row)}\n`, "utf8");
}

function isConflictError(error: unknown): boolean {
  if (!(error instanceof ApiError)) {
    return false;
  }

  if (error.status === 409) {
    return true;
  }

  const detailsText = JSON.stringify(error.details ?? "").toLowerCase();
  return detailsText.includes("already exists") || detailsText.includes("duplicate") || detailsText.includes("conflict");
}

function shouldRetry(error: unknown): boolean {
  if (!(error instanceof ApiError)) {
    return true;
  }

  if (error.status === undefined) {
    return true;
  }

  if (error.status === 429) {
    return true;
  }

  return error.status >= 500;
}

async function withRetry<T>(
  fn: () => Promise<T>,
  retries: number,
  label: string,
): Promise<T> {
  let attempt = 0;

  while (true) {
    try {
      return await fn();
    } catch (error) {
      attempt += 1;
      if (attempt > retries || !shouldRetry(error)) {
        throw error;
      }

      const delayMs = 500 * Math.pow(2, attempt - 1);
      log(`Retry ${attempt}/${retries} for ${label} after ${delayMs}ms`);
      await Bun.sleep(delayMs);
    }
  }
}

async function runWithConcurrency<T>(
  items: readonly T[],
  concurrency: number,
  worker: (item: T) => Promise<void>,
): Promise<void> {
  let index = 0;

  const runners = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (true) {
      const current = index;
      index += 1;
      if (current >= items.length) {
        return;
      }
      await worker(items[current]);
    }
  });

  await Promise.all(runners);
}

async function safeReadText(response: Response): Promise<string> {
  try {
    return await response.text();
  } catch {
    return "";
  }
}

function tsvValue(value: unknown): string {
  if (value === undefined || value === null) {
    return "";
  }

  return String(value).replaceAll("\t", " ").replaceAll("\n", " ").replaceAll("\r", " ");
}

function getImagesFromListResult(result: CloudflareListResult | undefined): CloudflareImage[] {
  if (!result) {
    return [];
  }

  if (Array.isArray(result)) {
    return result;
  }

  if (Array.isArray(result.images)) {
    return result.images;
  }

  return [];
}

function log(message: string): void {
  console.log(`${new Date().toISOString()} ${message}`);
}

async function runList(options: CliOptions, sourceClient: CloudflareImagesClient): Promise<void> {
  let page = options.startPage;
  let pagesRead = 0;
  let totalListed = 0;

  if (!options.listJson) {
    console.log("id\tfilename\tuploaded\trequireSignedURLs");
  }

  while (true) {
    if (options.maxPages !== undefined && pagesRead >= options.maxPages) {
      break;
    }

    const listEnvelope = await withRetry(
      () => sourceClient.listImages(page, options.pageSize),
      options.maxRetries,
      `list page ${page}`,
    );

    const images = getImagesFromListResult(listEnvelope.result);
    if (images.length === 0) {
      break;
    }

    for (const image of images) {
      if (options.listJson) {
        console.log(JSON.stringify(image));
      } else {
        console.log(
          [
            tsvValue(image.id),
            tsvValue(image.filename),
            tsvValue(image.uploaded),
            tsvValue(image.requireSignedURLs),
          ].join("\t"),
        );
      }
    }

    totalListed += images.length;
    pagesRead += 1;

    const totalPages = listEnvelope.result_info?.total_pages;
    if (totalPages && page >= totalPages) {
      break;
    }

    if (images.length < options.pageSize && !totalPages) {
      break;
    }

    page += 1;
  }

  console.error(`Listed ${totalListed} source images`);
}

async function runMigrate(
  options: CliOptions,
  sourceClient: CloudflareImagesClient,
  destinationClient: CloudflareImagesClient,
): Promise<void> {
  const completedIds = options.resume ? await readCompletedIds(options.stateFile) : new Set<string>();

  if (options.resume) {
    log(`Loaded ${completedIds.size} already completed IDs from ${options.stateFile}`);
  }

  let page = options.startPage;
  let totalSeen = 0;
  let migrated = 0;
  let skippedResume = 0;
  let skippedConflict = 0;
  let failed = 0;

  while (true) {
    const listEnvelope = await withRetry(
      () => sourceClient.listImages(page, options.pageSize),
      options.maxRetries,
      `list page ${page}`,
    );

    const images = getImagesFromListResult(listEnvelope.result);
    if (images.length === 0) {
      break;
    }

    totalSeen += images.length;
    const totalPages = listEnvelope.result_info?.total_pages;
    log(`Processing page ${page}${totalPages ? `/${totalPages}` : ""} (${images.length} images)`);

    await runWithConcurrency(images, options.concurrency, async (image) => {
      if (options.resume && completedIds.has(image.id)) {
        skippedResume += 1;
        log(`SKIP resume id=${image.id}`);
        return;
      }

      if (options.dryRun) {
        log(`DRY-RUN migrate id=${image.id} filename=${image.filename ?? "(none)"}`);
        return;
      }

      try {
        const blob = await withRetry(
          () => sourceClient.downloadBlob(image.id),
          options.maxRetries,
          `download ${image.id}`,
        );

        const metadata = options.preserveMetadata ? image.meta : undefined;
        const requireSignedURLs = options.preserveSignedUrls ? image.requireSignedURLs : undefined;
        const destinationId = options.preserveId ? image.id : undefined;

        const result = await withRetry(
          () =>
            destinationClient.uploadImage({
              id: destinationId,
              filename: image.filename,
              blob,
              metadata,
              requireSignedURLs,
            }),
          options.maxRetries,
          `upload ${image.id}`,
        );

        migrated += 1;
        completedIds.add(image.id);

        appendStateRow(options.stateFile, {
          id: image.id,
          status: "success",
          destinationId: result.id,
          timestamp: new Date().toISOString(),
        });

        log(`OK id=${image.id} -> destination_id=${result.id}`);
      } catch (error) {
        if (options.onConflict === "skip" && isConflictError(error)) {
          skippedConflict += 1;
          completedIds.add(image.id);

          appendStateRow(options.stateFile, {
            id: image.id,
            status: "skipped",
            reason: "conflict",
            timestamp: new Date().toISOString(),
          });

          log(`SKIP conflict id=${image.id}`);
          return;
        }

        failed += 1;
        appendStateRow(options.stateFile, {
          id: image.id,
          status: "failed",
          reason: String(error),
          timestamp: new Date().toISOString(),
        });

        log(`FAIL id=${image.id} error=${String(error)}`);
      }
    });

    if (totalPages && page >= totalPages) {
      break;
    }

    if (images.length < options.pageSize && !totalPages) {
      break;
    }

    page += 1;
  }

  log(
    `Done total_seen=${totalSeen} migrated=${migrated} skipped_resume=${skippedResume} skipped_conflict=${skippedConflict} failed=${failed} dry_run=${options.dryRun}`,
  );
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2));

  const sourceClient = new CloudflareImagesClient(
    options.sourceAccountId,
    options.sourceApiToken,
    options.apiBaseUrl,
    options.requestTimeoutMs,
  );

  if (options.command === "list") {
    await runList(options, sourceClient);
    return;
  }

  const destinationClient = new CloudflareImagesClient(
    requiredValue(options.destinationAccountId, "destinationAccountId"),
    requiredValue(options.destinationApiToken, "destinationApiToken"),
    options.apiBaseUrl,
    options.requestTimeoutMs,
  );

  await runMigrate(options, sourceClient, destinationClient);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
