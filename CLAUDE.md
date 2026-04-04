# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
pnpm test          # run tests in watch mode
pnpm test run      # run tests once
pnpm typecheck     # type check (tsc --noEmit)
pnpm build         # build (unbuild → dist/)
```

Run a single test by name:
```bash
pnpm vitest run test/index.test.ts -t "test name"
```

Pre-commit hook runs `pnpm test run` and `pnpm typecheck` in parallel.

## Architecture

This is a small npm library (`@classmethod/athena-query`) that wraps the AWS SDK v3 Athena client and exposes results as an async generator.

Three source files:

- **`src/index.ts`** — re-exports `AthenaQuery` as default
- **`src/athena-query.ts`** — public API: `AthenaQuery` class with single `query()` async generator method
- **`src/helper.ts`** — internal AWS SDK calls and data transformation (not exported in public API)

### Query flow

`AthenaQuery.query(sql, options)` does three things in sequence:

1. `startQueryExecution` — calls `athena.startQueryExecution()`, returns `QueryExecutionId`
2. `waitExecutionCompleted` — polls `athena.getQueryExecution()` every 200ms until state is `SUCCEEDED` or `FAILED`
3. Paginated loop — calls `getQueryResults()` with `NextToken` until exhausted, `yield*`-ing each page's rows

### Data transformation in `helper.ts`

`cleanUpPaginatedDML` strips the header row (only present on the first page, i.e. when `NextToken` is absent) and converts each row to an object keyed by column name.

`addDataType` maps Athena column types to JS types:
- `bigint` → `BigInt`
- `integer`, `tinyint`, `smallint`, `int`, `float`, `double` → `Number`
- `boolean` → `JSON.parse(value.toLowerCase())`
- `json` → `JSON.parse(value)`
- `varchar` and unknown types → `string`
- Missing `VarCharValue` (null in Athena) → key is omitted from the result object

### `executionParameters` quoting

In `athena-query.ts`, parameters are formatted before passing to the SDK:
- `string` → wrapped in single quotes (`'value'`)
- `number` / `bigint` → `.toString()`

### Build output

`unbuild` produces dual CJS/ESM output in `dist/` with types, configured via `package.json` `exports` field.

### Tests

All tests live in `test/index.test.ts` and use `aws-sdk-client-mock` to mock `AthenaClient` without hitting AWS.
