# Athena-Query

![Release](https://github.com/classmethod/athena-query/workflows/release/badge.svg)
![CI](https://github.com/classmethod/athena-query/workflows/CI/badge.svg)
[![npm version](https://img.shields.io/npm/v/@classmethod/athena-query.svg)](https://www.npmjs.com/@classmethod/athena-query)
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/classmethod/athena-query/blob/main/LICENSE)

**Athena-Query** provide simple interface to get athena query results.

Athena-Query was inspired and forked from [athena-express](https://github.com/ghdna/athena-express#readme).

> **Warning**
> Athena-Query support aws-sdk v3 only. So if you use aws-sdk v2, we recommend to use [athena-express](https://github.com/ghdna/athena-express#readme).

## Installation

```
npm install @classmethod/athena-query @aws-sdk/client-athena
```

```
yarn add @classmethod/athena-query @aws-sdk/client-athena
```

## Usage

### Basic Usage

Athena-Query provide [async generator function](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/async_function*).
So we can use it with `for await () {}`,

```ts
import { Athena } from "@aws-sdk/client-athena";
import AthenaQuery from "@classmethod/athena-query";

const athena = new Athena({});
const athenaQuery = new AthenaQuery(athena);

for await (const item of athenaQuery.query("SELECT * FROM waf_logs;")) {
  console.log(item); // You can get all items across pagination.
}
```

And if you break loop out, Athena-Query don't call unnecessary pages of `get-query-result` api.

### Options

When you initialize AthenaQuery class, you can pass options to specify the query target.

```ts
const athenaQuery = new AthenaQuery(athena, {
  db: "test-db",
  workgroup: "test-workgroup",
  catalog: "test-catalog",
});
```

When you query to Athena, you can pass options for query.

```ts
const resultGen = athenaQuery.query(
  `
    SELECT * FROM waf_logs
    WHERE name = ? AND groupId = ? AND score > ?;
  `,
  {
    executionParameters: ["test", 123, 456n],
    maxResults: 100,
  }
);
```

## Release

See [here](https://www.notion.so/athena-query-8d4fd5d098b944028dd9c7066a47ffe4#ee977ecfee9840c09e8d7b5a2ed5d3e3). (private)
