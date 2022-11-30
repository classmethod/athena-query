# Athena-Query

**Athena-Query** provide simple interface to get athena query results.

Athena-Query wad inspired and forked from [athena-express](https://github.com/ghdna/athena-express#readme).

> **Warning**
> Athena-Query support aws-sdk v3 only. So if you use aws-sdk v2, we recommend to use [athena-express](https://github.com/ghdna/athena-express#readme).

## Installation

```
npm install athena-query @aws-sdk/client-athena
```

```
yarn add athena-query @aws-sdk/client-athena
```

## Usage

Athena-Query provide [async generator function](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/async_function*).
So we can use it with `for await () {}`,

```ts
import { Athena } from "@aws-sdk/client-athena";
import AthenaQuery from "athena-query";

const athena = new Athena({});
const athenaQuery = new AthenaQuery(athena);

for await (const items of athenaQuery.query("SELECT * FROM waf_logs;")) {
  console.log(items); // You can get all items with pagination by query.
}
```
