import { beforeEach, test, expect } from "vitest";
import {
  Athena,
  AthenaClient,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
  StartQueryExecutionCommand,
} from "@aws-sdk/client-athena";
import { mockClient } from "aws-sdk-client-mock";
import AthenaQuery from "../src";

const athenaMock = mockClient(AthenaClient);

const athena = new Athena({});

beforeEach(() => {
  athenaMock.reset();
});

test("parse to json following ColumnInfo", async () => {
  athenaMock
    .on(StartQueryExecutionCommand)
    .resolves({ QueryExecutionId: "test-QueryExecutionId" })
    .on(GetQueryExecutionCommand)
    .resolves({ QueryExecution: { Status: { State: "SUCCEEDED" } } })
    .on(GetQueryResultsCommand)
    .resolves({
      ResultSet: {
        ResultSetMetadata: {
          ColumnInfo: [
            { Name: "name", Type: "varchar" },
            { Name: "disabled", Type: "boolean" },
            { Name: "timestamp", Type: "bigint" },
            { Name: "score1", Type: "integer" },
            { Name: "score2", Type: "tinyint" },
            { Name: "score3", Type: "smallint" },
            { Name: "score4", Type: "int" },
            { Name: "rate1", Type: "float" },
            { Name: "rate2", Type: "double" },
            { Name: "metadata", Type: "json" },
          ],
        },
        Rows: [
          {
            // header row
            Data: [
              { VarCharValue: "name" },
              { VarCharValue: "disabled" },
              { VarCharValue: "timestamp" },
              { VarCharValue: "score1" },
              { VarCharValue: "score2" },
              { VarCharValue: "score3" },
              { VarCharValue: "score4" },
              { VarCharValue: "rate1" },
              { VarCharValue: "rate2" },
              { VarCharValue: "metadata" },
            ],
          },
          {
            Data: [
              { VarCharValue: "test-name-1" },
              { VarCharValue: "true" },
              { VarCharValue: "1669718600001" },
              { VarCharValue: "101" },
              { VarCharValue: "102" },
              { VarCharValue: "103" },
              { VarCharValue: "104" },
              { VarCharValue: "1.01" },
              { VarCharValue: "1.02" },
              {
                VarCharValue: JSON.stringify({
                  key1: "value1",
                  key2: "value2",
                }),
              },
            ],
          },
        ],
      },
    });

  const athenaQuery = new AthenaQuery(athena);
  const resultGen = athenaQuery.query("");

  const res1 = await resultGen.next();

  expect(res1.value).toEqual({
    name: "test-name-1",
    disabled: true,
    timestamp: 1669718600001n,
    score1: 101,
    score2: 102,
    score3: 103,
    score4: 104,
    rate1: 1.01,
    rate2: 1.02,
    metadata: { key1: "value1", key2: "value2" },
  });
});

test("wait query completed", async () => {
  athenaMock
    .on(StartQueryExecutionCommand)
    .resolves({ QueryExecutionId: "test-QueryExecutionId" })
    .on(GetQueryExecutionCommand)
    .resolvesOnce({ QueryExecution: { Status: { State: "QUEUED" } } })
    .resolvesOnce({ QueryExecution: { Status: { State: "RUNNING" } } })
    .resolvesOnce({ QueryExecution: { Status: { State: "SUCCEEDED" } } })
    .on(GetQueryResultsCommand)
    .resolves({
      ResultSet: {
        ResultSetMetadata: {
          ColumnInfo: [{ Name: "name", Type: "varchar" }],
        },
        Rows: [
          { Data: [{ VarCharValue: "test-name-1" }] }, // header row
          { Data: [{ VarCharValue: "test-name-1" }] },
        ],
      },
    });

  const athenaQuery = new AthenaQuery(athena);
  const resultGen = athenaQuery.query("");

  const res1 = await resultGen.next();

  expect(res1.done).toBe(false);
  expect(res1.value).toEqual({ name: "test-name-1" });
});

test("get items with generator", async () => {
  athenaMock
    .on(StartQueryExecutionCommand)
    .resolves({ QueryExecutionId: "test-QueryExecutionId" })
    .on(GetQueryExecutionCommand)
    .resolves({ QueryExecution: { Status: { State: "SUCCEEDED" } } })
    .on(GetQueryResultsCommand)
    .resolvesOnce({
      NextToken: "test-NextToken-1",
      ResultSet: {
        ResultSetMetadata: {
          ColumnInfo: [{ Name: "name", Type: "varchar" }],
        },
        Rows: [
          { Data: [{ VarCharValue: "name" }] }, // header row
          { Data: [{ VarCharValue: "test-name-1" }] },
          { Data: [{ VarCharValue: "test-name-2" }] },
        ],
      },
    })
    .resolvesOnce({
      ResultSet: {
        ResultSetMetadata: {
          ColumnInfo: [{ Name: "name", Type: "varchar" }],
        },
        Rows: [{ Data: [{ VarCharValue: "test-name-3" }] }],
      },
    });

  const athenaQuery = new AthenaQuery(athena);
  const queryResultGen = athenaQuery.query("");

  const res1 = await queryResultGen.next();
  expect(res1.done).toBe(false);
  expect(res1.value).toEqual({ name: "test-name-1" });

  const res2 = await queryResultGen.next();
  expect(res2.done).toBe(false);
  expect(res2.value).toEqual({ name: "test-name-2" });

  const res3 = await queryResultGen.next();
  expect(res3.done).toBe(false);
  expect(res3.value).toEqual({ name: "test-name-3" });

  const res4 = await queryResultGen.next();
  expect(res4.done).toBe(true);
  expect(res4.value).toBe(undefined);
});

test("get all item with generator", async () => {
  athenaMock
    .on(StartQueryExecutionCommand)
    .resolves({ QueryExecutionId: "test-QueryExecutionId" })
    .on(GetQueryExecutionCommand)
    .resolves({ QueryExecution: { Status: { State: "SUCCEEDED" } } })
    .on(GetQueryResultsCommand)
    .resolvesOnce({
      NextToken: "test-NextToken-1",
      ResultSet: {
        ResultSetMetadata: {
          ColumnInfo: [{ Name: "name", Type: "varchar" }],
        },
        Rows: [
          { Data: [{ VarCharValue: "name" }] }, // header row
          { Data: [{ VarCharValue: "test-name-1" }] },
          { Data: [{ VarCharValue: "test-name-2" }] },
        ],
      },
    })
    .resolvesOnce({
      NextToken: "test-NextToken-2",
      ResultSet: {
        ResultSetMetadata: {
          ColumnInfo: [{ Name: "name", Type: "varchar" }],
        },
        Rows: [
          { Data: [{ VarCharValue: "test-name-3" }] },
          { Data: [{ VarCharValue: "test-name-4" }] },
        ],
      },
    })
    .resolvesOnce({
      ResultSet: {
        ResultSetMetadata: {
          ColumnInfo: [{ Name: "name", Type: "varchar" }],
        },
        Rows: [{ Data: [{ VarCharValue: "test-name-5" }] }],
      },
    });

  const allItems = [];

  const athenaQuery = new AthenaQuery(athena);
  for await (const item of athenaQuery.query("")) {
    allItems.push(item);
  }

  expect(allItems).toEqual([
    { name: "test-name-1" },
    { name: "test-name-2" },
    { name: "test-name-3" },
    { name: "test-name-4" },
    { name: "test-name-5" },
  ]);
});

test("pass args to sdk", async () => {
  athenaMock
    .on(StartQueryExecutionCommand)
    .resolves({ QueryExecutionId: "test-QueryExecutionId" })
    .on(GetQueryExecutionCommand)
    .resolves({ QueryExecution: { Status: { State: "SUCCEEDED" } } })
    .on(GetQueryResultsCommand)
    .resolves({
      ResultSet: {
        ResultSetMetadata: {
          ColumnInfo: [{ Name: "name", Type: "varchar" }],
        },
        Rows: [
          // header row
          { Data: [{ VarCharValue: "name" }] },
          { Data: [{ VarCharValue: "test-name-1" }] },
        ],
      },
    });

  const athenaQuery = new AthenaQuery(athena, {
    db: "test-db",
    workgroup: "test-workgroup",
    catalog: "test-catalog",
  });
  const resultGen = athenaQuery.query("SELECT test FROM test;", {
    executionParameters: ["test", 123, 456n],
    maxResults: 100,
  });

  await resultGen.next();

  expect(
    athenaMock.commandCalls(StartQueryExecutionCommand)[0].args[0].input,
  ).toEqual({
    QueryString: "SELECT test FROM test;",
    ExecutionParameters: ["'test'", "123", "456"],
    WorkGroup: "test-workgroup",
    QueryExecutionContext: {
      Catalog: "test-catalog",
      Database: "test-db",
    },
  });

  expect(
    athenaMock.commandCalls(GetQueryExecutionCommand)[0].args[0].input,
  ).toEqual({
    QueryExecutionId: "test-QueryExecutionId",
  });

  expect(
    athenaMock.commandCalls(GetQueryResultsCommand)[0].args[0].input,
  ).toEqual({
    QueryExecutionId: "test-QueryExecutionId",
    MaxResults: 100,
  });
});

test("throw exception when query is respond as failed", async () => {
  athenaMock
    .on(StartQueryExecutionCommand)
    .resolves({ QueryExecutionId: "test-QueryExecutionId" })
    .on(GetQueryExecutionCommand)
    .resolves({
      QueryExecution: {
        Status: { State: "FAILED", StateChangeReason: "for-test" },
      },
    });

  const athenaQuery = new AthenaQuery(athena);
  const resultGen = athenaQuery.query("");

  await expect(resultGen.next()).rejects.toThrow("for-test");
});

test("throw exception when query is respond as failed", async () => {
  athenaMock
    .on(StartQueryExecutionCommand)
    .resolves({ QueryExecutionId: undefined });

  const athenaQuery = new AthenaQuery(athena);
  const resultGen = athenaQuery.query("");

  await expect(resultGen.next()).rejects.toThrow(
    "No QueryExecutionId was responded.",
  );
});

test("If empty string is returned from AthenaSDK, it will be returned as an empty string", async () => {
  athenaMock
    .on(StartQueryExecutionCommand)
    .resolves({ QueryExecutionId: "test-QueryExecutionId" })
    .on(GetQueryExecutionCommand)
    .resolves({ QueryExecution: { Status: { State: "SUCCEEDED" } } })
    .on(GetQueryResultsCommand)
    .resolves({
      ResultSet: {
        ResultSetMetadata: {
          ColumnInfo: [
            { Name: "nullValue", Type: "unknown" },
            { Name: "emptyValue", Type: "varchar" },
          ],
        },
        Rows: [
          {
            // header row
            Data: [
              { VarCharValue: "nullValue" },
              { VarCharValue: "emptyValue" },
            ],
          },
          {
            Data: [{}, { VarCharValue: "" }],
          },
        ],
      },
    });

  const athenaQuery = new AthenaQuery(athena);
  const resultGen = athenaQuery.query("");
  const res1 = await resultGen.next();
  expect(res1.value).toEqual({
    // nullValue is removed from the object
    emptyValue: "",
  });
});
