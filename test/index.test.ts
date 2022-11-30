import {
  Athena,
  AthenaClient,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
  StartQueryExecutionCommand,
} from "@aws-sdk/client-athena";
import { mockClient } from "aws-sdk-client-mock";
import AthenaQuery from "..";

const athenaMock = mockClient(AthenaClient);

const athena = new Athena({});
let athenaQuery: AthenaQuery;

beforeEach(() => {
  athenaMock.reset();
  athenaQuery = new AthenaQuery(athena);
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
          ],
        },
        Rows: [
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
            ],
          },
        ],
      },
    });

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
        Rows: [{ Data: [{ VarCharValue: "test-name-1" }] }],
      },
    });

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

  const resultGen = athenaQuery.query("");

  await expect(resultGen.next()).rejects.toThrow("for-test");
});

test("throw exception when query is respond as failed", async () => {
  athenaMock
    .on(StartQueryExecutionCommand)
    .resolves({ QueryExecutionId: undefined });

  const resultGen = athenaQuery.query("");

  await expect(resultGen.next()).rejects.toThrow(
    "No QueryExecutionId was responded."
  );
});
