import type {
  Athena,
  GetQueryResultsCommandOutput,
} from "@aws-sdk/client-athena";

export type AtheneRecordData = Record<string, string | number | BigInt | null>;
type AtheneRecord = AtheneRecordData[];

async function startQueryExecution(params: {
  athena: Athena;
  sql: string;
  executionParameters?: string[];
  workgroup?: string;
  db?: string;
  catalog?: string;
}) {
  const output = await params.athena.startQueryExecution({
    QueryString: params.sql,
    ExecutionParameters: params.executionParameters,
    WorkGroup: params.workgroup || "primary",
    QueryExecutionContext: {
      Database: params.db || "default",
      Catalog: params.catalog,
    },
  });

  if (!output.QueryExecutionId) {
    throw new Error("No QueryExecutionId was responded.");
  }

  return output.QueryExecutionId;
}

async function waitExecutionCompleted(params: {
  athena: Athena;
  QueryExecutionId: string;
}): Promise<void> {
  const data = await params.athena.getQueryExecution({
    QueryExecutionId: params.QueryExecutionId,
  });

  const state = data.QueryExecution?.Status?.State;
  const reason = data.QueryExecution?.Status?.StateChangeReason;

  if (state === "SUCCEEDED") {
    return;
  } else if (state === "FAILED") {
    throw new Error(reason);
  } else {
    await wait(200);
    await waitExecutionCompleted(params);
  }
}

async function getQueryResults(params: {
  athena: Athena;
  MaxResults?: number;
  NextToken?: string;
  QueryExecutionId: string;
}): Promise<{ items: AtheneRecord; nextToken?: string }> {
  const queryResults = await params.athena.getQueryResults({
    QueryExecutionId: params.QueryExecutionId,
    MaxResults: params.MaxResults,
    NextToken: params.NextToken,
  });
  return {
    items: cleanUpPaginatedDML(
      queryResults,
      // If NextToken is not given, ignore first data.
      // Because the first data is header info.
      !params.NextToken
    ),
    nextToken: queryResults.NextToken,
  };
}

function cleanUpPaginatedDML(
  queryResults: GetQueryResultsCommandOutput,
  ignoreFirstData: boolean
): AtheneRecord {
  const dataTypes = getDataTypes(queryResults);
  if (!dataTypes) return [];

  const columnNames = Object.keys(dataTypes);

  const items = queryResults.ResultSet?.Rows?.reduce((acc, { Data }, index) => {
    if (ignoreFirstData && index === 0) return acc;
    if (!Data) return acc;

    const rowObject = Data?.reduce((acc, row, index) => {
      if (row.VarCharValue !== undefined && row.VarCharValue !== null) {
        // use mutable operation for performance
        acc[columnNames[index]] = row.VarCharValue;
      }
      return acc;
    }, {} as Record<string, string>);

    // use mutable operation for performance
    acc.push(addDataType(rowObject, dataTypes));
    return acc;
  }, [] as AtheneRecord);

  return items ?? [];
}

function addDataType(
  input: Record<string, string>,
  dataTypes: Record<string, string>
): AtheneRecordData {
  const updatedObjectWithDataType: Record<
    string,
    null | string | number | BigInt
  > = {};

  for (const key in input) {
    if (input[key] === null || input[key] === undefined) {
      updatedObjectWithDataType[key] = null;
    } else {
      switch (dataTypes[key]) {
        case "varchar":
          updatedObjectWithDataType[key] = input[key];
          break;
        case "boolean":
          updatedObjectWithDataType[key] = JSON.parse(input[key].toLowerCase());
          break;
        case "bigint":
          updatedObjectWithDataType[key] = BigInt(input[key]);
          break;
        case "integer":
        case "tinyint":
        case "smallint":
        case "int":
        case "float":
        case "double":
          updatedObjectWithDataType[key] = Number(input[key]);
          break;
        case "json":
          updatedObjectWithDataType[key] = JSON.parse(input[key]);
          break;
        default:
          updatedObjectWithDataType[key] = input[key];
      }
    }
  }
  return updatedObjectWithDataType;
}

function getDataTypes(
  queryResults: GetQueryResultsCommandOutput
): Record<string, string> | undefined {
  const columnInfoArray = queryResults.ResultSet?.ResultSetMetadata?.ColumnInfo;

  const columnInfoObject = columnInfoArray?.reduce((acc, columnInfo) => {
    if (columnInfo.Name && columnInfo.Type) {
      acc[columnInfo.Name] = columnInfo.Type;
    }
    return acc;
  }, {} as Record<string, string>);

  return columnInfoObject;
}

const wait = (ms: number) => new Promise((res) => setTimeout(res, ms));

export { startQueryExecution, waitExecutionCompleted, getQueryResults };
