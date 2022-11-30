import {
  Athena,
  Datum,
  GetQueryResultsCommandOutput,
} from "@aws-sdk/client-athena";

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
}) {
  const queryResults = await params.athena.getQueryResults({
    QueryExecutionId: params.QueryExecutionId,
    MaxResults: params.MaxResults,
    NextToken: params.NextToken,
  });
  return {
    items: await cleanUpPaginatedDML(queryResults),
    nextToken: queryResults.NextToken,
  };
}

async function cleanUpPaginatedDML(queryResults: GetQueryResultsCommandOutput) {
  const dataTypes = await getDataTypes(queryResults);
  if (!dataTypes) return [];

  const columnNames = Object.keys(dataTypes);
  let unformattedS3RowArray: Datum[] | null = null;
  let formattedArray: Record<string, string | number | BigInt | null>[] = [];

  for (let i = 0; i < (queryResults.ResultSet?.Rows?.length ?? 0); i++) {
    unformattedS3RowArray = queryResults.ResultSet?.Rows?.[i].Data ?? null;

    if (!unformattedS3RowArray) continue;

    const rowObject = unformattedS3RowArray?.reduce((acc, row, index) => {
      if (row.VarCharValue) {
        acc[columnNames[index]] = row.VarCharValue;
      }
      return acc;
    }, {} as Record<string, string>);

    formattedArray.push(addDataType(rowObject, dataTypes));
  }
  return formattedArray;
}

function addDataType(
  input: Record<string, string>,
  dataTypes: Record<string, string>
): Record<string, null | string | number | BigInt> {
  const updatedObjectWithDataType: Record<
    string,
    null | string | number | BigInt
  > = {};

  for (const key in input) {
    if (!input[key]) {
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
        default:
          updatedObjectWithDataType[key] = input[key];
      }
    }
  }
  return updatedObjectWithDataType;
}

async function getDataTypes(
  queryResults: GetQueryResultsCommandOutput
): Promise<Record<string, string> | undefined> {
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
