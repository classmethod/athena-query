import * as helpers from "./helper";
import type { Athena } from "@aws-sdk/client-athena";

type Options = {
  db?: string;
  workgroup?: string;
  catalog?: string;
};

export class AthenaQuery {
  constructor(
    private readonly athena: Athena,
    private readonly options?: Options
  ) {}

  async *query(
    sql: string,
    options?: {
      executionParameters?: (string | number | BigInt)[];
      maxResults?: number;
    }
  ): AsyncGenerator<helpers.AtheneRecordData, void, undefined> {
    const QueryExecutionId = await helpers.startQueryExecution({
      athena: this.athena,
      sql,
      executionParameters: options?.executionParameters?.map((param) => {
        const typeOfParam = typeof param;
        switch (typeOfParam) {
          case "bigint":
          case "number":
            return param.toString();
          case "string":
            return `'${param}'`;
          default:
            throw new Error(`${typeOfParam} type is not allowed.`);
        }
      }),
      ...this.options,
    });

    await helpers.waitExecutionCompleted({
      athena: this.athena,
      QueryExecutionId,
    });

    let nextToken: string | undefined;

    do {
      const queryResults = await helpers.getQueryResults({
        athena: this.athena,
        NextToken: nextToken,
        MaxResults: options?.maxResults,
        QueryExecutionId,
      });

      yield* queryResults.items;

      nextToken = queryResults.nextToken;
    } while (nextToken);
  }
}
