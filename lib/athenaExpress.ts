import * as helpers from "./helper";
import type { Athena } from "@aws-sdk/client-athena";

type Options = {
  db?: string;
  workgroup?: string;
  catalog?: string;
};

export class AthenaExpress {
  constructor(
    private readonly athena: Athena,
    private readonly options?: Options
  ) {}

  async *query(
    sql: string,
    options?: { executionParameters?: string[]; maxResults: number }
  ): AsyncGenerator<Record<string, string | number | BigInt | null>[]> {
    const QueryExecutionId = await helpers.startQueryExecution({
      athena: this.athena,
      sql,
      executionParameters: options?.executionParameters,
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

      yield queryResults.items;

      nextToken = queryResults.nextToken;
    } while (nextToken);
  }
}
