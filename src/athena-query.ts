import * as helpers from "./helper";
import type { Athena } from "@aws-sdk/client-athena";

type Options = {
  /**
   * The name of the workgroup in which the query is being started.
   */
  workgroup?: string;

  /**
   * The name of the database used in the query execution.
   * The database must exist in the catalog.
   */
  db?: string;

  /**
   * The name of the data catalog used in the query execution.
   */
  catalog?: string;
};

export class AthenaQuery {
  constructor(
    private readonly athena: Athena,
    private readonly options?: Options,
  ) {}

  /**
   * @see https://github.com/classmethod/athena-query#usage
   *
   * @param sql
   * @param options
   */
  async *query(
    sql: string,
    options?: {
      /**
       * A list of values for the parameters in a query.
       * The values are applied sequentially to the parameters in the query in the order in which the parameters occur.
       */
      executionParameters?: (string | number | BigInt)[];

      /**
       * The maximum number of results (rows) to return in this request.
       *
       * @deprecated We recommend you to use LIMIT clause in SQL.
       * Because even if you set it, athena-query will continue to retrieve results unless you break your for-loop.
       *
       * @see https://docs.aws.amazon.com/athena/latest/ug/select.html#select-parameters
       */
      maxResults?: number;
    },
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
