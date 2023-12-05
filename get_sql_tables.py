import looker_sdk
from bigquery_sql_parser.query import Query as ParseBigQuery
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

sdk = looker_sdk.init40("looker.ini")

class Dashboard:
    def __init__(self, id):
        self.id = id
        self.title = None
        self.creator = None
        self.url = None
        self.sql_table_names = []
    def get_metadata(self):
        dashboard = sdk.dashboard(dashboard_id=self.id)
        self.title = dashboard.title
        self.creator = dashboard.user_name
        self.url = dashboard.url

        queries = []

        for element in dashboard.dashboard_elements:
            # regular query dashboard elements
            if element.query != None:
                queries.append({'model': element.query.model, 'view': element.query.view, 'query_id': element.query.id})
            # merge query elements - get ids of queries that make up merged query
            elif element.merge_result_id != None:
                merge_query = sdk.merge_query(merge_query_id=element.merge_result_id)
                for query_id in merge_query.source_queries:
                    queries.append({'model': 'merge_query', 'view': 'merge_query', 'query_id': query_id.query_id})
            ## lookml dashboards use this 'result_maker' object. regular queries for lookml dashboards:
            elif element.result_maker != None and element.result_maker.query_id != None:
                queries.append({'model': element.result_maker.query.model, 'view': element.result_maker.query.view, 'query_id': element.result_maker.query.id})
            ## merge queries for lookml dashboards
            elif element.result_maker != None and element.result_maker.merge_result_id != None:
                merge_query = sdk.merge_query(merge_query_id=element.result_maker.merge_result_id)
                for query_id in merge_query.source_queries:
                    queries.append({'model': 'merge_query', 'view': 'merge_query', 'query_id': query_id.query_id})

        for query in queries:
            query_sql = sdk.run_query(
                query_id = query["query_id"],
                result_format = "sql"
            )
            parsed_query = ParseBigQuery(query_sql)
            table_ids = parsed_query.full_table_ids
            self.sql_table_names.extend(table_ids)
        #dedupe list
        self.sql_table_names = list(dict.fromkeys(self.sql_table_names))

if __name__ == "__main__":

    # return an error if user doesn't pass a dash ID
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("dashboard_id", help="Looker Dashboard ID String")
    args = vars(parser.parse_args())

    my_dash = Dashboard(args["dashboard_id"])
    my_dash.get_metadata()

    print(f"Dashboard Name {my_dash.title}")
    print(f"Dashboard Creator {my_dash.creator}")
    print(f"Dashboard URL {my_dash.url}")
    print(f"Dashboard SQL Table Names {my_dash.sql_table_names}")