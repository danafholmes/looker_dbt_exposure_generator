import looker_sdk
import os
from bigquery_sql_parser.query import Query as ParseBigQuery


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
            if element.query != None:
                queries.append({'model': element.query.model, 'view': element.query.view, 'query_id': element.query.id})
            elif element.merge_result_id != None:
                merge_query = sdk.merge_query(merge_query_id=element.merge_result_id)
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

my_dash = Dashboard("3013")
my_dash.get_metadata()



# # response = sdk.run_query(
# #     query_id="6118409",
# #     result_format="sql")

# # query = Query(response)
# # print(query.full_table_ids)


# ## merge queries

# # for element in response:
# #     print(element.merge_result_id)

# # response = sdk.merge_query(merge_query_id="5MU66waofvlfZVwtwO2Ixr")

# # for source_query in response.source_queries:
# #     print(source_query.query_id)

# #     query = sdk.query(query_id=source_query.query_id)
                      
# #     print(query.view)
