import looker_sdk
import yaml
import json
from bigquery_sql_parser.query import Query as ParseBigQuery
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

sdk = looker_sdk.init40("looker.ini")

def parse_manifest():

    dbt_objects_dict = {}

    with open('catalog.json') as f:
        data = json.load(f)

        for node in data["nodes"]:
            node_type = node.split('.')[0]
            node_project = node.split('.')[1]
            node_name = node.split('.')[2]
            node_jinja = f"ref('{node_name}')"

            node_metadata = {
                'node_type': node_type,
                'node_project': node_project,
                'node_name': node_name,
                'node_jinja': node_jinja
            }

            sql_object_type = data["nodes"][node]["metadata"]["type"]
            sql_database = data["nodes"][node]["metadata"]["database"]
            sql_schema = data["nodes"][node]["metadata"]["schema"]
            sql_name = data["nodes"][node]["metadata"]["name"]
            sql_full_name = f"{sql_database}.{sql_schema}.{sql_name}"

            dbt_objects_dict[sql_full_name] = node_metadata

    return dbt_objects_dict

class Dashboard:
    def __init__(self, id):
        self.id = id
        self.title = None
        self.creator = None
        self.url = None
        self.sql_table_names = []
        self.exposure = {}
    def get_metadata(self):
        dashboard = sdk.dashboard(dashboard_id=self.id)
        self.title = dashboard.title
        if dashboard.user_name == None or dashboard.user_name == '':
            self.creator = 'LookML Developers'
        else:
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

    dbt_objects = parse_manifest()

    depends_on = []

    for table in my_dash.sql_table_names:
        node_jinja = dbt_objects[table]["node_jinja"]
        depends_on.append(node_jinja)


    exposure = {
        'name': args["dashboard_id"],
        'label': my_dash.title,
        'type': 'dashboard',
        'url': my_dash.url,
        'owner': {'name': my_dash.creator},
        'depends_on': depends_on
    }

    exposures = []
    exposures.append(exposure)

    exposures_json = {
        'version': 2,
        'exposures': exposures
    }

    yaml_output = yaml.dump(exposures_json, sort_keys=False) 
    print(yaml_output)

