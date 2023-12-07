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

class Folder:
    def __init__(self, id):
        self.id = id
        self.dashboards = []
        self.looks = []
        self.folder_found = True

        folder = next(iter(sdk.search_folders(id=self.id)), None)
        ## the 'lookml' folder is a pseudo-folder that contains all lookml dashboards
        ## it isn't found when searching folders, but it always exists, so we can bypass the check for it
        if not folder and self.id != 'lookml':
            self.folder_found = False
            return
    def get_dashboards_in_folder(self):
        dashboards = sdk.folder_dashboards(folder_id=self.id, fields="id")
        for dashboard in dashboards:
            self.dashboards.append(dashboard["id"])
    def get_looks_in_folder(self):
        looks = sdk.folder_looks(folder_id=self.id, fields="id")
        for look in looks:
            self.looks.append(look["id"])

class Look:
    def __init__(self, id):
        self.id = id
        self.title = None
        self.creator = None
        self.url = None
        self.sql_table_names = []
        self.exposure = {}
        self.models_not_found = []
        self.look_found = True
    def get_metadata(self):
        look = next(iter(sdk.search_looks(id=self.id)), None)
        if not look:
            self.look_found = False
            return
        self.title = look.title
        if look.user_name == None or dashboard.user_name == '':
            self.creator = 'Unknown'
        else:
            self.creator = look.user_id
        self.url = look.url

        if look.query != None:
            query_sql = sdk.run_query(
                query_id = look.query["query_id"],
                result_format = "sql"
            )
            parsed_query = ParseBigQuery(query_sql)
            table_ids = parsed_query.full_table_ids
            self.sql_table_names.extend(table_ids)
        #dedupe list
        self.sql_table_names = list(dict.fromkeys(self.sql_table_names))

    def generate_exposure(self, dbt_objects):
        depends_on = []

        for table in self.sql_table_names:
            try:
                node_jinja = dbt_objects[table]["node_jinja"]
                depends_on.append(node_jinja)
            except KeyError:
                self.models_not_found.append({'table':table,'type':'dashboard','content_id':self.id})

        if len(depends_on) < 1:
            self.exposure = {
                'name': self.id,
                'label': self.title,
                'type': 'dashboard',
                'url': self.url,
                'owner': {'name': self.creator},
            }
        else:
            self.exposure = {
                'name': self.id,
                'label': self.title,
                'type': 'dashboard',
                'url': self.url,
                'owner': {'name': self.creator},
                'depends_on': depends_on
            }
    

class Dashboard:
    def __init__(self, id):
        self.id = id
        self.title = None
        self.creator = None
        self.url = None
        self.sql_table_names = []
        self.exposure = {}
        self.models_not_found = []
        self.dashboard_found = True
    def get_metadata(self):
        dashboard = next(iter(sdk.search_dashboards(id=self.id)), None)
        ## lookml dashboards don't seem to show up in search
        if not dashboard and '::' in self.id:
            try:
                dashboard = sdk.dashboard(dashboard_id=self.id)
            except:
                self.dashboard_found = False
                return
        elif not dashboard:
            self.dashboard_found = False
            return
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

    def generate_exposure(self, dbt_objects):
        depends_on = []

        for table in self.sql_table_names:
            try:
                node_jinja = dbt_objects[table]["node_jinja"]
                depends_on.append(node_jinja)
            except KeyError:
                self.models_not_found.append({'table':table,'type':'dashboard','content_id':self.id})

        if len(depends_on) < 1:
            self.exposure = {
                'name': self.id,
                'label': self.title,
                'type': 'dashboard',
                'url': self.url,
                'owner': {'name': self.creator},
            }
        else:
            self.exposure = {
                'name': self.id,
                'label': self.title,
                'type': 'dashboard',
                'url': self.url,
                'owner': {'name': self.creator},
                'depends_on': depends_on
            }




if __name__ == "__main__":

    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--dashboard", nargs="+", help="Looker Dashboard IDs")
    parser.add_argument("-f", "--folder", nargs="+", help="Looker Folder IDs")


    args = vars(parser.parse_args())

    dbt_objects = parse_manifest()

    exposures = []
    exposures_with_no_models= []
    dashboards = []
    if args["dashboard"] != None:
        dashboards.extend(args["dashboard"])
    folders = []
    if args["folder"] != None:
        folders.extend(args["folder"])
    dashboards_not_found = []
    models_not_found = [] 
    folders_not_found = []

    for folder in folders:
        folder=Folder(folder)
        if folder.folder_found == False:
            folders_not_found.append(folder.id)
            continue
        else:
            folder.get_dashboards_in_folder()
            dashboards.extend(folder.dashboards)

    for dashboard in dashboards:
        dashboard = Dashboard(dashboard)
        dashboard.get_metadata()
        if dashboard.dashboard_found == False:
            dashboards_not_found.append(dashboard.id)
            continue
        else:
            dashboard.generate_exposure(dbt_objects)
            if "depends_on" in dashboard.exposure:
                exposures.append(dashboard.exposure)
            else:
                exposures_with_no_models.append(dashboard.exposure)
            models_not_found.extend(dashboard.models_not_found)

    exposures_json = {
        'version': 2,
        'exposures': exposures
    }

    if len(exposures_json["exposures"])>0:
        yaml_output = yaml.dump(exposures_json, sort_keys=False) 
        print(yaml_output)
    else:
        print("No exposures generated!")

    if len(models_not_found) > 0:
        print('-----')
        print('Could not resolve dbt models for the follwing tables/views:')
        for model in models_not_found:
            print(f"- Content ID: {model['content_id']}; Content Type: {model['type']}; Table Name: {model['table']}")
        print('Verify that your catalog.json file is up to date and in the same folder as this program.')
        print('If the model still cannot be resolved, the table may be generated by a different dbt project or a different tool altogether.')
        print('-----')
    if len(folders_not_found) > 0:
        print('-----')        
        print('Could not resolve the following folders:')
        for folder in folders_not_found:
            print(f"- Folder ID: {folder}")
        print('-----')
    if len(dashboards_not_found) > 0:
        print('-----')
        print('Could not resolve the following dashboards:')
        for dashboard in dashboards_not_found:
            print(f"- Dashboard ID: {dashboard}")
        print('Verify that:')
        print('- Your looker.ini file has the correct credentials and that you can access the API succesfully.')
        print('- The user account and permission level associated with your API credential can access the dashboard.')
        print('- The dashboard exists and there are no typos in the name.')
        print('-----')
    if len(exposures_with_no_models) > 0:
        print('-----')
        print("These generated exposures didn't have any dbt models associated with them - there's no benefit to including them in your dbt project, but here they are in case you want them for something:")
        yaml_output = yaml.dump(exposures_with_no_models, sort_keys=False) 
        print(yaml_output)
        print('-----')



