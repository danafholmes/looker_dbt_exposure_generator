Instructions

Clone the repo to your local machine

Install requirements:

```
$ pip install -r requirements.txt
```
Create a [looker.ini file](https://developers.looker.com/api/getting-started)

Generate a catalog.json file

```
$ cd your/dbt/dir
$ dbt docs generate
$ cp ./compiled/catalog.json /directory/where/you/cloned/the/repo/

```
Run the application

```
$ cd /directory/where/you/cloned/the/repo/
$ python generate_exposure_from_dash.py DASHBOARD_ID

Caveats:

- SQL parser used is specific to BigQuery. Maybe it will work on other SQL dialects. Probably not. Wanna use it on a Looker project on a different dialect? Find a python SQL parser library for your database and figure it out

To Do: 

- Add ability to parse a look
- Add ability to pass multiple dashboards to be parsed or a directory to parse all dashboards from that directory
