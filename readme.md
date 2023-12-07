## Intro

This is a project born from the 2023 Looker hackathon. It uses the Looker SDK and some metadata from dbt to automatically generate [dbt Exposures](https://docs.getdbt.com/docs/build/exposures).

This lets you keep your dbt exposures up to date without manually writing the YAML for them, or going through the tedious process of figuring out what models your dashboards reference. With your exposures up to date and accurately catloging the models your dashboards reference, when a model or test produces an error, you can easily answer the age-old question of 'Something in dbt broke. What dashboards is this impacting?' 

Once you've updated your exposures, you might consider implementing [Elementary Data](https://github.com/elementary-data/elementary) to enhance your dbt instance's data observability and generate an enhanced lineage graph that lets you see what Looker Dashboards are impacted by the thing your data engineering team just broke.

## Pre-Requisites/Assumptions:

- You have a functioning dbt project, and your dbt project is used to generate the tables and views that are referenced by your Looker dashboards
- You have dbt installed and functioning locally, or a functioning dbt cloud instance, and you can generate your dbt docs and get a hold of a catalog.json file
- You have a Looker API key that has the permissions to retrieve dashboards, merged queries, and compile queries

## Make it go:

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
$ cp ./compiled/catalog.json /directory/where/you/cloned/the/repo

```


Generate an exposure for a dashboard:
```
$ python dbt_exposure_generator.py --dashboard DASHBOARD_ID

```

Generate an exposure for a look:
```
$ python dbt_exposure_generator.py --look LOOK_ID

```

Generate exposures for all dashboards in a folder:
```
$ python dbt_exposure_generator.py --folder FOLDER_ID

```

Generate exposures for dashboards AND looks in a folder:
```
$ python dbt_exposure_generator.py --folder FOLDER_ID --all

```

You can stack arguments, and they all accept multiple arguments too, eg:

```
$ python dbt_exposure_generator.py --folder FOLDER_ID1 FOLDER_ID2 --look LOOK_ID --dashboard DASHBOARD_ID456 DASHBOARD_ID789

```


Caveats:

- SQL parser used is specific to BigQuery. Maybe it will work on other SQL dialects. Probably not. Wanna use it on a Looker project on a different dialect? Find a python SQL parser library for your database and figure it out
