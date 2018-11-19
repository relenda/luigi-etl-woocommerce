# Dev Setup

### Local development environment

Requires: dinghy, docker-compose


### Config requirements

To run the pipeline locally for dev purposes please follow these steps:

Copy the ```.env.example``` file to ```.env``` and set all required settings.

Update ```luigi.cfg``` with dev mail and dev scheduler endpoint url:

```
[core]
default-scheduler-url = http://luigi.docker/

[email]
...
receiver  = YOURNAME@relenda.de
...
```

The BACKUP_SERVER_KEYFILE_CONTENT needs to be a base64 encoded string of the keyfile content.
Otherwise you can just place your public key with the file name ```id``` in the ```.ssh```directory of the project.

### Information to databases used in project

There are two db instances luigi needs to work:

1. PostgresDB - Result DB. Results of the pipeline saved here and also the marker table `table_updates` is saved here to save the states of luigis pipeline steps. The postgres DB is also used afterwards by metabase for visualization.
2. MariaDB - Intermediate DB. Every Woocommerce Dump is imported and transformed here


### Now lets get started

Start mysql, postgres and luigi deamon with docker-compose
```docker-compose up```

Start the luigi server by running the etl.py with docker-compose

```docker-compose run python etl.py```

OR

Start the luigi pipeline you want to develop or test (etl.py or etl_legacy.py) in PyCharm.
The call of the pipeline and its parameters can be found at the end of the file in the ```__main__``` method.