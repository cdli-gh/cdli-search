# CDLI Search experiment

This is a experimental search interface for the [Cuneiform Digital
Library Initiative](https://cdli.ucla.edu).
It is based on the [published data export](https://github.com/cdli-gh/data)
from the project.

## Quickstart

To try it out locally, start an Elasticsearch instance either as
a local process, or in a container:

```
sudo docker run -d --name cdli-es -p 9200:9200 -e "discovery.type=single-node" elasticsearch:7.2.0
```

If your instance is running somewhere else, pass the correct
location in the `ELASTICSEACH_URL` environment variable.

Now download a copy of the data files and upload them for indexing.
This will take a few minutes.

```
git clone https://github.com/cdli-gh/data ../cdli-data
pipenv install
pipenv run upload.py
```

Once the upload completes, you can test the search api.

Start the server with:

```
pipenv run uvicorn api.main:app
```

Then try the [example search page](http://localhost:8080/vue/index.html).
Or try a manual query:

```
curl localhost:9200/_search?q=K+162
```
