import os

from fastapi import FastAPI
from elasticsearch import Elasticsearch

host = os.environ.get('ELASTICSEARCH_URL', 'localhost')
es = Elasticsearch(host)

app = FastAPI()


@app.get('/')
async def hello():
    return {'about': 'CDLI API service'}


@app.get('/catalogue/{id}')
def catalogue(id: int):
    return es.get(index='cdli-catalogue-2019-07-13', id=id)
