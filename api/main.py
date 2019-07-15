import os

from fastapi import FastAPI
from fastapi import Query
from elasticsearch import Elasticsearch

host = os.environ.get('ELASTICSEARCH_URL', 'localhost')
es = Elasticsearch(host)
index_name = 'cdli-catalogue-2019-07-13'

app = FastAPI()


@app.get('/')
async def hello():
    return {'about': 'CDLI API service'}


@app.get('/search')
def search(
        q: str = Query(
            ..., title='Query string',
            description='String to search for in the database.',
            min_length=2),
        skip: int = 0,
        limit: int = 8,
        ):
    'General text search.'
    return es.search(index=index_name, body={
        'query': {'multi_match': {
            'query': q,
            'fields': [
                'designation',
                'primary_publication',
                'collection',
                'accession_no',
                'museum_no',
                'period',
                'provenence',
                'material'
            ]
        }}
    })


@app.get('/catalogue/{id}')
def catalogue(id: int):
    'Return catalogue metadata for the given P-number.'
    return es.get(index=index_name, id=id)
