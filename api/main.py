from fastapi import FastAPI

app = FastAPI()


@app.get('/')
async def hello():
    return {'about': 'CDLI API service'}


@app.get('/catalogue/{id}')
def catalogue(id: int):
    return {'id': id}

