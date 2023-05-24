from fastapi import FastAPI

from endpoints import generic_data

app = FastAPI()
app.include_router(generic_data.router)


@app.get("/")
async def root():
    return {"Hello": "World"}
