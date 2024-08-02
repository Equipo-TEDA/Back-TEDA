from fastapi import FastAPI
from routers.page1 import router_1

app = FastAPI()
app.include_router(router_1)

@app.get("/")
async def root():
    return {"message": "Hello Mundo"}

from routers.general_filters import router_filters
from routers.pruebas import prueba



app = FastAPI()
app.include_router(router_filters)
app.include_router(router_1)
app.include_router(prueba)



@app.get("/")
async def root():
    return {"message": "Hello World"}
