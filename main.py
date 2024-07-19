from fastapi import FastAPI
from routers.page1 import router_1
from routers.page1_client_filter import router_1_client_filter
from routers.filters import router_filters


app = FastAPI()
app.include_router(router_1)
app.include_router(router_1_client_filter)
app.include_router(router_filters)


@app.get("/")
async def root():
    return {"message": "Hello World"}