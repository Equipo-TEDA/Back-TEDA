from fastapi import FastAPI
from routers.page1 import router_1
from routers.page1_client_filter import router_1_client_filter
from routers.page1_search_filter import router_1_search_filter
from routers.page1_status_search_filter import router_1_status_search_filter
from routers.general_filters import router_filters
from routers.pruebas import prueba



app = FastAPI()
app.include_router(router_filters)
app.include_router(router_1)
app.include_router(router_1_client_filter)
app.include_router(router_1_search_filter)
app.include_router(router_1_status_search_filter)
app.include_router(prueba)



@app.get("/")
async def root():
    return {"message": "Hello World"}