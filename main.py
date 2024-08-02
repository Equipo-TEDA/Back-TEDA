from fastapi import FastAPI
from routers.page1 import router_1
from routers.general_filters import router_filters

app = FastAPI()

app.include_router(router_filters)
app.include_router(router_1)

@app.get("/")
async def root():
    return {"message": "Hello World"}
