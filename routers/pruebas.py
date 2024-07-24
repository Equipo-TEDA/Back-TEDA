from fastapi import APIRouter, Response, HTTPException, Depends, Query
from sqlalchemy.orm import Session  
from config.database import local_session
from sqlalchemy import text, func
from typing import List, Optional

prueba = APIRouter(prefix="/prueba",responses={404:{"message":"No encontrado"}})

def get_db():
    db = local_session()
    try:
        yield db
    finally:
        db.close()

#---------------------------------------------------------------

