from fastapi import APIRouter, Response, HTTPException, Depends, Query
from sqlalchemy.orm import Session  
from config.database import local_session
from sqlalchemy import text, func
from typing import List, Optional

router_filters = APIRouter(prefix="/router_filters",responses={404:{"message":"No encontrado"}})

def get_db():
    db = local_session()
    try:
        yield db
    finally:
        db.close()

#Acá se van a cargar los filtros "generales" para colocar en los botones de cada filtro
#------------------------------------------------------------------
# Filtro búsqueda.  
@router_filters.get("/lista_nombres_busquedas")
async def lista_nombres_busquedas(db: Session = Depends(get_db)):
    try:
        query = text("""
                    SELECT 
                        name AS busqueda                        
                    FROM search 
                    WHERE YEAR(date_opening) = 2024 AND id <> 22
                    ;
                    """)
        
        result = db.execute(query)
        rows = result.fetchall()

        # Convertir el resultado en un formato JSON esperado
        results = []  # Inicializa una lista vacía para almacenar los resultados en formato JSON.
        for row in rows:
            results.append({
                "busqueda": row[0]        
            })

        return {"results": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#----------------------------------------------------------------------

# Filtro cliente.  
@router_filters.get("/lista_nombres_clientes")
async def lista_nombres_clientes(db: Session = Depends(get_db)):
    try:
        query = text("""
                    SELECT 
                        name AS cliente                        
                    FROM client 
                    ;
                    """)
        
        result = db.execute(query)
        rows = result.fetchall()

        # Convertir el resultado en un formato JSON esperado
        results = []  # Inicializa una lista vacía para almacenar los resultados en formato JSON.
        for row in rows:
            results.append({
                "cliente": row[0]        
            })

        return {"results": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#----------------------------------------------------------------------

# Filtro estado búsqueda.  
@router_filters.get("/lista_nombres_estado_busquedas")
async def lista_nombres_estado_busquedas(db: Session = Depends(get_db)):
    try:
        query = text("""
                    SELECT 
                        name AS estado                        
                    FROM status_search 
                    ;
                    """)
        
        result = db.execute(query)
        rows = result.fetchall()

        # Convertir el resultado en un formato JSON esperado
        results = []  # Inicializa una lista vacía para almacenar los resultados en formato JSON.
        for row in rows:
            results.append({
                "estado": row[0]        
            })

        return {"results": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))