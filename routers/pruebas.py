from fastapi import APIRouter, Response, HTTPException, Depends, Query
from sqlalchemy.orm import Session  
from config.database import local_session
from sqlalchemy import text, func
from typing import List, Optional

prueba1_filter = APIRouter(prefix="/pag1_prueba",responses={404:{"message":"No encontrado"}})

def get_db():
    db = local_session()
    try:
        yield db
    finally:
        db.close()

#--------------------------------
# Cantidad de búsquedas TRABAJANDO

@prueba1_filter.get("/working_searchs_current_year_client_filter")
async def working_searchs_current_year_client_filter(
    client_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                c.name AS cliente, 
                COUNT(s.id) AS cantidad_búsquedas_trabajando
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE year(s.date_opening) = YEAR(curdate()) 
            AND s.id <> 22 
            AND s.status_search_id IN (1, 5, 4)
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        base_query += " GROUP BY cliente"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente": row[0],
                "cantidad_búsquedas_trabajando": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#--------------------------------    
#Eficacia de búsquedas
@prueba1_filter.get("/search_efficiency_client_filter")
async def search_efficiency_client_filter(
    client_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT
                c.name AS cliente,
                ROUND(((SELECT COUNT(*) 
                    FROM search
                    WHERE year(date_opening) = YEAR(curdate()) AND id <> 22 AND status_search_id = 3)
                /
                (SELECT COUNT(*) 
                    FROM search
                    WHERE year(date_opening) = YEAR(curdate()) AND id <> 22 AND status_search_id IN (2,3)))*100, 0) AS eficacia_de_busqueda_2024
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE 1=1
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        base_query += " GROUP BY cliente"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente": row[0],
                "eficacia_de_busqueda_2024": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#-----------------------------------------------------------
#Cantidad de búsquedas totales

@prueba1_filter.get("/search_current_year_client_filter")
async def search_current_year_client_filter(
    client_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                c.name "cliente",
                COUNT(s.id) "cantidad_búsquedas"
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22
            
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        base_query += " GROUP BY cliente"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente": row[0],
                "cantidad_búsquedas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#----------------------------------------------



















































































