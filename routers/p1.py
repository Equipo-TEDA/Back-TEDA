from fastapi import APIRouter, Response, HTTPException, Depends, Query
from sqlalchemy.orm import Session  
from config.database import local_session
from sqlalchemy import text, func
from typing import List, Optional

router_1_client_filter = APIRouter(prefix="/pag1_client_filter",responses={404:{"message":"No encontrado"}})

def get_db():
    db = local_session()
    try:
        yield db
    finally:
        db.close()

#-----------------------------------------------------------

#Eficacia de búsquedas
@router_1_client_filter.get("/search_efficiency_client_filter")
async def search_efficiency_client_filter(db: Session = Depends(get_db)):
    try:
        query = text("""
                    SELECT
                        c.name "cliente",
	                    ROUND(((SELECT COUNT(*) "cantidad_busquedas_ganadas"
		                        FROM search
		                        WHERE year(date_opening) = YEAR(curdate()) AND status_search_id = 3 AND id <> 22)
	                /
                        (SELECT COUNT(*) "cantidad_busquedas_ganadas_+_cerradas"
		                    FROM search
		                    WHERE year(date_opening) = YEAR(curdate()) AND status_search_id IN (2,3) AND id <> 22))*100, 0) "eficacia_de_busqueda_2024"
                    FROM search AS s
                    INNER JOIN client AS c ON s.client_id = c.id
                    GROUP BY cliente
                    ;
                    """)
        result = db.execute(query)
        efficiency = result.fetchall()

        results = []
        for row in efficiency:
            results.append({
                "cliente":row[0],
                "eficacia_de_busqueda_2024": row[1]
            })

        return {"efficiency": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))