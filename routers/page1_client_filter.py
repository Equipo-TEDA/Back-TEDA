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
#-----------------------------------------------------------
#Cantidad de búsquedas totales

@router_1_client_filter.get("/search_current_year_client_filter")
async def search_current_year_client_filter(db: Session = Depends(get_db)):
    try:
        query = text(
                """
                SELECT 
                    c.name "cliente",
                    COUNT(s.id) "cantidad_búsquedas"
                FROM search AS s
                INNER JOIN client AS c ON s.client_id = c.id
                WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22
                GROUP BY cliente
                ;
                """
        )

        result = db.execute(query)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente":row[0],
                "cantidad_búsquedas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#-----------------------------------------------------------------------
#Cantidad de vacantes totales

@router_1_client_filter.get("/total_vacancies_current_year_client_filter")
async def total_vacancies_current_year_client_filter(db: Session = Depends(get_db)):
    try:
        query = text(
                """
                SELECT 
                    c.name "cliente",
                    SUM(s.total_vacancies) "cantidad_vacantes"
                FROM search AS s
                INNER JOIN client AS c ON s.client_id = c.id
                WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22
                GROUP BY cliente
;
                """
        )

        result = db.execute(query)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente":row[0],
                "cantidad_vacantes": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#------------------------------------------------------------------
#Cantidad de búsquedas GANADAS

@router_1_client_filter.get("/earned_searchs_current_year_client_filter")
async def earned_searchs_current_year_client_filter(db: Session = Depends(get_db)):
    try:
        query = text(
                """
                SELECT 
                    c.name "cliente",
                    COUNT(s.id) "cantidad_búsquedas_ganadas"
                FROM search AS s
                INNER JOIN client AS c ON s.client_id = c.id
                WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 3
                GROUP BY cliente
                ;
                """
        )

        result = db.execute(query)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente":row[0],
                "cantidad_búsquedas_ganadas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#-----------------------------------------------------------------
#Cantidad de búsquedas CERRADAS

@router_1_client_filter.get("/closed_searchs_current_year_client_filter")
async def closed_searchs_current_year_client_filter(db: Session = Depends(get_db)):
    try:
        query = text(
                """
                SELECT 
                    c.name "cliente", 
                    COUNT(s.id) "cantidad_búsquedas_cerradas"
                FROM search AS s
                INNER JOIN client AS c ON s.client_id = c.id
                WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 2
                GROUP BY cliente
                ;
                """
        )

        result = db.execute(query)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente":row[0],
                "cantidad_búsquedas_cerradas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#-------------------------------------------------------------------
# Cantidad de búsquedas TRABAJANDO

@router_1_client_filter.get("/working_searchs_current_year_client_filter")
async def working_searchs_current_year_client_filter(db: Session = Depends(get_db)):
    try:
        query = text(
                """
                SELECT 
                    c.name "cliente", 
                    COUNT(s.id) "cantidad_búsquedas_trabajando"
                FROM search AS s
                INNER JOIN client AS c ON s.client_id = c.id
                WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id IN (1, 5, 4)
                GROUP BY cliente
                ;
                """
        )

        result = db.execute(query)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente":row[0],
                "cantidad_búsquedas_trabajando": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------
#Cantidad de búsquedas ABIERTAS

@router_1_client_filter.get("/open_searchs_current_year_client_filter")
async def open_searchs_current_year_client_filter(db: Session = Depends(get_db)):
    try:
        query = text(
                """
                SELECT 
                    c.name "cliente", 
                    COUNT(s.id) "cantidad_búsquedas_abiertas"
                FROM search AS s
                INNER JOIN client AS c ON s.client_id = c.id
                WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 1
                GROUP BY cliente
                ;
                """
        )

        result = db.execute(query)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente":row[0],
                "cantidad_búsquedas_abiertas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------
# Cantidad búsquedas Stand-By

@router_1_client_filter.get("/standby_searchs_current_year_client_filter")
async def standby_searchs_current_year_client_filter(db: Session = Depends(get_db)):
    try:
        query = text(
                """
                SELECT 
                    c.name "cliente", 
                    COUNT(s.id) "cantidad_búsquedas_standby"
                FROM search AS s
                INNER JOIN client AS c ON s.client_id = c.id
                WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 5
                GROUP BY cliente
                ;
                """
        )

        result = db.execute(query)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente":row[0],
                "cantidad_búsquedas_standby": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#------------------------------------------------------------------
# Cantidad búsquedas Hibernando

@router_1_client_filter.get("/hibernating_searchs_current_year_client_filter")
async def hibernating_searchs_current_year_client_filter(db: Session = Depends(get_db)):
    try:
        query = text(
                """
                SELECT 
                    c.name "cliente", 
                    COUNT(s.id) "cantidad_búsquedas_hibernando"
                FROM search AS s
                INNER JOIN client AS c ON s.client_id = c.id
                WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 4
                GROUP BY cliente
                ;
                """
        )

        result = db.execute(query)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente":row[0],
                "cantidad_búsquedas_hibernando": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#----------------------------------------------------------------
#Cantidad de vacantes, en búsquedas GANADAS

@router_1_client_filter.get("/earned_search_vacancies_current_year_client_filter")
async def earned_search_vacancies_current_year_client_filter(db: Session = Depends(get_db)):
    try:
        query = text(
                """
                SELECT 
                    c.name "cliente", 
                    sum(s.total_vacancies) "cantidad_vacantes_ganadas"
                FROM search AS s
                INNER JOIN client AS c ON s.client_id = c.id
                WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 3
                GROUP BY cliente;
                """
        )

        result = db.execute(query)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente":row[0],
                "cantidad_vacantes_ganadas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#----------------------------------------------------------------
#Cantidad de vacantes, en búsquedas CERRADAS

@router_1_client_filter.get("/closed_search_vacancies_current_year_client_filter")
async def closed_search_vacancies_current_year_client_filter(db: Session = Depends(get_db)):
    try:
        query = text(
                """
                SELECT 
                    c.name "cliente", 
                    sum(s.total_vacancies) "cantidad_vacantes_cerradas"
                FROM search AS s
                INNER JOIN client AS c ON s.client_id = c.id
                WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 2
                GROUP BY cliente
                ;
                """
        )

        result = db.execute(query)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente":row[0],
                "cantidad_vacantes_cerradas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#----------------------------------------------------------------
# Cantidad de vacantes, en búsquedas TRABAJANDO

@router_1_client_filter.get("/working_search_vacancies_current_year_client_filter")
async def working_search_vacancies_current_year_client_filter(db: Session = Depends(get_db)):
    try:
        query = text(
                """
                SELECT 
                    c.name "cliente", 
                    sum(s.total_vacancies) "cantidad_vacantes_trabajando"
                FROM search AS s
                INNER JOIN client AS c ON s.client_id = c.id
                WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id IN (1, 5, 4)
                GROUP BY cliente;
                """
        )

        result = db.execute(query)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente":row[0],
                "cantidad_vacantes_trabajando": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------------
# Cantidad de vacantes, en búsquedas ABIERTAS

@router_1_client_filter.get("/open_search_vacancies_current_year_client_filter")
async def open_search_vacancies_current_year_client_filter(db: Session = Depends(get_db)):
    try:
        query = text(
                """
                SELECT 
                    c.name "cliente", 
                    sum(s.total_vacancies) "cantidad_vacantes_abiertas"
                FROM search AS s
                INNER JOIN client AS c ON s.client_id = c.id
                WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 1
                GROUP BY cliente;
                """
        )

        result = db.execute(query)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente":row[0],
                "cantidad_vacantes_abiertas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------------
# Cantidad de vacantes, en búsquedas Stand-By

@router_1_client_filter.get("/standby_search_vacancies_current_year_client_filter")
async def standby_search_vacancies_current_year_client_filter(db: Session = Depends(get_db)):
    try:
        query = text(
                """
                SELECT 
                    c.name "cliente", 
                    sum(s.total_vacancies) "cantidad_vacantes_stand_by"
                FROM search as s
                INNER JOIN client AS c ON s.client_id = c.id
                WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 5
                GROUP BY cliente;
                """
        )

        result = db.execute(query)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente":row[0],
                "cantidad_vacantes_stand_by": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------------
# Cantidad de vacantes, en búsquedas HIBERNANDO

@router_1_client_filter.get("/hibernating_search_vacancies_current_year_client_filter")
async def hibernating_search_vacancies_current_year_client_filter(db: Session = Depends(get_db)):
    try:
        query = text(
                """
                SELECT 
                    c.name "cliente", 
                    sum(s.total_vacancies) "cantidad_vacantes_hibernando"
                FROM search AS s
                INNER JOIN client AS c ON s.client_id = c.id
                WHERE year(s.date_opening) = YEAR(CURDATE()) AND s.id <> 22 AND s.status_search_id = 4
                GROUP BY cliente
                ;
                """
        )

        result = db.execute(query)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente":row[0],
                "cantidad_vacantes_hibernando": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------------
# Cantidad de búsquedas GANADAS POR MES

@router_1_client_filter.get("/earned_searchs_per_month_client_filter")
async def earned_searchs_per_month_client_filter(db: Session = Depends(get_db)):
    try:
        query = text(
                """
                SELECT 
                    c.name "cliente", 
                    month(s.date_opening) "mes", 
                    SUM(s.vacancies) "cantidad_vacantes_cubiertas"
                FROM search AS s
                INNER JOIN client AS c ON s.client_id = c.id
                WHERE YEAR(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 3
                GROUP BY 1, 2;
                """
        )

        result = db.execute(query)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "cliente":row[0],
                "mes": row[1],
                "cantidad_vacantes_cubiertas": row[2]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------------
































