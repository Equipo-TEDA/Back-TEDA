from fastapi import APIRouter, Response, HTTPException, Depends, Query
from sqlalchemy.orm import Session  
from config.database import local_session
from sqlalchemy import text, func
from typing import List, Optional

router_1_search_filter = APIRouter(prefix="/pag1_search_filter",responses={404:{"message":"No encontrado"}})

def get_db():
    db = local_session()
    try:
        yield db
    finally:
        db.close()

#-----------------------------------------------------------
#Eficacia de búsquedas
@router_1_search_filter.get("/search_efficiency_search_filter")
async def search_efficiency_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT
                s.name AS busqueda,
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
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        base_query += " GROUP BY busqueda"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "eficacia_de_busqueda_2024": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#-----------------------------------------------------------
#Cantidad de búsquedas totales

@router_1_search_filter.get("/search_current_year_search_filter")
async def search_current_year_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                s.name AS busqueda,
                COUNT(s.id) AS cantidad_búsquedas
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE year(s.date_opening) = YEAR(curdate()) 
            AND s.id <> 22
        """
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        base_query += " GROUP BY busqueda"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "cantidad_búsquedas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#-----------------------------------------------------------------------
#Cantidad de vacantes totales

@router_1_search_filter.get("/total_vacancies_current_year_search_filter")
async def total_vacancies_current_year_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                s.name "busqueda",
                SUM(s.total_vacancies) "cantidad_vacantes"
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22
        """
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        base_query += " GROUP BY busqueda"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "cantidad_vacantes": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#------------------------------------------------------------------
#Cantidad de búsquedas GANADAS

@router_1_search_filter.get("/earned_searchs_current_year_search_filter")
async def earned_searchs_current_year_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                s.name "busqueda",
                COUNT(s.id) "cantidad_búsquedas_ganadas"
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 3
        """
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        base_query += " GROUP BY busqueda"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "cantidad_búsquedas_ganadas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#-----------------------------------------------------------------
#Cantidad de búsquedas CERRADAS

@router_1_search_filter.get("/closed_searchs_current_year_search_filter")
async def closed_searchs_current_year_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                s.name "busqueda", 
                COUNT(s.id) "cantidad_búsquedas_cerradas"
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 2
        """
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        base_query += " GROUP BY busqueda"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "cantidad_búsquedas_cerradas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#-------------------------------------------------------------------
# Cantidad de búsquedas TRABAJANDO

@router_1_search_filter.get("/working_searchs_current_year_search_filter")
async def working_searchs_current_year_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                s.name "busqueda", 
                COUNT(s.id) "cantidad_búsquedas_trabajando"
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id IN (1, 5, 4)
        """
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        base_query += " GROUP BY busqueda"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "cantidad_búsquedas_trabajando": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------
#Cantidad de búsquedas ABIERTAS

@router_1_search_filter.get("/open_searchs_current_year_search_filter")
async def open_searchs_current_year_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                s.name "busqueda", 
                COUNT(s.id) "cantidad_búsquedas_abiertas"
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 1
        """
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        base_query += " GROUP BY busqueda"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "cantidad_búsquedas_abiertas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------
# Cantidad búsquedas Stand-By

@router_1_search_filter.get("/standby_searchs_current_year_search_filter")
async def standby_searchs_current_year_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                s.name "busqueda", 
                COUNT(s.id) "cantidad_búsquedas_standby"
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 5
        """
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        base_query += " GROUP BY busqueda"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "cantidad_búsquedas_standby": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#------------------------------------------------------------------
# Cantidad búsquedas Hibernando

@router_1_search_filter.get("/hibernating_searchs_current_year_search_filter")
async def hibernating_searchs_current_year_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                s.name "busqueda", 
                COUNT(s.id) "cantidad_búsquedas_hibernando"
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 4
        """
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        base_query += " GROUP BY busqueda"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "cantidad_búsquedas_hibernando": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#----------------------------------------------------------------
#Cantidad de vacantes, en búsquedas GANADAS

@router_1_search_filter.get("/earned_search_vacancies_current_year_search_filter")
async def earned_search_vacancies_current_year_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                s.name "busqueda", 
                sum(s.total_vacancies) "cantidad_vacantes_ganadas"
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 3
        """
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        base_query += " GROUP BY busqueda"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "cantidad_vacantes_ganadas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#----------------------------------------------------------------
#Cantidad de vacantes, en búsquedas CERRADAS

@router_1_search_filter.get("/closed_search_vacancies_current_year_search_filter")
async def closed_search_vacancies_current_year_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                s.name "busqueda", 
                sum(s.total_vacancies) "cantidad_vacantes_cerradas"
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 2
        """
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        base_query += " GROUP BY busqueda"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "cantidad_vacantes_cerradas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#----------------------------------------------------------------
# Cantidad de vacantes, en búsquedas TRABAJANDO

@router_1_search_filter.get("/working_search_vacancies_current_year_search_filter")
async def working_search_vacancies_current_year_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                s.name "busqueda", 
                sum(s.total_vacancies) "cantidad_vacantes_trabajando"
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id IN (1, 5, 4)
        """
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        base_query += " GROUP BY busqueda"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "cantidad_vacantes_trabajando": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------------
# Cantidad de vacantes, en búsquedas ABIERTAS

@router_1_search_filter.get("/open_search_vacancies_current_year_search_filter")
async def open_search_vacancies_current_year_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                s.name "busqueda", 
                sum(s.total_vacancies) "cantidad_vacantes_abiertas"
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 1
        """
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        base_query += " GROUP BY busqueda"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "cantidad_vacantes_abiertas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------------
# Cantidad de vacantes, en búsquedas Stand-By

@router_1_search_filter.get("/standby_search_vacancies_current_year_search_filter")
async def standby_search_vacancies_current_year_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                s.name "busqueda", 
                sum(s.total_vacancies) "cantidad_vacantes_stand_by"
            FROM search as s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 5
        """
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        base_query += " GROUP BY busqueda"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "cantidad_vacantes_stand_by": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------------
# Cantidad de vacantes, en búsquedas HIBERNANDO

@router_1_search_filter.get("/hibernating_search_vacancies_current_year_search_filter")
async def hibernating_search_vacancies_current_year_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                s.name "busqueda", 
                sum(s.total_vacancies) "cantidad_vacantes_hibernando"
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE year(s.date_opening) = YEAR(CURDATE()) AND s.id <> 22 AND s.status_search_id = 4
        """
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        base_query += " GROUP BY busqueda"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "cantidad_vacantes_hibernando": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------------
# Cantidad de búsquedas GANADAS POR MES

@router_1_search_filter.get("/earned_search_per_month_search_filter")
async def earned_search_per_month_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                s.name "busqueda", 
                month(s.date_opening) "mes", 
                SUM(s.vacancies) "cantidad_vacantes_cubiertas"
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            WHERE YEAR(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 3
        """
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        base_query += " GROUP BY busqueda, mes"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "mes": row[1],
                "cantidad_vacantes_cubiertas": row[2]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------------
# Tabla 

@router_1_search_filter.get("/table_search_details_current_year_search_filter")
async def table_search_details_current_year_search_filter(
    search_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                s.name AS busqueda, 
                c.name AS cliente, 
                ss.name AS estado, 
                s.date_opening AS fecha_apertura,
                s.total_vacancies AS vacantes, 
                datediff(now(), s.date_opening) AS dias_en_etapa
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE YEAR(s.date_opening) = YEAR(curdate()) 
            AND s.id <> 22
        """
        
        if search_name:
            base_query += " AND s.name = :search_name"

        query = text(base_query)
        params = {}
        
        if search_name:
            params['search_name'] = search_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "busqueda": row[0],
                "cliente": row[1],
                "estado": row[2],
                "fecha_apertura": row[3],
                "vacantes": row[4],
                "dias_en_etapa": row[5]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#------------------------------------------------------------------