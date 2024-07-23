from fastapi import APIRouter, Response, HTTPException, Depends, Query
from sqlalchemy.orm import Session  
from config.database import local_session
from sqlalchemy import text, func
from typing import List, Optional

router_1_status_search_filter = APIRouter(prefix="/pag1_status_search_filter",responses={404:{"message":"No encontrado"}})

def get_db():
    db = local_session()
    try:
        yield db
    finally:
        db.close()

#-----------------------------------------------------------
#Eficacia de búsquedas
@router_1_status_search_filter.get("/search_efficiency_status_search_filter")
async def search_efficiency_status_search_filter(
    status_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT
                ss.name AS estado,
                ROUND(((SELECT COUNT(*) 
                    FROM search
                    WHERE year(date_opening) = YEAR(curdate()) AND id <> 22 AND status_search_id = 3)
                /
                (SELECT COUNT(*) 
                    FROM search
                    WHERE year(date_opening) = YEAR(curdate()) AND id <> 22 AND status_search_id IN (2,3)))*100, 0) AS eficacia_de_busqueda_2024
            FROM search AS s
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE YEAR(s.date_opening) = YEAR(curdate()) 
            AND s.id <> 22
        """
        
        if status_name:
            base_query += " AND ss.name = :status_name"
        
        base_query += " GROUP BY estado ORDER BY eficacia_de_busqueda_2024 DESC"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "estado": row[0],
                "eficacia_de_busqueda_2024": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#-----------------------------------------------------------
#Cantidad de búsquedas totales

@router_1_status_search_filter.get("/count_searchs_status_search_filter")
async def count_searchs_status_search_filter(
    status_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                ss.name "estado",
                COUNT(s.id) "cantidad_busquedas"
            FROM search AS s
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22
        """
        
        if status_name:
            base_query += " AND ss.name = :status_name"
        
        base_query += " GROUP BY estado ORDER BY cantidad_busquedas DESC"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "estado": row[0],
                "cantidad_busquedas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#-----------------------------------------------------------------------
#Cantidad de vacantes totales

@router_1_status_search_filter.get("/total_vacancies_status_search_filter")
async def total_vacancies_status_search_filter(
    status_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                ss.name "estado",
                SUM(s.total_vacancies) "cantidad_vacantes"
            FROM search AS s
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22
        """
        
        if status_name:
            base_query += " AND ss.name = :status_name"
        
        base_query += " GROUP BY estado ORDER BY cantidad_vacantes DESC"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "estado": row[0],
                "cantidad_vacantes": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#------------------------------------------------------------------
#Cantidad de búsquedas GANADAS

@router_1_status_search_filter.get("/earned_searchs_status_search_filter")
async def earned_searchs_status_search_filter(
    status_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                ss.name "estado",
                COUNT(s.id) "cantidad_busquedas_ganadas"
            FROM search AS s
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 3
        """
        
        if status_name:
            base_query += " AND ss.name = :status_name"
        
        base_query += " GROUP BY estado ORDER BY cantidad_busquedas_ganadas DESC"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "estado": row[0],
                "cantidad_busquedas_ganadas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#-----------------------------------------------------------------
#Cantidad de búsquedas CERRADAS

@router_1_status_search_filter.get("/closed_searchs_status_search_filter")
async def closed_searchs_status_search_filter(
    status_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                ss.name "estado", 
                COUNT(s.id) "cantidad_busquedas_cerradas"
            FROM search AS s
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 2
        """
        
        if status_name:
            base_query += " AND ss.name = :status_name"
        
        base_query += " GROUP BY estado ORDER BY cantidad_busquedas_cerradas DESC"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "estado": row[0],
                "cantidad_busquedas_cerradas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#-------------------------------------------------------------------
# Cantidad de búsquedas TRABAJANDO

@router_1_status_search_filter.get("/working_searchs_status_search_filter")
async def working_searchs_status_search_filter(
    status_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                ss.name "estado", 
                COUNT(s.id) "cantidad_busquedas_trabajando"
            FROM search AS s
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id IN (1, 5, 4)
        """
        
        if status_name:
            base_query += " AND ss.name = :status_name"
        
        base_query += " GROUP BY estado ORDER BY cantidad_busquedas_trabajando DESC"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "estado": row[0],
                "cantidad_busquedas_trabajando": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------
#Cantidad de búsquedas ABIERTAS

@router_1_status_search_filter.get("/open_searchs_status_search_filter")
async def open_searchs_status_search_filter(
    status_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                ss.name "estado", 
                COUNT(s.id) "cantidad_busquedas_abiertas"
            FROM search AS s
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 1
        """
        
        if status_name:
            base_query += " AND ss.name = :status_name"
        
        base_query += " GROUP BY estado ORDER BY cantidad_busquedas_abiertas DESC"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "estado": row[0],
                "cantidad_busquedas_abiertas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------
# Cantidad búsquedas Stand-By

@router_1_status_search_filter.get("/standby_searchs_status_search_filter")
async def standby_searchs_status_search_filter(
    status_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                ss.name "estado", 
                COUNT(s.id) "cantidad_busquedas_standby"
            FROM search AS s
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 5
        """
        
        if status_name:
            base_query += " AND ss.name = :status_name"
        
        base_query += " GROUP BY estado ORDER BY cantidad_busquedas_standby DESC"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "estado": row[0],
                "cantidad_busquedas_standby": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#------------------------------------------------------------------
# Cantidad búsquedas Hibernando

@router_1_status_search_filter.get("/hibernating_searchs_status_search_filter")
async def hibernating_searchs_status_search_filter(
    status_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                ss.name "estado", 
                COUNT(s.id) "cantidad_busquedas_hibernando"
            FROM search AS s
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 4
        """
        
        if status_name:
            base_query += " AND ss.name = :status_name"
        
        base_query += " GROUP BY estado ORDER BY cantidad_busquedas_hibernando DESC"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "estado": row[0],
                "cantidad_busquedas_hibernando": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#----------------------------------------------------------------
#Cantidad de vacantes, en búsquedas GANADAS

@router_1_status_search_filter.get("/earned_search_vacancies_status_search_filter")
async def earned_search_vacancies_status_search_filter(
    status_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                ss.name "estado", 
                sum(s.total_vacancies) "cantidad_vacantes_ganadas"
            FROM search AS s
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 3
        """
        
        if status_name:
            base_query += " AND ss.name = :status_name"
        
        base_query += " GROUP BY estado ORDER BY cantidad_vacantes_ganadas DESC"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "estado": row[0],
                "cantidad_vacantes_ganadas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#----------------------------------------------------------------
#Cantidad de vacantes, en búsquedas CERRADAS

@router_1_status_search_filter.get("/closed_search_vacancies_status_search_filter")
async def closed_search_vacancies_status_search_filter(
    status_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                ss.name "estado", 
                sum(s.total_vacancies) "cantidad_vacantes_cerradas"
            FROM search AS s
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 2
        """
        
        if status_name:
            base_query += " AND ss.name = :status_name"
        
        base_query += " GROUP BY estado ORDER BY cantidad_vacantes_cerradas DESC"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "estado": row[0],
                "cantidad_vacantes_cerradas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#----------------------------------------------------------------
# Cantidad de vacantes, en búsquedas TRABAJANDO

@router_1_status_search_filter.get("/working_search_vacancies_status_search_filter")
async def working_search_vacancies_status_search_filter(
    status_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                ss.name "estado", 
                sum(s.total_vacancies) "cantidad_vacantes_trabajando"
            FROM search AS s
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id IN (1, 5, 4)
        """
        
        if status_name:
            base_query += " AND ss.name = :status_name"
        
        base_query += " GROUP BY estado ORDER BY cantidad_vacantes_trabajando DESC"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "estado": row[0],
                "cantidad_vacantes_trabajando": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------------
# Cantidad de vacantes, en búsquedas ABIERTAS

@router_1_status_search_filter.get("/open_search_vacancies_status_search_filter")
async def open_search_vacancies_status_search_filter(
    status_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                ss.name "estado", 
                sum(s.total_vacancies) "cantidad_vacantes_abiertas"
            FROM search AS s
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 1
        """
        
        if status_name:
            base_query += " AND ss.name = :status_name"
        
        base_query += " GROUP BY estado ORDER BY cantidad_vacantes_abiertas DESC"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "estado": row[0],
                "cantidad_vacantes_abiertas": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------------
# Cantidad de vacantes, en búsquedas Stand-By

@router_1_status_search_filter.get("/standby_search_vacancies_status_search_filter")
async def standby_search_vacancies_status_search_filter(
    status_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                ss.name "estado", 
                sum(s.total_vacancies) "cantidad_vacantes_stand_by"
            FROM search AS s
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 5
        """
        
        if status_name:
            base_query += " AND ss.name = :status_name"
        
        base_query += " GROUP BY estado ORDER BY cantidad_vacantes_stand_by DESC"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "estado": row[0],
                "cantidad_vacantes_stand_by": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------------
# Cantidad de vacantes, en búsquedas HIBERNANDO

@router_1_status_search_filter.get("/hibernating_search_vacancies_status_search_filter")
async def hibernating_search_vacancies_status_search_filter(
    status_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT  
                ss.name "estado", 
                sum(s.total_vacancies) "cantidad_vacantes_hibernando"
            FROM search AS s
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.date_opening) = YEAR(CURDATE()) AND s.id <> 22 AND s.status_search_id = 4
        """
        
        if status_name:
            base_query += " AND ss.name = :status_name"
        
        base_query += " GROUP BY estado ORDER BY cantidad_vacantes_hibernando DESC"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "estado": row[0],
                "cantidad_vacantes_hibernando": row[1]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------------
# Cantidad de búsquedas GANADAS POR MES

@router_1_status_search_filter.get("/hibernating_search_vacancies_status_search_filter")
async def hibernating_search_vacancies_status_search_filter(
    status_name: Optional[str] = Query(None), 
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                ss.name "estado", 
                month(s.date_opening) "mes", 
                COUNT(s.id) "cantidad_búsquedas_ganadas"
            FROM search AS s
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE YEAR(s.date_opening) = YEAR(curdate()) AND s.id <> 22 AND s.status_search_id = 3
        """
        
        if status_name:
            base_query += " AND ss.name = :status_name"
        
        base_query += " GROUP BY estado, mes ORDER BY cantidad_búsquedas_ganadas DESC"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
        result = db.execute(query, params)
        count = result.fetchall()

        results = []
        for row in count:
            results.append({
                "estado": row[0],
                "mes": row[1],
                "cantidad_búsquedas_ganadas": row[2]
            })

        return {"count": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#--------------------------------------------------------------------
# Tabla 

@router_1_status_search_filter.get("/table_search_details_current_year_status_search_filter")
async def table_search_details_current_year_status_search_filter(
    status_name: Optional[str] = Query(None), 
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
        
        if status_name:
            base_query += " AND ss.name = :status_name"

        query = text(base_query)
        params = {}
        
        if status_name:
            params['status_name'] = status_name
        
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