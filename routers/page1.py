from fastapi import APIRouter, Response, HTTPException, Depends, Query
from sqlalchemy.orm import Session  
from config.database import local_session
from sqlalchemy import text, func
from typing import List, Optional

router_1 = APIRouter(prefix="/pag1",responses={404:{"message":"No encontrado"}})

def get_db():
    db = local_session()
    try:
        yield db
    finally:
        db.close()

#----------------------------------------------------------------------
#Eficacia de búsqueda

@router_1.get("/search_efficiency")
async def eficacia_de_busqueda(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        base_query_won = """
            SELECT COUNT(*) AS cantidad_busquedas_ganadas
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.modification_date) = YEAR(curdate()) 
            AND s.status_search_id = 3
            AND s.id <> 22
        """
        
        base_query_total = """
            SELECT COUNT(*) AS cantidad_busquedas_ganadas_y_cerradas
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.modification_date) = YEAR(curdate()) 
            AND s.status_search_id IN (2, 3)
            AND s.id <> 22
        """
        
        if client_name:
            base_query_won += " AND c.name = :client_name"
            base_query_total += " AND c.name = :client_name"
        
        if search_name:
            base_query_won += " AND s.name = :search_name"
            base_query_total += " AND s.name = :search_name"
        
        if status_name:
            base_query_won += " AND ss.name = :status_name"
            base_query_total += " AND ss.name = :status_name"

        query_won = text(base_query_won)
        query_total = text(base_query_total)
        
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result_won = db.execute(query_won, params)
        result_total = db.execute(query_total, params)
        
        count_won = result_won.scalar()
        count_total = result_total.scalar()

        if count_total == 0:
            efficiency = 0
        else:
            efficiency = round((count_won / count_total) * 100, 0)

        return {"efficiency": efficiency}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#----------------------------------------------------------------------
#Cantidad de busquedas totales en el año corriente
@router_1.get("/total_search_count")
async def cantidad_de_busquedas_totales(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT 
                COUNT(s.id) AS cantidad_búsquedas
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.modification_date) = YEAR(curdate()) 
            AND s.id <> 22
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        if status_name:
            base_query += " AND ss.name = :status_name"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result = db.execute(query, params)
        count = result.scalar()  # Obtiene un solo valor en lugar de una fila completa

        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#----------------------------------------------------------------------    
#Cantidad de vacantes totales en el año corriente
@router_1.get("/vacancies_current_year")
async def cantidad_vacantes_totales(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT SUM(s.total_vacancies) AS cantidad_vacantes
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.modification_date) = YEAR(curdate()) AND s.id <> 22
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        if status_name:
            base_query += " AND ss.name = :status_name"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result = db.execute(query, params)
        count = result.scalar()

        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 

#----------------------------------------------------------------------    
#Cantidad de busquedas ganadas en el año corriente
@router_1.get("/earned_searchs_current_year")
async def cantidad_busquedas_ganadas(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT COUNT(s.id) AS cantidad_búsquedas_ganadas
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.modification_date) = YEAR(curdate()) AND s.status_search_id = 3 AND s.id <> 22
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        if status_name:
            base_query += " AND ss.name = :status_name"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result = db.execute(query, params)
        count = result.scalar()

        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#----------------------------------------------------------------------    

#Cantidad de busquedas cerradas en el año corriente
@router_1.get("/closed_searchs_current_year")
async def cantidad_busquedas_cerradas(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT COUNT(s.id) AS cantidad_búsquedas_cerradas
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.modification_date) = YEAR(curdate()) AND s.status_search_id = 2 AND s.id <> 22
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        if status_name:
            base_query += " AND ss.name = :status_name"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result = db.execute(query, params)
        count = result.scalar()

        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

#----------------------------------------------------------------------

# Cantidad de búsquedas TRABAJANDO(Abiertas (1) + Stand-by (5) + Hibernando (4))(tarjeta)
@router_1.get("/working_searchs")
async def cantidad_busquedas_trabajando(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT COUNT(s.id) AS cantidad_búsquedas_cerradas
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.modification_date) = YEAR(curdate()) AND s.status_search_id IN (1, 5, 4) AND s.id <> 22
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        if status_name:
            base_query += " AND ss.name = :status_name"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result = db.execute(query, params)
        count = result.scalar()

        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#----------------------------------------------------------------------    
# Cantidad de búsquedas abiertas del 2024
@router_1.get("/open_searchs")
async def cantidad_busquedas_abiertas(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT COUNT(s.id) AS cantidad_búsquedas_abiertas
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.modification_date) = YEAR(curdate()) AND s.status_search_id = 1 AND s.id <> 22
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        if status_name:
            base_query += " AND ss.name = :status_name"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result = db.execute(query, params)
        count = result.scalar()

        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


#----------------------------------------------------------------------    
# Cantidad de búsquedas stand-by del 2024
@router_1.get("/standby_searchs")
async def cantidad_busquedas_standby(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT COUNT(s.id) AS cantidad_búsquedas_standby
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.modification_date) = YEAR(curdate()) AND s.status_search_id = 5 AND s.id <> 22
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        if status_name:
            base_query += " AND ss.name = :status_name"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result = db.execute(query, params)
        count = result.scalar()

        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#----------------------------------------------------------------------
# Cantidad de búsquedas hibernando del 2024
@router_1.get("/hibernating_searchs")
async def cantidad_busquedas_hibernando(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT COUNT(s.id) AS cantidad_búsquedas_hibernando
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.modification_date) = YEAR(curdate()) AND s.status_search_id = 4 AND s.id <> 22
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        if status_name:
            base_query += " AND ss.name = :status_name"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result = db.execute(query, params)
        count = result.scalar()

        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#----------------------------------------------------------------------
# Cantidad de vacantes, en búsquedas GANADAS
@router_1.get("/earned_search_vacancies")
async def cantidad_vacantes_en_busq_ganadas(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT SUM(s.total_vacancies) AS cantidad_vacantes_ganadas
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.modification_date) = YEAR(curdate()) AND s.status_search_id = 3 AND s.id <> 22
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        if status_name:
            base_query += " AND ss.name = :status_name"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result = db.execute(query, params)
        count = result.scalar()

        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


#----------------------------------------------------------------------    
# Cantidad de vacantes, en búsquedas CERRADAS
@router_1.get("/closed_search_vacancies")
async def cantidad_vacantes_en_busq_cerradas(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT SUM(s.total_vacancies) AS cantidad_vacantes_cerradas
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.modification_date) = YEAR(curdate()) AND s.status_search_id = 2 AND s.id <> 22
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        if status_name:
            base_query += " AND ss.name = :status_name"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result = db.execute(query, params)
        count = result.scalar()

        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


#----------------------------------------------------------------------    
# Cantidad de vacantes, en búsquedas TRABAJANDO(Abiertas + Stand-By + Hibernando)
@router_1.get("/working_search_vacancies")
async def cantidad_vacantes_en_busq_trabajando(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT SUM(s.total_vacancies) AS cantidad_vacantes_trabajando
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.modification_date) = YEAR(curdate()) AND s.status_search_id IN (1, 5, 4) AND s.id <> 22
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        if status_name:
            base_query += " AND ss.name = :status_name"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result = db.execute(query, params)
        count = result.scalar()

        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


#----------------------------------------------------------------------    
# Cantidad de vacantes, en búsquedas ABIERTAS
@router_1.get("/open_search_vacancies")
async def cantidad_vacantes_en_busq_abiertas(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT SUM(s.total_vacancies) AS cantidad_vacantes_hibernando
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.modification_date) = YEAR(curdate()) AND s.status_search_id = 1 AND s.id <> 22
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        if status_name:
            base_query += " AND ss.name = :status_name"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result = db.execute(query, params)
        count = result.scalar()

        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


#----------------------------------------------------------------------    
# Cantidad de vacantes, en búsquedas Stand-By
@router_1.get("/stand_search_vacancies")
async def cantidad_vacantes_en_busq_standby(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT SUM(s.total_vacancies) AS cantidad_vacantes_stand_by
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.modification_date) = YEAR(curdate()) AND s.status_search_id = 5 AND s.id <> 22
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        if status_name:
            base_query += " AND ss.name = :status_name"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result = db.execute(query, params)
        count = result.scalar()

        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


#----------------------------------------------------------------------    
# Cantidad de vacantes, en búsquedas HIBERNANDO
@router_1.get("/hibernating_search_vacancies")
async def cantidad_vacantes_en_busq_hibernando(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        base_query = """
            SELECT SUM(s.total_vacancies) AS cantidad_vacantes_hibernando
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE year(s.modification_date) = YEAR(curdate()) AND s.status_search_id = 4 AND s.id <> 22
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
        
        if search_name:
            base_query += " AND s.name = :search_name"
        
        if status_name:
            base_query += " AND ss.name = :status_name"

        query = text(base_query)
        params = {}
        
        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result = db.execute(query, params)
        count = result.scalar()

        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


#----------------------------------------------------------------------    
# Cantidad de búsquedas GANADAS POR MES
@router_1.get("/earned_searchs_per_month")
async def cantidad_busquedas_ganadas_por_mes(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        query_str = """
            SELECT MONTH(s.modification_date) AS mes, COUNT(s.id) AS cantidad_búsquedas_ganadas
            FROM search s
            JOIN client c ON s.client_id = c.id
            JOIN status_search ss ON s.status_search_id = ss.id
            WHERE YEAR(s.modification_date) = YEAR(curdate()) AND s.status_search_id = 3
        """
        
        conditions = []
        if client_name:
            conditions.append("c.name = :client_name")
        
        if search_name:
            conditions.append("s.name = :search_name")
        
        if status_name:
            conditions.append("ss.name = :status_name")
        
        if conditions:
            query_str += " AND " + " AND ".join(conditions)

        query_str += " GROUP BY mes"
        
        query = text(query_str)

        params = {}

        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result = db.execute(query, params)
        rows = result.fetchall()

        # Convertir el resultado en un formato JSON esperado
        results = [{"mes": row[0], "cantidad_búsquedas_ganadas": row[1]} for row in rows]

        return {"results": results}

    except Exception as e: 
        raise HTTPException(status_code=500, detail=str(e))

#----------------------------------------------------------------------
# Tabla 
@router_1.get("/table_client_status_search")
async def tabla(
    client_name: Optional[str] = Query(None), 
    search_name: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    page: int = Query(1, ge=1), #-> con esto lo que hacemos es controlar la página actual que se visualiza; ge=1 define que el valor mínimo es 1
    page_size: int = Query(10, ge=1), #-> con esto lo que hacemos es controlar la cantidad de registros que se 
                                     # visualizan por defecto en la página actual; ge=1 define que el valor mínimo es 1 
    db: Session = Depends(get_db)
):
    try:
        offset = (page - 1) * page_size # Define desde qué registro empieza a mostrar en la página
                                        # La lógica es la siguiente, el offset es el primer registro que se 
                                        # muestra en la page indicada, y el page_size indica cuántos registros 
                                        # mostrar en la página definida
        
        base_query = """
            SELECT 
                s.name AS busqueda, 
                c.name AS cliente, 
                ss.name AS estado, 
                s.date_opening AS fecha_apertura,
                s.total_vacancies AS vacantes, 
                datediff(now(), s.modification_date) AS dias_en_etapa
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE YEAR(s.modification_date) = YEAR(curdate()) AND s.id <> 22
        """
        
        count_query = """
            SELECT COUNT(*)
            FROM search AS s
            INNER JOIN client AS c ON s.client_id = c.id
            INNER JOIN status_search AS ss ON s.status_search_id = ss.id
            WHERE YEAR(s.modification_date) = YEAR(curdate()) AND s.id <> 22
        """
        
        if client_name:
            base_query += " AND c.name = :client_name"
            count_query += " AND c.name = :client_name"
        
        if search_name:
            base_query += " AND s.name = :search_name"
            count_query += " AND s.name = :search_name"
        
        if status_name:
            base_query += " AND ss.name = :status_name"
            count_query += " AND ss.name = :status_name"
        
        base_query += " LIMIT :limit OFFSET :offset"

        query = text(base_query)
        count_query = text(count_query)

        params = {'limit': page_size, 'offset': offset}
        
        if client_name:
            params['client_name'] = client_name
        
        if search_name:
            params['search_name'] = search_name
        
        if status_name:
            params['status_name'] = status_name

        result = db.execute(query, params)
        rows = result.fetchall()

        count_result = db.execute(count_query, params)
        total_count = count_result.scalar()

        results = []
        for row in rows:
            results.append({
                "busqueda": row[0],            
                "cliente": row[1],             
                "estado": row[2],              
                "fecha_apertura": row[3].isoformat(), 
                "vacantes": row[4],            
                "dias_en_etapa": row[5]        
            })

        return {
            "results": results,
            "total": total_count,
            "page": page,
            "page_size": page_size,
            "total_pages": (total_count + page_size - 1) // page_size
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))