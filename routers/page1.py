from fastapi import APIRouter, Response, HTTPException, Depends
from sqlalchemy.orm import Session  
from config.database import local_session
from sqlalchemy import text, func
from typing import List

router_1 = APIRouter(prefix="/pag1",responses={404:{"message":"No encontrado"}})

def get_db():
    db = local_session()
    try:
        yield db
    finally:
        db.close()

#Eficacia de búsquedas
@router_1.get("/search_efficiency")
async def search_efficiency(db: Session = Depends(get_db)):
    try:
        query = text("""
                    SELECT
                        ROUND(((SELECT COUNT(*) "cantidad_busquedas_ganadas"
                                FROM search
                                WHERE year(date_opening) = YEAR(curdate()) AND status_search_id = 3 AND id <> 22)
                    /
                        (SELECT COUNT(*) "cantidad_busquedas_ganadas_+_cerradas"
                            FROM search
                            WHERE year(date_opening) = YEAR(curdate()) AND status_search_id IN (2,3) AND id <> 22))*100, 0) "eficacia_de_busqueda_2024"
                    ;
                    """)
        result = db.execute(query)
        efficiency = result.scalar()

        return {"efficiency": efficiency}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#Cantidad de busquedas totales en el año corriente
@router_1.get("/search_current_year")
async def search_current_year(db: Session = Depends(get_db)):
    try:
        query = text("""
                        SELECT COUNT(id) "cantidad_búsquedas_2024"
                        FROM search
                        WHERE year(date_opening) = 2024;
                    """)
        result = db.execute(query)
        count = result.scalar()

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#Cantidad de vacantes totales en el año corriente
@router_1.get("/vacancies_current_year")
async def vacancies_current_year(db: Session = Depends(get_db)):
    try:
        query = text("""
                    SELECT SUM(total_vacancies) "cantidad_vacantes_2024"
                    FROM search
                    WHERE year(date_opening) = 2024;
                    """)
        result = db.execute(query)
        count = result.scalar()

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#Cantidad de busquedas ganadas en el año corriente
@router_1.get("/earned_searchs_current_year")
async def earned_searchs_current_year(db: Session = Depends(get_db)):
    try:
        query = text("""
                    SELECT COUNT(id) "cantidad_búsquedas_ganadas_2024"
                    FROM search
                    WHERE year(date_opening) = 2024 AND status_search_id = 3;
                    """)
        result = db.execute(query)
        count = result.scalar()

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#Cantidad de busquedas cerradas en el año corriente
@router_1.get("/closed_searchs_current_year")
async def closed_searchs_current_year(db: Session = Depends(get_db)):
    try:
        query = text("""
                        SELECT COUNT(id) "cantidad_búsquedas_cerradas_2024"
                        FROM search
                        WHERE year(date_opening) = 2024 AND status_search_id = 2;
                    """)
        result = db.execute(query)
        count = result.scalar()

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# Cantidad de búsquedas TRABAJANDO(Abiertas (1) + Stand-by (5) + Hibernando (4))(tarjeta)
@router_1.get("/working_searchs_current_year")
async def working_searchs_current_year(db: Session = Depends(get_db)):
    try:
        query = text("""
                    SELECT COUNT(id) "cantidad_búsquedas_trabajando_2024"
                    FROM search
                    WHERE year(date_opening) = 2024 AND status_search_id IN (1, 5, 4);
                    """)
        result = db.execute(query)
        count = result.scalar()

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Cantidad de búsquedas abiertas del 2024
@router_1.get("/open_searchs_current_year")
async def open_searchs_current_year(db: Session = Depends(get_db)):
    try:
        query = text("""
                        SELECT COUNT(id) "cantidad_búsquedas_abiertas_2024"
                        FROM search
                        WHERE year(date_opening) = 2024 AND status_search_id = 1;
                    """)
        result = db.execute(query)
        count = result.scalar()

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Cantidad de búsquedas stand-by del 2024
@router_1.get("/stand_by_searchs_current_year")
async def stand_by_searchs_current_year(db: Session = Depends(get_db)):
    try:
        query = text("""
                        SELECT COUNT(id) "cantidad_búsquedas_standby_2024"
                        FROM search
                        WHERE year(date_opening) = 2024 AND status_search_id = 5;
                    """)
        result = db.execute(query)
        count = result.scalar()

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# Cantidad de búsquedas hibernando del 2024
@router_1.get("/hibernating_searchs_current_year")
async def hibernating_searchs_current_year(db: Session = Depends(get_db)):
    try:
        query = text("""
                        SELECT COUNT(id) "cantidad_búsquedas_hibernando_2024"
                        FROM search
                        WHERE year(date_opening) = 2024 AND status_search_id = 4;
                    """)
        result = db.execute(query)
        count = result.scalar()

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# Cantidad de vacantes, en búsquedas GANADAS
@router_1.get("/earned_search_vacancies")
async def earned_search_vacancies(db: Session = Depends(get_db)):
    try:
        query = text("""
                        SELECT sum(total_vacancies) "cantidad_vacantes_ganadas_2024"
                        FROM search
                        WHERE year(date_opening) = 2024 AND status_search_id = 3;
                    """)
        result = db.execute(query)
        count = result.scalar()

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Cantidad de vacantes, en búsquedas CERRADAS
@router_1.get("/closed_search_vacancies")
async def closed_search_vacancies(db: Session = Depends(get_db)):
    try:
        query = text("""
                        SELECT sum(total_vacancies) "cantidad_vacantes_cerradas_2024"
                        FROM search
                        WHERE year(date_opening) = 2024 AND status_search_id = 2;
                    """)
        result = db.execute(query)
        count = result.scalar()

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Cantidad de vacantes, en búsquedas TRABAJANDO(Abiertas + Stand-By + Hibernando)
@router_1.get("/working_search_vacancies")
async def working_search_vacancies(db: Session = Depends(get_db)):
    try:
        query = text("""
                        SELECT sum(total_vacancies) "cantidad_vacantes_trabajando_2024"
                        FROM search
                        WHERE year(date_opening) = 2024 AND status_search_id IN (1, 5, 4);
                    """)
        result = db.execute(query)
        count = result.scalar()

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Cantidad de vacantes, en búsquedas ABIERTAS
@router_1.get("/open_search_vacancies")
async def open_search_vacancies(db: Session = Depends(get_db)):
    try:
        query = text("""
                        SELECT sum(total_vacancies) "cantidad_vacantes_abiertas_2024"
                        FROM search
                        WHERE year(date_opening) = 2024 AND status_search_id = 1;
                    """)
        result = db.execute(query)
        count = result.scalar()

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Cantidad de vacantes, en búsquedas Stand-By
@router_1.get("/stand_search_vacancies")
async def stand_search_vacancies(db: Session = Depends(get_db)):
    try:
        query = text("""
                        SELECT sum(total_vacancies) "cantidad_vacantes_stand_by_2024"
                        FROM search
                        WHERE year(date_opening) = 2024 AND status_search_id = 5;
                    """)
        result = db.execute(query)
        count = result.scalar()

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Cantidad de vacantes, en búsquedas HIBERNANDO
@router_1.get("/hibernating_search_vacancies")
async def hibernating_search_vacancies(db: Session = Depends(get_db)):
    try:
        query = text("""
                        SELECT sum(total_vacancies) "cantidad_vacantes_hibernando_2024"
                        FROM search
                        WHERE year(date_opening) = 2024 AND status_search_id = 4;
                    """)
        result = db.execute(query)
        count = result.scalar()

        return {"count": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Cantidad de búsquedas GANADAS POR MES
@router_1.get("/earned_searchs_per_month")
async def earned_searchs_per_month(db: Session = Depends(get_db)):
    try:
        query = text("""
            SELECT MONTH(date_opening) AS mes, COUNT(id) AS cantidad_búsquedas_ganadas_2024
            FROM search
            WHERE YEAR(date_opening) = 2024 AND status_search_id = 3
            GROUP BY mes;
        """)
        
        result = db.execute(query)
        rows = result.fetchall()

        # Convertir el resultado en un formato JSON esperado
        results = [{"mes": row['mes'], "cantidad_búsquedas_ganadas_2024": row['cantidad_búsquedas_ganadas_2024']} for row in rows]

        return {"results": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# Tabla (join entre search -> client (para el nombre de cliente),
#				search -> status_search (para el nombre del estado)
@router_1.get("/table_client_status_search")
async def table_client_status_search(db: Session = Depends(get_db)):
    try:
        query = text("""
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
                    WHERE YEAR(s.date_opening) = 2024
                    ;
                    """)
        
        result = db.execute(query)
        rows = result.fetchall()

        # Convertir el resultado en un formato JSON esperado
        results = []  # Inicializa una lista vacía para almacenar los resultados en formato JSON.
        for row in rows:
            results.append({
                "busqueda": row[0],            
                "cliente": row[1],             
                "estado": row[2],              
                "fecha_apertura": row[3].isoformat(), 
                "vacantes": row[4],            
                "dias_en_etapa": row[5]        
            })

        return {"results": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))