from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import psycopg2

app = FastAPI()

DB_CONFIG = {
    "host": "postgres",
    "database": "cars",
    "user": "program",
    "password": "test"
}

@app.get("/manage/health")
def health():
    return JSONResponse(content={"status": "OK"})

@app.get("/api/v1/cars")
def get_cars(
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1),
    showAll: bool = Query(False)
):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        count_query = "SELECT COUNT(*) FROM cars"
        if not showAll:
            count_query += " WHERE availability = true"
        cur.execute(count_query)
        total_elements = cur.fetchone()[0]

        offset = (page - 1) * size
        select_query = """
            SELECT car_uid, brand, model, registration_number, power, price, type, availability
            FROM cars
        """
        if not showAll:
            select_query += " WHERE availability = true"
        select_query += " ORDER BY id LIMIT %s OFFSET %s"

        cur.execute(select_query, (size, offset))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        items = [
            {
                "carUid": str(r[0]),
                "brand": r[1],
                "model": r[2],
                "registrationNumber": r[3],
                "power": r[4],
                "price": r[5],
                "type": r[6],
                "available": r[7]  
            }
            for r in rows
        ]

        return JSONResponse(content={
            "page": page,
            "pageSize": len(items),
            "totalElements": total_elements,
            "items": items
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/v1/cars/{car_uid}/reserve")
def reserve_car(car_uid: str):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("UPDATE cars SET availability = false WHERE car_uid = %s", (car_uid,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Car not found")
        conn.commit()
        cur.close()
        conn.close()
        return JSONResponse(content={"status": "reserved"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/v1/cars/{car_uid}/release")
def release_car(car_uid: str):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("UPDATE cars SET availability = true WHERE car_uid = %s", (car_uid,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Car not found")
        conn.commit()
        cur.close()
        conn.close()
        return JSONResponse(content={"status": "released"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/cars/{car_uid}")
def get_car_by_uid(car_uid: str):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            SELECT car_uid, brand, model, registration_number, power, price, type, availability
            FROM cars
            WHERE car_uid = %s
        """, (car_uid,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Car not found")
        return {
            "carUid": str(row[0]),
            "brand": row[1],
            "model": row[2],
            "registrationNumber": row[3],
            "power": row[4],
            "price": row[5],
            "type": row[6],
            "available": row[7]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

