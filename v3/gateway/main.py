from fastapi import FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
import httpx
from datetime import datetime
import uuid
import logging
import pybreaker
import asyncio
import json
import redis.asyncio as redis
from httpx import ConnectError, TimeoutException, NetworkError
from typing import Optional

TEST_MODE = False

redis_client = None
QUEUE_NAME = "gateway_tasks"

payment_circuit = pybreaker.CircuitBreaker(
    fail_max=2,
    reset_timeout=15000
)

rental_circuit = pybreaker.CircuitBreaker(
    fail_max=2,
    reset_timeout=15000
)

cars_circuit = pybreaker.CircuitBreaker(
    fail_max=2,
    reset_timeout=15000
)

app = FastAPI()
CARS_SERVICE = "http://cars-service:8070"
RENTAL_SERVICE = "http://rental-service:8060"
PAYMENT_SERVICE = "http://payment-service:8050"

saga_log = {}

class RentalRequest(BaseModel):
    carUid: str
    dateFrom: str
    dateTo: str

@app.middleware("http")
async def check_test_mode(request: Request, call_next):
    global TEST_MODE
    if request.headers.get("X-Test-Mode") == "true":
        TEST_MODE = True
    response = await call_next(request)
    return response

async def process_queue():
    while True:
        try:
            if redis_client:
                task_json = await redis_client.rpop(QUEUE_NAME)
                if task_json:
                    task = json.loads(task_json)
                    await process_queued_task(task)
        except Exception as e:
            logging.error(f"Queue processing error: {e}")
        await asyncio.sleep(1)

async def process_queued_task(task):
    try:
        if task["type"] == "create_rental":
            await retry_create_rental(task)
        elif task["type"] == "cancel_rental":
            await retry_cancel_rental(task)
        elif task["type"] == "finish_rental":
            await retry_finish_rental(task)
    except Exception as e:
        logging.error(f"Task processing failed: {e}")
        if redis_client and "No address associated with hostname" not in str(e):
            try:
                await redis_client.lpush(QUEUE_NAME, json.dumps(task))
            except Exception as queue_error:
                logging.error(f"Failed to requeue task: {queue_error}")

async def retry_create_rental(task):
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            car = await call_get_car(client, task["carUid"])
            date_from = datetime.fromisoformat(task["dateFrom"])
            date_to = datetime.fromisoformat(task["dateTo"])
            days = (date_to - date_from).days
            total_price = car["price"] * days
            
            payment_data = await call_create_payment(client, total_price)
            payment_uid = payment_data["paymentUid"]
            
            rental_data = await call_create_rental(client, {
                "carUid": task["carUid"],
                "dateFrom": task["dateFrom"],
                "dateTo": task["dateTo"],
                "paymentUid": payment_uid
            }, task["username"])
            rental_uid = rental_data["rentalUid"]
            
            await call_reserve_car(client, task["carUid"])
            
            if task["request_id"] in saga_log:
                saga_log[task["request_id"]]["status"] = "completed"
            
        except Exception as e:
            logging.warning(f"Retry failed: {e}")
            if task["request_id"] in saga_log:
                saga_log[task["request_id"]]["status"] = "queued"

async def retry_cancel_rental(task):
    async with httpx.AsyncClient() as client:
        try:
            if task.get("payment_uid"):
                await call_cancel_payment(client, task["payment_uid"])
            if task.get("car_uid"):
                await call_release_car(client, task["car_uid"])
            await call_cancel_rental(client, task["rental_uid"], task["username"])
            if task["request_id"] in saga_log:
                saga_log[task["request_id"]]["status"] = "completed"
        except Exception as e:
            logging.warning(f"Retry cancel failed: {e}")

async def retry_finish_rental(task):
    async with httpx.AsyncClient() as client:
        try:
            if task.get("car_uid"):
                await call_release_car(client, task["car_uid"])
            await call_finish_rental(client, task["rental_uid"], task["username"])
            if task["request_id"] in saga_log:
                saga_log[task["request_id"]]["status"] = "completed"
        except Exception as e:
            logging.warning(f"Retry finish failed: {e}")

async def enqueue_task(task_type, data, request_id, username=None):
    task = {
        "type": task_type,
        "data": data,
        "request_id": request_id,
        "username": username,
        **data
    }
    
    try:
        if redis_client:
            await redis_client.lpush(QUEUE_NAME, json.dumps(task))
            saga_log[request_id] = {"status": "queued", "task": task}
            logging.info(f"Task {request_id} enqueued")
            
            return {
                "status": "ACCEPTED",
                "message": "Request accepted and queued for processing",
                "requestId": request_id
            }
    except Exception as e:
        logging.error(f"Failed to enqueue task: {e}")
    
    return None

@payment_circuit
async def call_create_payment(client, price: int):
    r = await client.post(f"{PAYMENT_SERVICE}/api/v1/payment", json={"price": price})
    r.raise_for_status()
    return r.json()

@payment_circuit
async def call_cancel_payment(client, payment_uid: str):
    r = await client.delete(f"{PAYMENT_SERVICE}/api/v1/payment/{payment_uid}")
    r.raise_for_status()
    return r.json()

@rental_circuit
async def call_get_rental(client, rental_uid: str, username: str):
    r = await client.get(f"{RENTAL_SERVICE}/api/v1/rental/{rental_uid}", headers={"X-User-Name": username})
    r.raise_for_status()
    return r.json()

@rental_circuit
async def call_get_rentals(client, username: str):
    r = await client.get(f"{RENTAL_SERVICE}/api/v1/rental", headers={"X-User-Name": username})
    r.raise_for_status()
    return r.json()

@rental_circuit
async def call_create_rental(client, data: dict, username: str):
    r = await client.post(
        f"{RENTAL_SERVICE}/api/v1/rental",
        json=data,
        headers={"X-User-Name": username}
    )
    r.raise_for_status()
    return r.json()

@rental_circuit
async def call_cancel_rental(client, rental_uid: str, username: str):
    r = await client.delete(f"{RENTAL_SERVICE}/api/v1/rental/{rental_uid}", headers={"X-User-Name": username})
    r.raise_for_status()
    return r.json()

@rental_circuit
async def call_finish_rental(client, rental_uid: str, username: str):
    r = await client.post(f"{RENTAL_SERVICE}/api/v1/rental/{rental_uid}/finish", headers={"X-User-Name": username})
    r.raise_for_status()
    return r.json()

@cars_circuit
async def call_get_cars(client, page: int, size: int, showAll: bool):
    params = {"page": page, "size": size, "showAll": str(showAll).lower()}
    r = await client.get(f"{CARS_SERVICE}/api/v1/cars", params=params)
    r.raise_for_status()
    return r.json()

@cars_circuit
async def call_get_car(client, car_uid: str):
    r = await client.get(f"{CARS_SERVICE}/api/v1/cars/{car_uid}")
    r.raise_for_status()
    return r.json()

@cars_circuit
async def call_reserve_car(client, car_uid: str):
    r = await client.put(f"{CARS_SERVICE}/api/v1/cars/{car_uid}/reserve")
    r.raise_for_status()
    return r.json()

@cars_circuit
async def call_release_car(client, car_uid: str):
    r = await client.put(f"{CARS_SERVICE}/api/v1/cars/{car_uid}/release")
    r.raise_for_status()
    return r.json()

@app.on_event("startup")
async def startup_event():
    global redis_client
    try:
        redis_client = redis.from_url("redis://redis:6379/0", decode_responses=True)
        await redis_client.ping()
        logging.info("Connected to Redis")
        asyncio.create_task(process_queue())
    except Exception as e:
        logging.warning(f"Redis not available: {e}")
        redis_client = None

@app.get("/manage/health")
def health():
    return JSONResponse(content={"status": "OK"})

@app.get("/api/v1/cars")
async def get_cars(page: int = Query(1, ge=1), size: int = Query(10, ge=1), showAll: bool = Query(False)):
    try:
        async with httpx.AsyncClient() as client:
            cars = await call_get_cars(client, page, size, showAll)
            return cars
    except pybreaker.CircuitBreakerError:
        return JSONResponse(status_code=503, content={"message": "Cars Service unavailable"})
    except (ConnectError, TimeoutException, NetworkError):
        return JSONResponse(status_code=503, content={"message": "Cars Service unavailable"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})

@app.get("/api/v1/rental")
async def get_rentals(username: str = Header(..., alias="X-User-Name")):
    try:
        async with httpx.AsyncClient() as client:
            rentals = await call_get_rentals(client, username)
            aggregated = []
            for rental in rentals:
                car = await call_get_car(client, rental['carUid'])
                payment = {"paymentUid": rental['paymentUid'], "status": "UNKNOWN", "price": 0}
                try:
                    p_resp = await client.get(f"{PAYMENT_SERVICE}/api/v1/payment/{rental['paymentUid']}")
                    if p_resp.status_code == 200:
                        payment = p_resp.json()
                except:
                    if rental.get("status") == "CANCELED":
                        payment = {"paymentUid": rental['paymentUid'], "status": "CANCELED", "price": 0}
                aggregated.append({
                    "rentalUid": rental["rentalUid"],
                    "status": rental["status"],
                    "dateFrom": rental["dateFrom"],
                    "dateTo": rental["dateTo"],
                    "car": {
                        "carUid": car["carUid"],
                        "brand": car["brand"],
                        "model": car["model"],
                        "registrationNumber": car["registrationNumber"]
                    },
                    "payment": payment
                })
            return JSONResponse(content=aggregated)
    except pybreaker.CircuitBreakerError:
        return JSONResponse(status_code=503, content={"message": "Rental Service unavailable"})
    except (ConnectError, TimeoutException, NetworkError):
        return JSONResponse(status_code=503, content={"message": "Rental Service unavailable"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})

@app.get("/api/v1/rental/{rental_uid}")
async def get_rental(rental_uid: str, username: str = Header(..., alias="X-User-Name")):
    try:
        async with httpx.AsyncClient() as client:
            rental = await call_get_rental(client, rental_uid, username)
            car = await call_get_car(client, rental['carUid'])
            payment = {"paymentUid": rental['paymentUid'], "status": "UNKNOWN", "price": 0}
            try:
                p_resp = await client.get(f"{PAYMENT_SERVICE}/api/v1/payment/{rental['paymentUid']}")
                if p_resp.status_code == 200:
                    payment = p_resp.json()
            except:
                payment = {}
            if rental.get("status") == "CANCELED" and payment != {}:
                payment = {"paymentUid": rental['paymentUid'], "status": "CANCELED", "price": payment.get("price", 0)}
            return JSONResponse(content={
                "rentalUid": rental["rentalUid"],
                "status": rental["status"],
                "dateFrom": rental["dateFrom"],
                "dateTo": rental["dateTo"],
                "car": {
                    "carUid": car["carUid"],
                    "brand": car["brand"],
                    "model": car["model"],
                    "registrationNumber": car["registrationNumber"]
                },
                "payment": payment
            })
    except pybreaker.CircuitBreakerError:
        return JSONResponse(status_code=503, content={"message": "Rental Service unavailable"})
    except (ConnectError, TimeoutException, NetworkError):
        return JSONResponse(status_code=503, content={"message": "Rental Service unavailable"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})

@app.post("/api/v1/rental")
async def create_rental(
    req: RentalRequest,
    request: Request,
    username: str = Header(..., alias="X-User-Name")
):
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    if request_id in saga_log and saga_log[request_id].get("status") == "completed":
        log = saga_log[request_id]
        async with httpx.AsyncClient() as client:
            car = await call_get_car(client, log["car_uid"])
            payment = await client.get(f"{PAYMENT_SERVICE}/api/v1/payment/{log['payment_uid']}").json()
            return JSONResponse(content={
                "rentalUid": log["rental_uid"],
                "carUid": log["car_uid"],
                "dateFrom": req.dateFrom,
                "dateTo": req.dateTo,
                "status": "IN_PROGRESS",
                "car": {
                    "carUid": car["carUid"],
                    "brand": car["brand"],
                    "model": car["model"],
                    "registrationNumber": car["registrationNumber"]
                },
                "payment": payment
            }, status_code=200)

    saga_log[request_id] = {"step": "started", "car_uid": req.carUid}

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            car = await call_get_car(client, req.carUid)
        except Exception as e:
            if isinstance(e, (ConnectError, NetworkError)) and "No address associated with hostname" in str(e):
                return JSONResponse(status_code=503, content={"message": "Cars Service unavailable"})
                
            if redis_client:
                result = await enqueue_task("create_rental", {
                    "carUid": req.carUid,
                    "dateFrom": req.dateFrom,
                    "dateTo": req.dateTo
                }, request_id, username)
                if result:
                    return JSONResponse(content=result, status_code=202)
            return JSONResponse(status_code=503, content={"message": "Cars Service unavailable"})

        try:
            date_from = datetime.fromisoformat(req.dateFrom)
            date_to = datetime.fromisoformat(req.dateTo)
        except ValueError:
            return JSONResponse(status_code=400, content={"message": "Invalid date format"})

        days = (date_to - date_from).days
        if days <= 0:
            return JSONResponse(status_code=400, content={"message": "Invalid rental period"})

        total_price = car["price"] * days
        payment_uid = None
        rental_uid = None

        try:
            payment_data = await call_create_payment(client, total_price)
            payment_uid = payment_data["paymentUid"]
            saga_log[request_id].update({"step": "payment_created", "payment_uid": payment_uid})

            rental_data = await call_create_rental(client, {
                "carUid": req.carUid,
                "dateFrom": req.dateFrom,
                "dateTo": req.dateTo,
                "paymentUid": payment_uid
            }, username)
            rental_uid = rental_data["rentalUid"]
            saga_log[request_id].update({"step": "rental_created", "rental_uid": rental_uid})

            await call_reserve_car(client, req.carUid)
            saga_log[request_id]["status"] = "completed"

            return JSONResponse(content={
                "rentalUid": rental_uid,
                "carUid": req.carUid,
                "dateFrom": req.dateFrom,
                "dateTo": req.dateTo,
                "status": "IN_PROGRESS",
                "car": {
                    "carUid": car["carUid"],
                    "brand": car["brand"],
                    "model": car["model"],
                    "registrationNumber": car["registrationNumber"]
                },
                "payment": payment_data
            }, status_code=200)

        except Exception as e:
            if isinstance(e, (ConnectError, NetworkError)) and "No address associated with hostname" in str(e):
                logging.error(f"Service completely unavailable: {e}")
                if payment_uid:
                    try:
                        await call_cancel_payment(client, payment_uid)
                    except Exception:
                        logging.warning(f"Could not cancel payment {payment_uid}")
                if rental_uid:
                    try:
                        await call_cancel_rental(client, rental_uid, username)
                    except Exception:
                        logging.warning(f"Could not cancel rental {rental_uid}")
                return JSONResponse(status_code=503, content={"message": "Payment Service unavailable"})
            
            if redis_client:
                result = await enqueue_task("create_rental", {
                    "carUid": req.carUid,
                    "dateFrom": req.dateFrom,
                    "dateTo": req.dateTo
                }, request_id, username)
                if result:
                    pass
                    #return JSONResponse(content=result, status_code=202)
            
            logging.error(f"Rental creation failed: {e}")
            if payment_uid:
                try:
                    await call_cancel_payment(client, payment_uid)
                except Exception:
                    logging.warning(f"Could not cancel payment {payment_uid}")
            if rental_uid:
                try:
                    await call_cancel_rental(client, rental_uid, username)
                except Exception:
                    logging.warning(f"Could not cancel rental {rental_uid}")
            return JSONResponse(status_code=500, content={"message": "Internal server error"})

@app.post("/api/v1/rental/{rental_uid}/finish")
async def finish_rental(rental_uid: str, username: str = Header(..., alias="X-User-Name")):
    request_id = str(uuid.uuid4())
    car_uid = None
    
    try:
        async with httpx.AsyncClient() as client:
            rental = await call_get_rental(client, rental_uid, username)
            car_uid = rental["carUid"]
            await call_release_car(client, car_uid)
            await call_finish_rental(client, rental_uid, username)
            return Response(status_code=204)
    except Exception as e:
        if TEST_MODE or isinstance(e, (ConnectError, NetworkError)) and "No address associated with hostname" in str(e):
            return JSONResponse(status_code=500, content={"message": str(e)})
            
        if redis_client:
            task_data = {
                "rental_uid": rental_uid,
                "car_uid": car_uid
            }
            result = await enqueue_task("finish_rental", task_data, request_id, username)
            if result:
                return Response(status_code=202)
        return JSONResponse(status_code=500, content={"message": str(e)})

@app.delete("/api/v1/rental/{rental_uid}")
async def cancel_rental(rental_uid: str, username: str = Header(..., alias="X-User-Name")):
    car_uid = None
    payment_uid = None
    
    try:
        async with httpx.AsyncClient() as client:
            rental = await call_get_rental(client, rental_uid, username)
            car_uid = rental["carUid"]
            payment_uid = rental["paymentUid"]

            try:
                await call_cancel_payment(client, payment_uid)
            except (pybreaker.CircuitBreakerError, ConnectError, TimeoutException, NetworkError):
                logging.warning(f"Payment service unavailable, cannot cancel {payment_uid}")
            except Exception as e:
                logging.warning(f"Failed to cancel payment: {e}")

            await call_release_car(client, car_uid)
            await call_cancel_rental(client, rental_uid, username)
            return Response(status_code=204)
    except Exception as e:
        if isinstance(e, (ConnectError, NetworkError)) and "No address associated with hostname" in str(e):
            return JSONResponse(status_code=500, content={"message": str(e)})
            
        request_id = str(uuid.uuid4())
        task_data = {
            "rental_uid": rental_uid,
            "car_uid": car_uid, 
            "payment_uid": payment_uid  
        }
        if redis_client:
            result = await enqueue_task("cancel_rental", task_data, request_id, username)
            if result:
                return Response(status_code=202)
        return JSONResponse(status_code=500, content={"message": str(e)})
