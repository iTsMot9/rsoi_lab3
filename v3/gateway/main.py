from fastapi import FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
import httpx
from datetime import datetime
import uuid
import logging
import pybreaker
from httpx import ConnectError, TimeoutException, NetworkError

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
            })

    saga_log[request_id] = {"step": "started", "car_uid": req.carUid}

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            car = await call_get_car(client, req.carUid)
        except pybreaker.CircuitBreakerError:
            return JSONResponse(status_code=503, content={"message": "Cars Service unavailable"})
        except (ConnectError, TimeoutException, NetworkError):
            return JSONResponse(status_code=503, content={"message": "Cars Service unavailable"})
        except Exception as e:
            return JSONResponse(status_code=500, content={"message": str(e)})

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
            })

        except pybreaker.CircuitBreakerError:
            logging.error("Payment service circuit breaker is OPEN")
            return JSONResponse(status_code=503, content={"message": "Payment Service unavailable"})
        except (ConnectError, TimeoutException, NetworkError) as e:
            logging.error(f"Payment service unreachable: {e}")
            return JSONResponse(status_code=503, content={"message": "Payment Service unavailable"})
        except Exception as e:
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
    try:
        async with httpx.AsyncClient() as client:
            rental = await call_get_rental(client, rental_uid, username)
            car_uid = rental["carUid"]
            await call_release_car(client, car_uid)
            await call_finish_rental(client, rental_uid, username)
            return Response(status_code=204)
    except pybreaker.CircuitBreakerError:
        return JSONResponse(status_code=503, content={"message": "Rental Service unavailable"})
    except (ConnectError, TimeoutException, NetworkError):
        return JSONResponse(status_code=503, content={"message": "Rental Service unavailable"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})

@app.delete("/api/v1/rental/{rental_uid}")
async def cancel_rental(rental_uid: str, username: str = Header(..., alias="X-User-Name")):
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
    except pybreaker.CircuitBreakerError:
        return JSONResponse(status_code=503, content={"message": "Rental Service unavailable"})
    except (ConnectError, TimeoutException, NetworkError):
        return JSONResponse(status_code=503, content={"message": "Rental Service unavailable"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})
