from fastapi.responses import Response
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import httpx
from datetime import datetime
import time

app = FastAPI()

CARS_SERVICE = "http://cars-service:8070"
RENTAL_SERVICE = "http://rental-service:8060"
PAYMENT_SERVICE = "http://payment-service:8050"


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 3, recovery_timeout: int = 30):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"

    async def call(self, func, fallback_func):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
            else:
                return await fallback_func()

        try:
            result = await func()
            self._on_success()
            return result
        except Exception:
            self._on_failure()
            return await fallback_func()

    def _on_success(self):
        self.failure_count = 0
        self.state = "CLOSED"

    def _on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"


cars_cb = CircuitBreaker("cars", failure_threshold=3, recovery_timeout=30)
rental_cb = CircuitBreaker("rental", failure_threshold=3, recovery_timeout=30)
payment_cb = CircuitBreaker("payment", failure_threshold=3, recovery_timeout=30)


async def cars_fallback():
    raise HTTPException(status_code=503, detail="Cars Service unavailable")

async def rental_fallback():
    raise HTTPException(status_code=503, detail="Rental Service unavailable")

async def payment_create_fallback():
    raise HTTPException(status_code=503, detail="Payment Service unavailable")

async def payment_read_fallback():
    raise Exception("Payment Service unavailable")

@app.get("/manage/health")
def health():
    return JSONResponse(content={"status": "OK"})


@app.get("/api/v1/cars")
async def get_cars(page: int = Query(1, ge=1), size: int = Query(10, ge=1), showAll: bool = Query(False)):
    async def _call():
        async with httpx.AsyncClient() as client:
            params = {"page": page, "size": size, "showAll": str(showAll).lower()}
            r = await client.get(f"{CARS_SERVICE}/api/v1/cars", params=params, timeout=3.0)
            r.raise_for_status()
            return r.json()
    return await cars_cb.call(_call, cars_fallback)


@app.get("/api/v1/rental")
async def get_rentals(username: str = Header(..., alias="X-User-Name")):
    async def _call():
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{RENTAL_SERVICE}/api/v1/rental", headers={"X-User-Name": username}, timeout=3.0)
            r.raise_for_status()
            rentals = r.json()
            aggregated = []
            for rental in rentals:
                car_resp = await client.get(f"{CARS_SERVICE}/api/v1/cars/{rental['carUid']}", timeout=3.0)
                car_resp.raise_for_status()
                car = car_resp.json()

                payment = {}
                try:
                    payment_resp = await client.get(f"{PAYMENT_SERVICE}/api/v1/payment/{rental['paymentUid']}", timeout=3.0)
                    payment_resp.raise_for_status()
                    payment = payment_resp.json()
                except (httpx.RequestError, httpx.HTTPStatusError, Exception):
                    payment = {}

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
    return await rental_cb.call(_call, rental_fallback)


@app.get("/api/v1/rental/{rental_uid}")
async def get_rental(rental_uid: str, username: str = Header(..., alias="X-User-Name")):
    async def _call():
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{RENTAL_SERVICE}/api/v1/rental/{rental_uid}", headers={"X-User-Name": username}, timeout=3.0)
            r.raise_for_status()
            rental = r.json()

            car_resp = await client.get(f"{CARS_SERVICE}/api/v1/cars/{rental['carUid']}", timeout=3.0)
            car_resp.raise_for_status()
            car = car_resp.json()

            payment = {}
            try:
                payment_resp = await client.get(f"{PAYMENT_SERVICE}/api/v1/payment/{rental['paymentUid']}", timeout=3.0)
                payment_resp.raise_for_status()
                payment = payment_resp.json()
            except (httpx.RequestError, httpx.HTTPStatusError, Exception):
                payment = {}

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
    return await rental_cb.call(_call, rental_fallback)


class RentalRequest(BaseModel):
    carUid: str
    dateFrom: str
    dateTo: str


@app.post("/api/v1/rental")
async def create_rental(req: RentalRequest, username: str = Header(..., alias="X-User-Name")):
    async with httpx.AsyncClient(timeout=3.0) as client:
        r_cars = await client.get(f"{CARS_SERVICE}/api/v1/cars?showAll=true&page=1&size=1000", timeout=3.0)
        r_cars.raise_for_status()
        cars_list = r_cars.json().get("items", [])
        car = next((c for c in cars_list if c["carUid"] == req.carUid), None)
        if not car:
            raise HTTPException(status_code=404, detail="Car not found")

        date_from = datetime.strptime(req.dateFrom, "%Y-%m-%d")
        date_to = datetime.strptime(req.dateTo, "%Y-%m-%d")
        days = (date_to - date_from).days
        if days <= 0:
            raise HTTPException(status_code=400, detail="Invalid rental period")
        total_price = car["price"] * days

        async def _create_payment():
            payment = await client.post(f"{PAYMENT_SERVICE}/api/v1/payment", json={"price": total_price}, timeout=3.0)
            payment.raise_for_status()
            return payment.json()

        try:
            payment_data = await payment_cb.call(_create_payment, payment_create_fallback)
        except HTTPException as e:
            if e.detail == "Payment Service unavailable":
                raise HTTPException(status_code=503, detail="Payment Service unavailable")
            raise

        payment_uid = payment_data["paymentUid"]

        rental = await client.post(
            f"{RENTAL_SERVICE}/api/v1/rental",
            json={"carUid": req.carUid, "dateFrom": req.dateFrom, "dateTo": req.dateTo, "paymentUid": payment_uid},
            headers={"X-User-Name": username},
            timeout=3.0
        )
        rental.raise_for_status()
        rental_data = rental.json()
        rental_uid = rental_data["rentalUid"]

        await client.put(f"{CARS_SERVICE}/api/v1/cars/{req.carUid}/reserve", timeout=3.0)

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
            "payment": {
                "paymentUid": payment_data["paymentUid"],
                "status": payment_data["status"],
                "price": payment_data["price"]
            }
        })


@app.post("/api/v1/rental/{rental_uid}/finish")
async def finish_rental(rental_uid: str, username: str = Header(..., alias="X-User-Name")):
    async def _call():
        async with httpx.AsyncClient() as client:
            rental = await client.get(f"{RENTAL_SERVICE}/api/v1/rental/{rental_uid}", headers={"X-User-Name": username}, timeout=3.0)
            rental.raise_for_status()
            rental_data = rental.json()
            car_uid = rental_data["carUid"]
            await client.put(f"{CARS_SERVICE}/api/v1/cars/{car_uid}/release", timeout=3.0)
            r = await client.post(f"{RENTAL_SERVICE}/api/v1/rental/{rental_uid}/finish", headers={"X-User-Name": username}, timeout=3.0)
            r.raise_for_status()
        return Response(status_code=204)
    return await rental_cb.call(_call, rental_fallback)


@app.delete("/api/v1/rental/{rental_uid}")
async def cancel_rental(rental_uid: str, username: str = Header(..., alias="X-User-Name")):
    async def _get_rental():
        async with httpx.AsyncClient() as client:
            rental = await client.get(f"{RENTAL_SERVICE}/api/v1/rental/{rental_uid}", headers={"X-User-Name": username}, timeout=3.0)
            rental.raise_for_status()
            return rental.json()
    rental_data = await rental_cb.call(_get_rental, rental_fallback)
    car_uid = rental_data["carUid"]
    payment_uid = rental_data["paymentUid"]

    async def _release_car():
        async with httpx.AsyncClient() as client:
            await client.put(f"{CARS_SERVICE}/api/v1/cars/{car_uid}/release", timeout=3.0)
    await cars_cb.call(_release_car, cars_fallback)

    async def _cancel_payment():
        async with httpx.AsyncClient() as client:
            await client.delete(f"{PAYMENT_SERVICE}/api/v1/payment/{payment_uid}", timeout=3.0)
    try:
        await payment_cb.call(_cancel_payment, lambda: None)
    except:
        pass

    async def _cancel_rental():
        async with httpx.AsyncClient() as client:
            r = await client.delete(f"{RENTAL_SERVICE}/api/v1/rental/{rental_uid}", headers={"X-User-Name": username}, timeout=3.0)
            r.raise_for_status()
        return Response(status_code=204)
    return await rental_cb.call(_cancel_rental, rental_fallback)
