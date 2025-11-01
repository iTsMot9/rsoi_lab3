import pytest
from main import app
from fastapi.testclient import TestClient

client = TestClient(app)

def test_health():
    response = client.get("/manage/health")
    assert response.status_code == 200
    assert response.json() == {"status": "OK"}

def test_create_rental():
    headers = {"X-User-Name": "Alex"}
    body = {
        "carUid": "109b42f3-198d-4c89-9276-a7520a7120ab",
        "dateFrom": "2025-11-01",
        "dateTo": "2025-11-05"
    }
    response = client.post("/api/v1/rental", json=body, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert "rentalUid" in data
