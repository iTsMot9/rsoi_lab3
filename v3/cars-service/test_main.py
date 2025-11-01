import pytest
from main import app
from fastapi.testclient import TestClient

client = TestClient(app)

def test_health():
    response = client.get("/manage/health")
    assert response.status_code == 200
    assert response.json() == {"status": "OK"}

def test_get_cars_empty():
    response = client.get("/api/v1/cars")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
