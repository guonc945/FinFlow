import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_project_sync_endpoint():
    # Test the background sync endpoint
    response = client.post("/api/projects/sync")
    assert response.status_code == 200
    assert response.json()["detail"] == "Project synchronization started"

def test_get_projects():
    # Test the projects list endpoint
    response = client.get("/api/projects")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
