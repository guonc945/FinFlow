from pathlib import Path
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import fetch_parks
import fetch_residents
import models
from database import Base


TEST_TABLES = [
    models.ExternalService.__table__,
    models.ExternalApi.__table__,
    models.GlobalVariable.__table__,
    models.ProjectList.__table__,
    models.House.__table__,
    models.Park.__table__,
    models.Resident.__table__,
]


def make_session_factory():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine, tables=TEST_TABLES)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def test_insert_residents_bulk_upserts_rows():
    session_factory = make_session_factory()
    original_session_local = fetch_residents.SessionLocal
    fetch_residents.SessionLocal = session_factory

    try:
        db = session_factory()
        db.add(
            models.Resident(
                resident_id="R-001",
                community_id="12436",
                community_name="Old Community",
                name="Old Name",
                phone="10086",
            )
        )
        db.commit()
        db.close()

        result = fetch_residents.insert_residents(
            [
                {
                    "id": "R-001",
                    "communityID": "12436",
                    "communityName": "New Community",
                    "name": "Alice",
                    "phone": "13800000000",
                    "houseList": [{"id": "H-001"}],
                    "labelList": ["owner"],
                },
                {
                    "id": "R-002",
                    "communityID": "12436",
                    "communityName": "New Community",
                    "name": "Bob",
                    "phone": "13900000000",
                },
                {
                    "id": "",
                    "communityID": "12436",
                    "name": "Invalid",
                },
            ]
        )

        assert result == {"inserted": 2, "skipped": 1}

        db = session_factory()
        rows = db.query(models.Resident).order_by(models.Resident.resident_id).all()
        assert [row.resident_id for row in rows] == ["R-001", "R-002"]
        assert rows[0].name == "Alice"
        assert rows[0].community_name == "New Community"
        assert rows[0].houses is not None
        db.close()
    finally:
        fetch_residents.SessionLocal = original_session_local


def test_sync_residents_for_community_uses_has_more_signal():
    session_factory = make_session_factory()
    original_session_local = fetch_residents.SessionLocal
    original_build_variable_map = fetch_residents.build_variable_map
    original_get_api_url_by_id = fetch_residents.get_api_url_by_id
    original_insert_residents = fetch_residents.insert_residents
    original_request = fetch_residents.marki_client.request
    fetch_residents.SessionLocal = session_factory

    request_calls = []

    try:
        db = session_factory()
        db.add(models.ProjectList(proj_id=12436, proj_name="Community A"))
        db.commit()
        db.close()

        fetch_residents.build_variable_map = lambda db: {}
        fetch_residents.get_api_url_by_id = lambda api_id: "https://example.test/getUserList"
        fetch_residents.insert_residents = lambda data_list, community_name=None: {"inserted": len(data_list), "skipped": 0}

        responses = [
            {"data": {"list": [{"id": "R-001", "communityID": "12436", "name": "Alice"}], "hasMore": True}},
            {"data": {"list": [{"id": "R-002", "communityID": "12436", "name": "Bob"}], "hasMore": False}},
        ]

        def fake_request(method, url, params=None, json_data=None, timeout=30, retry_on_401=True, extra_headers=None):
            request_calls.append({"method": method, "url": url, "params": params})
            return responses[len(request_calls) - 1]

        fetch_residents.marki_client.request = fake_request

        total = fetch_residents.sync_residents_for_community("12436")

        assert total == 2
        assert len(request_calls) == 2
        assert request_calls[0]["params"]["page"] == 1
        assert request_calls[1]["params"]["page"] == 2
    finally:
        fetch_residents.SessionLocal = original_session_local
        fetch_residents.build_variable_map = original_build_variable_map
        fetch_residents.get_api_url_by_id = original_get_api_url_by_id
        fetch_residents.insert_residents = original_insert_residents
        fetch_residents.marki_client.request = original_request


def test_insert_parks_bulk_resolves_house_fk():
    session_factory = make_session_factory()
    original_session_local = fetch_parks.SessionLocal
    fetch_parks.SessionLocal = session_factory

    try:
        db = session_factory()
        house = models.House(
            house_id="H-001",
            community_id="12436",
            community_name="Community A",
            house_name="1-1-101",
        )
        db.add(house)
        db.commit()
        db.refresh(house)
        db.close()

        result = fetch_parks.insert_parks(
            [
                {
                    "id": "P-001",
                    "communityId": "12436",
                    "communityName": "Community A",
                    "name": "Park A",
                    "parkTypeName": "Ground",
                    "state": 1,
                    "userItem": {"name": "Alice"},
                    "houseItem": {"id": "H-001", "name": "1-1-101"},
                },
                {
                    "id": "P-002",
                    "communityId": "12436",
                    "communityName": "Community A",
                    "name": "Park B",
                    "houseItem": {"id": "H-404", "name": "Unknown"},
                },
                {
                    "id": "",
                    "communityId": "12436",
                    "name": "Invalid",
                },
            ]
        )

        assert result == {"inserted": 2, "skipped": 1}

        db = session_factory()
        rows = db.query(models.Park).order_by(models.Park.park_id).all()
        assert [row.park_id for row in rows] == ["P-001", "P-002"]
        assert rows[0].house_fk == house.id
        assert rows[1].house_fk is None
        db.close()
    finally:
        fetch_parks.SessionLocal = original_session_local


def test_sync_parks_for_community_stops_on_has_more_false():
    session_factory = make_session_factory()
    original_session_local = fetch_parks.SessionLocal
    original_build_variable_map = fetch_parks.build_variable_map
    original_get_api_url_by_id = fetch_parks.get_api_url_by_id
    original_insert_parks = fetch_parks.insert_parks
    original_request = fetch_parks.marki_client.request
    fetch_parks.SessionLocal = session_factory

    request_calls = []

    try:
        db = session_factory()
        db.add(models.ProjectList(proj_id=12436, proj_name="Community A"))
        db.commit()
        db.close()

        fetch_parks.build_variable_map = lambda db: {}
        fetch_parks.get_api_url_by_id = lambda api_id: "https://example.test/getParkList"
        fetch_parks.insert_parks = lambda data_list, community_name=None: {"inserted": len(data_list), "skipped": 0}

        responses = [
            {"data": {"list": [{"id": "P-001", "communityId": "12436", "name": "Park A"}], "hasMore": True}},
            {"data": {"list": [{"id": "P-002", "communityId": "12436", "name": "Park B"}], "hasMore": False}},
        ]

        def fake_request(method, url, params=None, json_data=None, timeout=30, retry_on_401=True, extra_headers=None):
            request_calls.append({"method": method, "url": url, "params": params})
            return responses[len(request_calls) - 1]

        fetch_parks.marki_client.request = fake_request

        total = fetch_parks.sync_parks_for_community("12436")

        assert total == 2
        assert len(request_calls) == 2
        assert request_calls[0]["params"]["page"] == 1
        assert request_calls[1]["params"]["page"] == 2
    finally:
        fetch_parks.SessionLocal = original_session_local
        fetch_parks.build_variable_map = original_build_variable_map
        fetch_parks.get_api_url_by_id = original_get_api_url_by_id
        fetch_parks.insert_parks = original_insert_parks
        fetch_parks.marki_client.request = original_request
