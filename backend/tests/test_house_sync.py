from pathlib import Path
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import fetch_houses
import models
from database import Base


TEST_TABLES = [
    models.ExternalService.__table__,
    models.ExternalApi.__table__,
    models.KingdeeHouse.__table__,
    models.ProjectList.__table__,
    models.House.__table__,
    models.HouseUser.__table__,
    models.Park.__table__,
]


def make_session_factory():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine, tables=TEST_TABLES)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def test_insert_houses_replaces_users_and_updates_park_links():
    session_factory = make_session_factory()
    original_session_local = fetch_houses.SessionLocal
    fetch_houses.SessionLocal = session_factory

    try:
        db = session_factory()
        existing_house = models.House(
            house_id="H-001",
            community_id="12436",
            community_name="旧园区",
            house_name="旧房号",
        )
        db.add(existing_house)
        db.commit()
        db.refresh(existing_house)

        db.add(
            models.HouseUser(
                house_fk=existing_house.id,
                item_id=1,
                name="旧住户",
            )
        )
        db.add(
            models.Park(
                park_id="P-001",
                community_id="12436",
                community_name="旧园区",
                name="车位A",
                house_id="H-001",
            )
        )
        db.commit()
        db.close()

        result = fetch_houses.insert_houses(
            [
                {
                    "id": "H-001",
                    "communityID": "12436",
                    "communityName": "新园区",
                    "name": "1栋1单元101",
                    "userList": [
                        {"id": 2, "name": "张三", "ownerPhone": "13800000000"},
                        {"id": 3, "name": "李四", "ownerPhone": "13900000000"},
                    ],
                },
                {
                    "id": "H-002",
                    "communityID": "12436",
                    "communityName": "新园区",
                    "name": "1栋1单元102",
                    "userList": [],
                },
                {
                    "id": "",
                    "communityID": "12436",
                    "communityName": "新园区",
                    "name": "无效数据",
                },
            ],
            community_name="兜底园区",
        )

        assert result == {"inserted": 2, "skipped": 1}

        db = session_factory()
        houses = {
            row.house_id: row
            for row in db.query(models.House).order_by(models.House.house_id).all()
        }
        assert sorted(houses.keys()) == ["H-001", "H-002"]
        assert houses["H-001"].house_name == "1栋1单元101"
        assert houses["H-001"].community_name == "新园区"

        users = db.query(models.HouseUser).order_by(models.HouseUser.item_id).all()
        assert [user.item_id for user in users] == [2, 3]
        assert [user.name for user in users] == ["张三", "李四"]

        park = db.query(models.Park).filter(models.Park.park_id == "P-001").first()
        assert park.house_fk == houses["H-001"].id
        db.close()
    finally:
        fetch_houses.SessionLocal = original_session_local


def test_sync_houses_for_community_continues_when_has_more_is_true():
    session_factory = make_session_factory()
    original_session_local = fetch_houses.SessionLocal
    original_build_variable_map = fetch_houses.build_variable_map
    original_get_api_url = fetch_houses.get_api_url
    original_insert_houses = fetch_houses.insert_houses
    original_request = fetch_houses.marki_client.request
    fetch_houses.SessionLocal = session_factory

    request_calls = []

    try:
        db = session_factory()
        db.add(models.ProjectList(proj_id=12436, proj_name="测试园区"))
        db.commit()
        db.close()

        fetch_houses.build_variable_map = lambda db: {}
        fetch_houses.get_api_url = lambda api_name: "https://example.test/getHouseList"
        fetch_houses.insert_houses = lambda data_list, community_name=None: {"inserted": len(data_list), "skipped": 0}

        responses = [
            {
                "data": {
                    "list": [{"id": "H-001", "communityID": "12436", "name": "101"}],
                    "hasMore": True,
                }
            },
            {
                "data": {
                    "list": [{"id": "H-002", "communityID": "12436", "name": "102"}],
                    "hasMore": False,
                }
            },
        ]

        def fake_request(method, url, params=None, json_data=None, timeout=30, retry_on_401=True, extra_headers=None):
            request_calls.append({"method": method, "url": url, "params": params})
            return responses[len(request_calls) - 1]

        fetch_houses.marki_client.request = fake_request

        total = fetch_houses.sync_houses_for_community("12436")

        assert total == 2
        assert len(request_calls) == 2
        assert request_calls[0]["params"]["page"] == 1
        assert request_calls[1]["params"]["page"] == 2
    finally:
        fetch_houses.SessionLocal = original_session_local
        fetch_houses.build_variable_map = original_build_variable_map
        fetch_houses.get_api_url = original_get_api_url
        fetch_houses.insert_houses = original_insert_houses
        fetch_houses.marki_client.request = original_request
