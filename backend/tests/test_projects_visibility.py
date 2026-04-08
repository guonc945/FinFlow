from pathlib import Path
import sys

from fastapi import Request
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import models
from api.routers import master_data
from database import Base


TEST_TABLES = [
    models.Organization.__table__,
    models.KingdeeAccountBook.__table__,
    models.AuxiliaryData.__table__,
    models.KingdeeBankAccount.__table__,
    models.ProjectList.__table__,
    models.User.__table__,
]


def make_session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine, tables=TEST_TABLES)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def make_request(headers=None):
    normalized_headers = []
    for key, value in (headers or {}).items():
        normalized_headers.append((key.lower().encode("latin-1"), str(value).encode("latin-1")))
    return Request({"type": "http", "headers": normalized_headers})


def seed_projects(db):
    org = models.Organization(id=1, name="Org")
    admin = models.User(
        id=1,
        username="admin",
        password_hash="x",
        org_id=org.id,
        status=1,
        role="admin",
    )
    book_a = models.KingdeeAccountBook(id="book-a", number="001", name="Book A")
    book_b = models.KingdeeAccountBook(id="book-b", number="002", name="Book B")
    root_aux = models.AuxiliaryData(
        id="aux-root",
        number="ROOT",
        name="Root Project",
        group_name="管理项目",
    )
    child_aux = models.AuxiliaryData(
        id="aux-child",
        number="CHILD",
        name="Child Project",
        group_name="管理项目",
        parent_number="ROOT",
        parent_name="Root Project",
    )
    project_a = models.ProjectList(
        proj_id=101,
        proj_name="Mapped A",
        kingdee_account_book_id="book-a",
        kingdee_project_id="aux-child",
    )
    project_b = models.ProjectList(proj_id=202, proj_name="Mapped B", kingdee_account_book_id="book-b")
    project_unmapped = models.ProjectList(proj_id=303, proj_name="Unmapped", kingdee_account_book_id=None)
    db.add_all([org, admin, book_a, book_b, root_aux, child_aux, project_a, project_b, project_unmapped])
    db.commit()
    return admin


def test_projects_page_is_not_filtered_by_current_account_book():
    db = make_session()
    admin = seed_projects(db)
    request = make_request({"X-Account-Book-Number": "001"})

    result = master_data.get_projects(
        request=request,
        skip=0,
        limit=100,
        current_account_book_only=False,
        db=db,
        current_user=admin,
        allowed_community_ids=[101],
    )

    assert [item["proj_id"] for item in result["items"]] == [101, 202, 303]
    assert result["total"] == 3
    assert result["items"][0]["kingdee_project"]["full_path"] == "CHILD Root Project / Child Project"
    assert result["items"][0]["kingdee_project"]["parent_number"] == "ROOT"


def test_current_account_book_only_still_filters_projects():
    db = make_session()
    admin = seed_projects(db)
    request = make_request({"X-Account-Book-Number": "001"})
    original_decode = master_data._decode_header_value
    master_data._decode_header_value = lambda value: value
    try:
        result = master_data.get_projects(
            request=request,
            skip=0,
            limit=100,
            current_account_book_only=True,
            db=db,
            current_user=admin,
            allowed_community_ids=[101],
        )
    finally:
        master_data._decode_header_value = original_decode

    assert [item["proj_id"] for item in result["items"]] == [101]
    assert result["total"] == 1
