from pathlib import Path
import sys

from fastapi import Request
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import models
from database import Base
from api.dependencies import get_allowed_community_ids


TEST_TABLES = [
    models.Organization.__table__,
    models.KingdeeAccountBook.__table__,
    models.ProjectList.__table__,
    models.User.__table__,
    models.user_account_books,
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


def seed_basic_scope_data(db):
    org = models.Organization(id=1, name="Org")
    book_a = models.KingdeeAccountBook(id="book-a", number="001", name="Book A")
    book_b = models.KingdeeAccountBook(id="book-b", number="002", name="Book B")
    project_a = models.ProjectList(proj_id=101, proj_name="Project A", kingdee_account_book_id="book-a")
    project_b = models.ProjectList(proj_id=202, proj_name="Project B", kingdee_account_book_id="book-b")
    db.add_all([org, book_a, book_b, project_a, project_b])
    db.commit()
    return org, book_a, book_b


def test_admin_scope_respects_selected_account_book():
    db = make_session()
    org, _, _ = seed_basic_scope_data(db)
    admin = models.User(
        id=1,
        username="admin",
        password_hash="x",
        org_id=org.id,
        status=1,
        role="admin",
    )
    db.add(admin)
    db.commit()

    request = make_request({"X-Account-Book-Number": "001"})

    assert get_allowed_community_ids(request, db, admin) == [101]


def test_regular_user_cannot_switch_into_unassigned_account_book():
    db = make_session()
    org, book_a, _ = seed_basic_scope_data(db)
    user = models.User(
        id=2,
        username="user-a",
        password_hash="x",
        org_id=org.id,
        status=1,
        role="user",
        account_books=[book_a],
    )
    db.add(user)
    db.commit()

    request = make_request({"X-Account-Book-Number": "002"})

    assert get_allowed_community_ids(request, db, user) == []


def test_regular_user_without_explicit_header_falls_back_to_authorized_books():
    db = make_session()
    org, book_a, _ = seed_basic_scope_data(db)
    user = models.User(
        id=3,
        username="user-b",
        password_hash="x",
        org_id=org.id,
        status=1,
        role="user",
        account_books=[book_a],
    )
    db.add(user)
    db.commit()

    request = make_request()

    assert get_allowed_community_ids(request, db, user) == [101]
