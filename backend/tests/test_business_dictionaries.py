from pathlib import Path
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi import HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import models
import schemas
from api.routers import business_dictionaries
from database import Base


TEST_TABLES = [
    models.Organization.__table__,
    models.User.__table__,
    models.BusinessDictionary.__table__,
    models.BusinessDictionaryItem.__table__,
]


def make_session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine, tables=TEST_TABLES)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return Session()


def make_admin(db):
    org = models.Organization(id=1, name="Org")
    admin = models.User(
        id=1,
        username="admin",
        password_hash="x",
        org_id=1,
        status=1,
        role="admin",
    )
    db.add_all([org, admin])
    db.commit()
    return admin


def test_enum_dictionary_returns_flat_items():
    db = make_session()
    admin = make_admin(db)
    original_require = business_dictionaries._require_api_permission
    business_dictionaries._require_api_permission = lambda *args, **kwargs: None
    try:
        dictionary = business_dictionaries.create_business_dictionary(
            payload=schemas.BusinessDictionaryCreate(
                key="bill_status",
                name="账单状态",
                dict_type="enum",
            ),
            db=db,
            current_user=admin,
        )
        business_dictionaries.create_business_dictionary_item(
            dictionary_id=dictionary["id"],
            payload=schemas.BusinessDictionaryItemCreate(code="pending", label="待处理", value="P"),
            db=db,
            current_user=admin,
        )
        business_dictionaries.create_business_dictionary_item(
            dictionary_id=dictionary["id"],
            payload=schemas.BusinessDictionaryItemCreate(code="paid", label="已支付", value="Y"),
            db=db,
            current_user=admin,
        )

        resolved = business_dictionaries.resolve_business_dictionary_by_key(
            dict_key="bill_status",
            active_only=True,
            db=db,
            current_user=admin,
        )
    finally:
        business_dictionaries._require_api_permission = original_require

    assert resolved.dictionary.key == "bill_status"
    assert resolved.dictionary.dict_type == "enum"
    assert [item.code for item in resolved.items] == ["pending", "paid"]
    assert resolved.tree == []


def test_hierarchy_dictionary_builds_tree_and_blocks_cycle():
    db = make_session()
    admin = make_admin(db)
    original_require = business_dictionaries._require_api_permission
    business_dictionaries._require_api_permission = lambda *args, **kwargs: None
    try:
        dictionary = business_dictionaries.create_business_dictionary(
            payload=schemas.BusinessDictionaryCreate(
                key="project_category",
                name="项目分类",
                dict_type="hierarchy",
            ),
            db=db,
            current_user=admin,
        )
        root = business_dictionaries.create_business_dictionary_item(
            dictionary_id=dictionary["id"],
            payload=schemas.BusinessDictionaryItemCreate(code="root", label="根分类"),
            db=db,
            current_user=admin,
        )
        child = business_dictionaries.create_business_dictionary_item(
            dictionary_id=dictionary["id"],
            payload=schemas.BusinessDictionaryItemCreate(code="child", label="子分类", parent_id=root["id"]),
            db=db,
            current_user=admin,
        )

        tree = business_dictionaries.get_business_dictionary_tree(
            dictionary_id=dictionary["id"],
            active_only=True,
            db=db,
            current_user=admin,
        )

        error_text = None
        try:
            business_dictionaries.update_business_dictionary_item(
                item_id=root["id"],
                payload=schemas.BusinessDictionaryItemUpdate(parent_id=child["id"]),
                db=db,
                current_user=admin,
            )
        except HTTPException as exc:
            error_text = str(exc.detail)
    finally:
        business_dictionaries._require_api_permission = original_require

    assert len(tree) == 1
    assert tree[0]["code"] == "root"
    assert tree[0]["children"][0]["code"] == "child"
    assert tree[0]["children"][0]["path"] == "根分类 / 子分类"
    assert error_text and "Circular parent relationship detected" in error_text
