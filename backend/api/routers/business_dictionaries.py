# -*- coding: utf-8 -*-
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import models
import schemas
from api.dependencies import _require_api_permission, get_current_user, get_db

router = APIRouter()


def _serialize_dictionary(dictionary: models.BusinessDictionary, item_count: Optional[int] = None) -> dict:
    return {
        "id": dictionary.id,
        "key": dictionary.key,
        "name": dictionary.name,
        "dict_type": dictionary.dict_type,
        "category": dictionary.category,
        "description": dictionary.description,
        "is_active": dictionary.is_active,
        "item_count": len(dictionary.items) if item_count is None else int(item_count or 0),
        "created_at": dictionary.created_at,
        "updated_at": dictionary.updated_at,
    }


def _build_item_path(item_id: int, item_map: Dict[int, models.BusinessDictionaryItem]) -> str:
    parts: List[str] = []
    cursor = item_map.get(item_id)
    visited = set()
    while cursor and cursor.id not in visited:
        visited.add(cursor.id)
        parts.append(cursor.label)
        cursor = item_map.get(cursor.parent_id) if cursor.parent_id else None
    return " / ".join(reversed(parts))


def _build_item_level(item_id: int, item_map: Dict[int, models.BusinessDictionaryItem]) -> int:
    level = 1
    cursor = item_map.get(item_id)
    visited = set()
    while cursor and cursor.parent_id and cursor.id not in visited:
        visited.add(cursor.id)
        level += 1
        cursor = item_map.get(cursor.parent_id)
    return level


def _serialize_item(
    item: models.BusinessDictionaryItem,
    item_map: Dict[int, models.BusinessDictionaryItem],
) -> dict:
    return {
        "id": item.id,
        "dictionary_id": item.dictionary_id,
        "code": item.code,
        "label": item.label,
        "value": item.value,
        "parent_id": item.parent_id,
        "sort_order": item.sort_order or 0,
        "status": item.status or 1,
        "description": item.description,
        "extra_json": item.extra_json,
        "level": _build_item_level(item.id, item_map),
        "path": _build_item_path(item.id, item_map),
        "created_at": item.created_at,
        "updated_at": item.updated_at,
        "children": [],
    }


def _build_item_tree(
    items: List[models.BusinessDictionaryItem],
    item_map: Dict[int, models.BusinessDictionaryItem],
    parent_id: Optional[int] = None,
) -> List[dict]:
    result: List[dict] = []
    children = [item for item in items if item.parent_id == parent_id]
    for child in sorted(children, key=lambda row: (row.sort_order or 0, row.id)):
        node = _serialize_item(child, item_map)
        node["children"] = _build_item_tree(items, item_map, child.id)
        result.append(node)
    return result


def _get_dictionary_or_404(db: Session, dictionary_id: int) -> models.BusinessDictionary:
    dictionary = (
        db.query(models.BusinessDictionary)
        .filter(models.BusinessDictionary.id == dictionary_id)
        .first()
    )
    if not dictionary:
        raise HTTPException(status_code=404, detail="Business dictionary not found")
    return dictionary


def _get_dictionary_by_key_or_404(db: Session, dict_key: str) -> models.BusinessDictionary:
    dictionary = (
        db.query(models.BusinessDictionary)
        .filter(models.BusinessDictionary.key == dict_key)
        .first()
    )
    if not dictionary:
        raise HTTPException(status_code=404, detail="Business dictionary not found")
    return dictionary


def _get_item_or_404(db: Session, item_id: int) -> models.BusinessDictionaryItem:
    item = (
        db.query(models.BusinessDictionaryItem)
        .filter(models.BusinessDictionaryItem.id == item_id)
        .first()
    )
    if not item:
        raise HTTPException(status_code=404, detail="Business dictionary item not found")
    return item


def _parse_extra_json(value: Optional[str]) -> Optional[str]:
    text = None if value is None else str(value).strip()
    if not text:
        return None
    try:
        import json

        parsed = json.loads(text)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"extra_json is invalid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="extra_json must be a JSON object")
    return text


def _validate_dictionary_payload(
    db: Session,
    payload: schemas.BusinessDictionaryCreate | schemas.BusinessDictionaryUpdate,
    *,
    existing: Optional[models.BusinessDictionary] = None,
) -> None:
    key = str((payload.key if hasattr(payload, "key") else None) or (existing.key if existing else "")).strip()
    name = str((payload.name if hasattr(payload, "name") else None) or (existing.name if existing else "")).strip()
    dict_type = (payload.dict_type if hasattr(payload, "dict_type") else None) or (existing.dict_type if existing else "enum")

    if not key:
        raise HTTPException(status_code=400, detail="Dictionary key is required")
    if not name:
        raise HTTPException(status_code=400, detail="Dictionary name is required")
    if dict_type not in {"enum", "hierarchy"}:
        raise HTTPException(status_code=400, detail="Unsupported dictionary type")

    query = db.query(models.BusinessDictionary).filter(models.BusinessDictionary.key == key)
    if existing:
        query = query.filter(models.BusinessDictionary.id != existing.id)
    duplicate = query.first()
    if duplicate:
        raise HTTPException(status_code=400, detail="Dictionary key already exists")

    if existing and dict_type == "enum":
        child_count = (
            db.query(models.BusinessDictionaryItem)
            .filter(
                models.BusinessDictionaryItem.dictionary_id == existing.id,
                models.BusinessDictionaryItem.parent_id.isnot(None),
            )
            .count()
        )
        if child_count > 0:
            raise HTTPException(
                status_code=400,
                detail="Cannot change dictionary type to enum while hierarchy items still exist",
            )


def _validate_item_parent(
    db: Session,
    dictionary: models.BusinessDictionary,
    parent_id: Optional[int],
    *,
    current_item_id: Optional[int] = None,
) -> Optional[models.BusinessDictionaryItem]:
    if dictionary.dict_type == "enum":
        if parent_id is not None:
            raise HTTPException(status_code=400, detail="Enum dictionary items cannot have parent_id")
        return None

    if parent_id is None:
        return None

    if current_item_id is not None and current_item_id == parent_id:
        raise HTTPException(status_code=400, detail="Dictionary item cannot be its own parent")

    parent = (
        db.query(models.BusinessDictionaryItem)
        .filter(models.BusinessDictionaryItem.id == parent_id)
        .first()
    )
    if not parent or parent.dictionary_id != dictionary.id:
        raise HTTPException(status_code=400, detail="Parent item not found in current dictionary")

    if current_item_id is not None:
        cursor = parent
        visited = {current_item_id}
        while cursor:
            if cursor.id in visited:
                raise HTTPException(status_code=400, detail="Circular parent relationship detected")
            visited.add(cursor.id)
            cursor = (
                db.query(models.BusinessDictionaryItem)
                .filter(models.BusinessDictionaryItem.id == cursor.parent_id)
                .first()
                if cursor.parent_id is not None
                else None
            )
    return parent


def _serialize_dictionary_items(items: List[models.BusinessDictionaryItem]) -> List[dict]:
    item_map = {item.id: item for item in items}
    return [
        _serialize_item(item, item_map)
        for item in sorted(items, key=lambda row: (row.sort_order or 0, row.id))
    ]


def _build_dictionary_resolve_response(dictionary: models.BusinessDictionary) -> schemas.BusinessDictionaryResolveResponse:
    items = (
        dictionary.items
        if "items" in dictionary.__dict__
        else []
    )
    item_map = {item.id: item for item in items}
    flat_items = [
        _serialize_item(item, item_map)
        for item in sorted(items, key=lambda row: (row.sort_order or 0, row.id))
    ]
    tree_items = _build_item_tree(items, item_map) if dictionary.dict_type == "hierarchy" else []
    return schemas.BusinessDictionaryResolveResponse(
        dictionary=_serialize_dictionary(dictionary, item_count=len(items)),
        items=flat_items,
        tree=tree_items,
    )


@router.get("/api/business-dictionaries", response_model=List[schemas.BusinessDictionaryResponse])
def list_business_dictionaries(
    dict_type: Optional[str] = Query(None),
    keyword: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    item_counts = (
        db.query(
            models.BusinessDictionary.id.label("dictionary_id"),
            func.count(models.BusinessDictionaryItem.id).label("item_count"),
        )
        .outerjoin(
            models.BusinessDictionaryItem,
            models.BusinessDictionaryItem.dictionary_id == models.BusinessDictionary.id,
        )
        .group_by(models.BusinessDictionary.id)
        .subquery()
    )

    query = (
        db.query(models.BusinessDictionary, item_counts.c.item_count)
        .outerjoin(item_counts, item_counts.c.dictionary_id == models.BusinessDictionary.id)
    )
    if dict_type:
        query = query.filter(models.BusinessDictionary.dict_type == dict_type)
    if keyword:
        like_text = f"%{keyword.strip()}%"
        query = query.filter(
            or_(
                models.BusinessDictionary.key.ilike(like_text),
                models.BusinessDictionary.name.ilike(like_text),
                models.BusinessDictionary.category.ilike(like_text),
                models.BusinessDictionary.description.ilike(like_text),
            )
        )
    rows = query.order_by(models.BusinessDictionary.id.desc()).all()
    return [_serialize_dictionary(dictionary, item_count=item_count) for dictionary, item_count in rows]


@router.get("/api/business-dictionaries/{dictionary_id}", response_model=schemas.BusinessDictionaryResponse)
def get_business_dictionary(
    dictionary_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    dictionary = _get_dictionary_or_404(db, dictionary_id)
    item_count = (
        db.query(models.BusinessDictionaryItem)
        .filter(models.BusinessDictionaryItem.dictionary_id == dictionary.id)
        .count()
    )
    return _serialize_dictionary(dictionary, item_count=item_count)


@router.get("/api/business-dictionaries/key/{dict_key}", response_model=schemas.BusinessDictionaryResolveResponse)
def resolve_business_dictionary_by_key(
    dict_key: str,
    active_only: bool = Query(True),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    dictionary = _get_dictionary_by_key_or_404(db, dict_key)
    items_query = db.query(models.BusinessDictionaryItem).filter(
        models.BusinessDictionaryItem.dictionary_id == dictionary.id
    )
    if active_only:
        items_query = items_query.filter(models.BusinessDictionaryItem.status == 1)
    dictionary.items = items_query.order_by(
        models.BusinessDictionaryItem.sort_order.asc(),
        models.BusinessDictionaryItem.id.asc(),
    ).all()
    return _build_dictionary_resolve_response(dictionary)


@router.post("/api/business-dictionaries", response_model=schemas.BusinessDictionaryResponse)
def create_business_dictionary(
    payload: schemas.BusinessDictionaryCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    _validate_dictionary_payload(db, payload)
    dictionary = models.BusinessDictionary(
        key=payload.key.strip(),
        name=payload.name.strip(),
        dict_type=payload.dict_type,
        category=(payload.category or "common").strip() or "common",
        description=payload.description,
        is_active=payload.is_active if payload.is_active is not None else True,
    )
    db.add(dictionary)
    db.commit()
    db.refresh(dictionary)
    return _serialize_dictionary(dictionary, item_count=0)


@router.put("/api/business-dictionaries/{dictionary_id}", response_model=schemas.BusinessDictionaryResponse)
def update_business_dictionary(
    dictionary_id: int,
    payload: schemas.BusinessDictionaryUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    dictionary = _get_dictionary_or_404(db, dictionary_id)
    _validate_dictionary_payload(db, payload, existing=dictionary)
    update_data = payload.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        if key in {"key", "name", "category"} and isinstance(value, str):
            value = value.strip()
        setattr(dictionary, key, value)
    db.commit()
    db.refresh(dictionary)
    item_count = (
        db.query(models.BusinessDictionaryItem)
        .filter(models.BusinessDictionaryItem.dictionary_id == dictionary.id)
        .count()
    )
    return _serialize_dictionary(dictionary, item_count=item_count)


@router.delete("/api/business-dictionaries/{dictionary_id}")
def delete_business_dictionary(
    dictionary_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    dictionary = _get_dictionary_or_404(db, dictionary_id)
    db.delete(dictionary)
    db.commit()
    return {"message": "Business dictionary deleted"}


@router.get("/api/business-dictionaries/{dictionary_id}/items", response_model=List[schemas.BusinessDictionaryItemResponse])
def list_business_dictionary_items(
    dictionary_id: int,
    active_only: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    dictionary = _get_dictionary_or_404(db, dictionary_id)
    query = db.query(models.BusinessDictionaryItem).filter(
        models.BusinessDictionaryItem.dictionary_id == dictionary.id
    )
    if active_only:
        query = query.filter(models.BusinessDictionaryItem.status == 1)
    items = query.order_by(
        models.BusinessDictionaryItem.sort_order.asc(),
        models.BusinessDictionaryItem.id.asc(),
    ).all()
    return _serialize_dictionary_items(items)


@router.get("/api/business-dictionaries/{dictionary_id}/tree", response_model=List[schemas.BusinessDictionaryItemResponse])
def get_business_dictionary_tree(
    dictionary_id: int,
    active_only: bool = Query(True),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    dictionary = _get_dictionary_or_404(db, dictionary_id)
    query = db.query(models.BusinessDictionaryItem).filter(
        models.BusinessDictionaryItem.dictionary_id == dictionary.id
    )
    if active_only:
        query = query.filter(models.BusinessDictionaryItem.status == 1)
    items = query.order_by(
        models.BusinessDictionaryItem.sort_order.asc(),
        models.BusinessDictionaryItem.id.asc(),
    ).all()
    item_map = {item.id: item for item in items}
    return _build_item_tree(items, item_map)


@router.post("/api/business-dictionaries/{dictionary_id}/items", response_model=schemas.BusinessDictionaryItemResponse)
def create_business_dictionary_item(
    dictionary_id: int,
    payload: schemas.BusinessDictionaryItemCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    dictionary = _get_dictionary_or_404(db, dictionary_id)
    _validate_item_parent(db, dictionary, payload.parent_id)
    item = models.BusinessDictionaryItem(
        dictionary_id=dictionary.id,
        code=payload.code.strip(),
        label=payload.label.strip(),
        value=payload.value,
        parent_id=payload.parent_id,
        sort_order=payload.sort_order or 0,
        status=payload.status if payload.status is not None else 1,
        description=payload.description,
        extra_json=_parse_extra_json(payload.extra_json),
    )
    db.add(item)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail="Dictionary item code already exists") from exc
    db.refresh(item)
    items = db.query(models.BusinessDictionaryItem).filter(
        models.BusinessDictionaryItem.dictionary_id == dictionary.id
    ).all()
    item_map = {row.id: row for row in items}
    return _serialize_item(item, item_map)


@router.put("/api/business-dictionaries/items/{item_id}", response_model=schemas.BusinessDictionaryItemResponse)
def update_business_dictionary_item(
    item_id: int,
    payload: schemas.BusinessDictionaryItemUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    item = _get_item_or_404(db, item_id)
    dictionary = _get_dictionary_or_404(db, item.dictionary_id)
    update_data = payload.model_dump(exclude_unset=True)
    next_parent_id = update_data.get("parent_id", item.parent_id)
    _validate_item_parent(db, dictionary, next_parent_id, current_item_id=item.id)

    if "extra_json" in update_data:
        update_data["extra_json"] = _parse_extra_json(update_data["extra_json"])

    for key, value in update_data.items():
        if key in {"code", "label"} and isinstance(value, str):
            value = value.strip()
        setattr(item, key, value)

    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail="Dictionary item code already exists") from exc
    db.refresh(item)
    items = db.query(models.BusinessDictionaryItem).filter(
        models.BusinessDictionaryItem.dictionary_id == dictionary.id
    ).all()
    item_map = {row.id: row for row in items}
    return _serialize_item(item, item_map)


@router.delete("/api/business-dictionaries/items/{item_id}")
def delete_business_dictionary_item(
    item_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "setting.manage")
    item = _get_item_or_404(db, item_id)
    child_exists = (
        db.query(models.BusinessDictionaryItem)
        .filter(models.BusinessDictionaryItem.parent_id == item.id)
        .first()
    )
    if child_exists:
        raise HTTPException(status_code=400, detail="Cannot delete item with children")
    db.delete(item)
    db.commit()
    return {"message": "Business dictionary item deleted"}
