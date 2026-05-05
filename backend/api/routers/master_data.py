# -*- coding: utf-8 -*-
from importlib import import_module
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request
from sqlalchemy import String, cast, or_
from sqlalchemy.orm import Session, joinedload, selectinload

import models
import schemas
from api.dependencies import (
    _require_api_permission,
    get_allowed_community_ids,
    get_current_user,
    get_db,
)
from fetch_charge_items import sync_charge_items
from fetch_houses import sync_houses
from fetch_parks import sync_parks
from fetch_residents import sync_residents

router = APIRouter()


def _main_attr(name: str):
    return getattr(import_module("main"), name)


def _tracker():
    return _main_attr("tracker")


def _decode_header_value(*args, **kwargs):
    return _main_attr("_decode_header_value")(*args, **kwargs)


def _load_auxiliary_project_map(db: Session, projects: List[models.ProjectList]):
    pending_numbers = {
        (project.kingdee_project.number or "").strip()
        for project in projects
        if project.kingdee_project and (project.kingdee_project.number or "").strip()
    }
    auxiliary_by_number = {}

    while pending_numbers:
        batch_numbers = [number for number in pending_numbers if number and number not in auxiliary_by_number]
        if not batch_numbers:
            break

        rows = (
            db.query(models.AuxiliaryData)
            .filter(models.AuxiliaryData.number.in_(batch_numbers))
            .all()
        )
        pending_numbers = set()
        for row in rows:
            auxiliary_by_number[row.number] = row
            parent_number = (row.parent_number or "").strip()
            if parent_number and parent_number not in auxiliary_by_number:
                pending_numbers.add(parent_number)

    return auxiliary_by_number


def _build_auxiliary_project_full_path(
    project: Optional[models.AuxiliaryData],
    auxiliary_by_number,
):
    if not project:
        return None

    segments = []
    visited = set()
    current = auxiliary_by_number.get(project.number) or project

    while current and current.number and current.number not in visited:
        visited.add(current.number)
        if current.name:
            segments.insert(0, current.name)
        parent_number = (current.parent_number or "").strip()
        current = auxiliary_by_number.get(parent_number) if parent_number else None

    path_text = " / ".join(segments)
    return " ".join(part for part in [project.number, path_text] if part).strip()


@router.get("/api/charge-items", response_model=List[schemas.ChargeItemResponse])
def get_charge_items(
    request: Request,
    community_id: Optional[str] = None,
    search: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    _require_api_permission(db, current_user, "charge_item.manage")
    query = db.query(models.ChargeItem).options(
        joinedload(models.ChargeItem.kingdee_tax_rate),
    )
    if allowed_community_ids:
        c_ids = [str(cid) for cid in allowed_community_ids]
        query = query.filter(models.ChargeItem.communityid.in_(c_ids))
    else:
        return []

    account_book_number = _decode_header_value(request.headers.get("X-Account-Book-Number")) if request else None
    if account_book_number:
        book_community_rows = (
            db.query(models.ProjectList.proj_id)
            .join(
                models.KingdeeAccountBook,
                cast(models.ProjectList.kingdee_account_book_id, String) == cast(models.KingdeeAccountBook.id, String),
            )
            .filter(models.KingdeeAccountBook.number == account_book_number)
            .all()
        )
        if not book_community_rows:
            return []

        allowed_set = {str(cid) for cid in allowed_community_ids}
        book_set = {str(row[0]) for row in book_community_rows}
        scoped_community_ids = list(allowed_set & book_set)
        if not scoped_community_ids:
            return []
        query = query.filter(models.ChargeItem.communityid.in_(scoped_community_ids))

    if community_id:
        query = query.filter(models.ChargeItem.communityid == community_id)
    if search:
        search_filter = or_(
            models.ChargeItem.item_name.ilike(f"%{search}%"),
            cast(models.ChargeItem.item_id, String).ilike(f"%{search}%"),
            models.ChargeItem.category_name.ilike(f"%{search}%"),
        )
        query = query.filter(search_filter)

    items = query.order_by(models.ChargeItem.item_id.asc()).offset(skip).limit(limit).all()
    return items


@router.put("/api/charge-items/{item_id}")
def update_charge_item(
    item_id: int,
    data: schemas.ChargeItemUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "charge_item.manage")
    item = db.query(models.ChargeItem).filter(models.ChargeItem.item_id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Charge item not found")

    update_data = data.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(item, key, value)

    db.commit()
    return {"message": "Updated successfully"}


@router.post("/api/charge-items/sync")
def sync_charge_items_endpoint(
    background_tasks: BackgroundTasks,
    request: schemas.BillSyncRequest = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    """Sync charge items for the specified communities."""
    _require_api_permission(db, current_user, "charge_item.manage")
    if request and request.community_ids:
        community_ids = [cid for cid in request.community_ids if cid in allowed_community_ids]
    else:
        community_ids = allowed_community_ids

    if not community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")

    str_ids = [str(cid) for cid in community_ids]
    background_tasks.add_task(sync_charge_items, str_ids)
    return {"message": "Charge items sync started", "community_ids": str_ids}


@router.get("/api/projects")
def get_projects(
    request: Request,
    skip: int = 0,
    limit: int = 100,
    current_account_book_only: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    _require_api_permission(db, current_user, "project.manage")
    query = db.query(models.ProjectList)

    if current_account_book_only:
        if allowed_community_ids:
            query = query.filter(models.ProjectList.proj_id.in_(allowed_community_ids))
        else:
            return {"items": [], "total": 0}

        account_book_number = _decode_header_value(request.headers.get("X-Account-Book-Number")) if request else None
        if not account_book_number:
            return {"items": [], "total": 0}

        query = query.join(
            models.KingdeeAccountBook,
            cast(models.ProjectList.kingdee_account_book_id, String) == cast(models.KingdeeAccountBook.id, String),
        ).filter(models.KingdeeAccountBook.number == account_book_number)

    total = query.count()
    projects = query.options(
        joinedload(models.ProjectList.kingdee_project),
        joinedload(models.ProjectList.default_receive_bank),
        joinedload(models.ProjectList.default_pay_bank),
        joinedload(models.ProjectList.kingdee_account_book),
    ).order_by(models.ProjectList.proj_id.asc()).offset(skip).limit(limit).all()
    auxiliary_by_number = _load_auxiliary_project_map(db, projects)

    items = [
        {
            "proj_id": project.proj_id,
            "proj_name": project.proj_name,
            "kingdee_project_id": project.kingdee_project_id,
            "kingdee_project": {
                "id": project.kingdee_project.id,
                "number": project.kingdee_project.number,
                "name": project.kingdee_project.name,
                "group_name": project.kingdee_project.group_name,
                "group_number": project.kingdee_project.group_number,
                "parent_number": project.kingdee_project.parent_number,
                "parent_name": project.kingdee_project.parent_name,
                "full_path": _build_auxiliary_project_full_path(project.kingdee_project, auxiliary_by_number),
            }
            if project.kingdee_project
            else None,
            "default_receive_bank_id": project.default_receive_bank_id,
            "default_receive_bank": {
                "id": project.default_receive_bank.id,
                "name": project.default_receive_bank.name,
                "bankaccountnumber": project.default_receive_bank.bankaccountnumber,
                "bank_name": project.default_receive_bank.bank_name,
            }
            if project.default_receive_bank
            else None,
            "default_pay_bank_id": project.default_pay_bank_id,
            "default_pay_bank": {
                "id": project.default_pay_bank.id,
                "name": project.default_pay_bank.name,
                "bankaccountnumber": project.default_pay_bank.bankaccountnumber,
                "bank_name": project.default_pay_bank.bank_name,
            }
            if project.default_pay_bank
            else None,
            "kingdee_account_book_id": project.kingdee_account_book_id,
            "kingdee_account_book": {
                "id": project.kingdee_account_book.id,
                "number": project.kingdee_account_book.number,
                "name": project.kingdee_account_book.name,
            }
            if project.kingdee_account_book
            else None,
            "created_at": project.created_at,
        }
        for project in projects
    ]

    return {"items": items, "total": total}


@router.put("/api/projects/{proj_id}")
def update_project(
    proj_id: int,
    data: schemas.ProjectUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "project.manage")
    project = db.query(models.ProjectList).filter(models.ProjectList.proj_id == proj_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    if data.kingdee_project_id is not None:
        project.kingdee_project_id = data.kingdee_project_id
    if data.default_receive_bank_id is not None:
        project.default_receive_bank_id = data.default_receive_bank_id or None
    if data.default_pay_bank_id is not None:
        project.default_pay_bank_id = data.default_pay_bank_id or None
    if hasattr(data, "kingdee_account_book_id") and data.kingdee_account_book_id is not None:
        project.kingdee_account_book_id = data.kingdee_account_book_id or None

    db.commit()
    return {"message": "Success"}


@router.get("/api/houses", response_model=List[schemas.HouseResponse])
def get_houses(
    community_id: Optional[str] = None,
    search: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    query = db.query(models.House).options(
        joinedload(models.House.kingdee_house),
        selectinload(models.House.user_list),
        selectinload(models.House.parks),
    )

    if allowed_community_ids:
        c_ids = [str(cid) for cid in allowed_community_ids]
        query = query.filter(models.House.community_id.in_(c_ids))
    else:
        return []

    if community_id:
        query = query.filter(models.House.community_id == community_id)
    if search:
        search_filter = or_(
            models.House.house_name.ilike(f"%{search}%"),
            models.House.house_id.ilike(f"%{search}%"),
            models.House.building_name.ilike(f"%{search}%"),
        )
        query = query.filter(search_filter)
    houses = query.order_by(models.House.created_at.desc()).offset(skip).limit(limit).all()
    return houses


@router.put("/api/houses/{house_id}", response_model=schemas.HouseResponse)
def update_house(house_id: int, data: schemas.HouseUpdate, db: Session = Depends(get_db)):
    house = db.query(models.House).filter(models.House.id == house_id).first()
    if not house:
        raise HTTPException(status_code=404, detail="House not found")

    if data.kingdee_house_id is not None:
        house.kingdee_house_id = data.kingdee_house_id

    db.commit()
    db.refresh(house)
    return house


@router.post("/api/houses/sync")
def sync_houses_endpoint(
    background_tasks: BackgroundTasks,
    request: schemas.HouseSyncRequest,
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    """Sync house data for the specified communities."""
    if request.community_ids:
        community_ids = [cid for cid in request.community_ids if cid in allowed_community_ids]
    else:
        community_ids = allowed_community_ids

    if not community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")

    str_ids = [str(cid) for cid in community_ids]
    task_id = _tracker().create_task(str_ids)

    background_tasks.add_task(sync_houses, str_ids, task_id)
    return {
        "message": "House sync started",
        "task_id": task_id,
        "community_ids": str_ids,
    }


@router.get("/api/residents")
def get_residents(
    community_id: Optional[str] = None,
    search: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    query = db.query(models.Resident)

    if allowed_community_ids:
        c_ids = [str(cid) for cid in allowed_community_ids]
        query = query.filter(models.Resident.community_id.in_(c_ids))
    else:
        return {"items": [], "total": 0}

    if community_id:
        query = query.filter(models.Resident.community_id == community_id)
    if search:
        search_filter = or_(
            models.Resident.name.ilike(f"%{search}%"),
            models.Resident.resident_id.ilike(f"%{search}%"),
            models.Resident.phone.ilike(f"%{search}%"),
        )
        query = query.filter(search_filter)

    total = query.count()
    residents = (
        query.options(joinedload(models.Resident.kingdee_customer))
        .order_by(models.Resident.created_at.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

    return {
        "items": residents,
        "total": total,
    }


@router.put("/api/residents/{resident_id}", response_model=schemas.ResidentResponse)
def update_resident(resident_id: int, data: schemas.ResidentUpdate, db: Session = Depends(get_db)):
    resident = db.query(models.Resident).filter(models.Resident.id == resident_id).first()
    if not resident:
        raise HTTPException(status_code=404, detail="Resident not found")

    if data.kingdee_customer_id is not None:
        resident.kingdee_customer_id = data.kingdee_customer_id

    db.commit()
    db.refresh(resident)
    return resident


@router.post("/api/residents/sync")
def sync_residents_endpoint(
    background_tasks: BackgroundTasks,
    request: schemas.ResidentSyncRequest,
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    """Sync resident data for the specified communities."""
    if request.community_ids:
        community_ids = [cid for cid in request.community_ids if cid in allowed_community_ids]
    else:
        community_ids = allowed_community_ids

    if not community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")

    str_ids = [str(cid) for cid in community_ids]
    task_id = _tracker().create_task(str_ids)

    background_tasks.add_task(sync_residents, str_ids, task_id)
    return {
        "message": "Resident sync started",
        "task_id": task_id,
        "community_ids": str_ids,
    }


@router.get("/api/parks", response_model=List[schemas.ParkResponse])
def get_parks(
    community_id: Optional[str] = None,
    search: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    query = db.query(models.Park)

    if allowed_community_ids:
        c_ids = [str(cid) for cid in allowed_community_ids]
        query = query.filter(models.Park.community_id.in_(c_ids))
    else:
        return []

    if community_id:
        query = query.filter(models.Park.community_id == community_id)
    if search:
        search_filter = or_(
            models.Park.name.ilike(f"%{search}%"),
            models.Park.park_id.ilike(f"%{search}%"),
            models.Park.user_name.ilike(f"%{search}%"),
        )
        query = query.filter(search_filter)
    parks = query.order_by(models.Park.created_at.desc()).offset(skip).limit(limit).all()
    return parks


@router.put("/api/parks/{park_id}")
def update_park(park_id: int, data: schemas.ParkUpdate, db: Session = Depends(get_db)):
    park = db.query(models.Park).filter(models.Park.id == park_id).first()
    if not park:
        raise HTTPException(status_code=404, detail="Park not found")

    update_data = data.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(park, key, value)

    db.commit()
    db.refresh(park)
    return {"message": "Updated successfully"}


@router.post("/api/parks/sync")
def sync_parks_endpoint(
    background_tasks: BackgroundTasks,
    request: schemas.ParkSyncRequest,
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    """Sync park data for the specified communities."""
    if request.community_ids:
        community_ids = [cid for cid in request.community_ids if cid in allowed_community_ids]
    else:
        community_ids = allowed_community_ids

    if not community_ids:
        raise HTTPException(status_code=403, detail="No authorized communities for this account book")

    str_ids = [str(cid) for cid in community_ids]
    task_id = _tracker().create_task(str_ids)

    background_tasks.add_task(sync_parks, str_ids, task_id)
    return {
        "message": "Park sync started",
        "task_id": task_id,
        "community_ids": str_ids,
    }
