# -*- coding: utf-8 -*-
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

import models
import schemas
from api.dependencies import _require_api_permission, get_current_user, get_db


router = APIRouter()


def _build_category_tree(items: List[models.ReportingReportCategory], parent_id: Optional[int] = None) -> List[dict]:
    """Build a tree structure from flat category list."""
    children = [item for item in items if item.parent_id == parent_id]
    result = []
    for child in sorted(children, key=lambda x: (x.sort_order or 0, x.id)):
        node = _serialize_category(child)
        node["children"] = _build_category_tree(items, parent_id=child.id)
        result.append(node)
    return result


def _build_category_path(items: List[models.ReportingReportCategory], category_id: int) -> str:
    """Build a path string like 'Parent / Child / Grandchild'."""
    id_map = {item.id: item for item in items}
    parts = []
    current_id = category_id
    visited = set()
    while current_id and current_id not in visited:
        visited.add(current_id)
        cat = id_map.get(current_id)
        if not cat:
            break
        parts.append(cat.name)
        current_id = cat.parent_id
    return " / ".join(reversed(parts))


def _serialize_category(category: models.ReportingReportCategory) -> dict:
    return {
        "id": category.id,
        "name": category.name,
        "parent_id": category.parent_id,
        "sort_order": category.sort_order or 0,
        "status": category.status or 1,
        "description": category.description,
        "path": None,
        "created_at": category.created_at,
        "updated_at": category.updated_at,
        "children": [],
    }


@router.get("/api/reporting/report-categories")
def list_reporting_report_categories(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    items = db.query(models.ReportingReportCategory).order_by(
        models.ReportingReportCategory.sort_order,
        models.ReportingReportCategory.id,
    ).all()

    # Build paths
    id_map = {item.id: item for item in items}
    result = []
    for item in items:
        serialized = _serialize_category(item)
        serialized["path"] = _build_category_path(items, item.id)
        result.append(serialized)
    return result


@router.get("/api/reporting/report-categories/tree")
def get_reporting_report_categories_tree(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    items = db.query(models.ReportingReportCategory).order_by(
        models.ReportingReportCategory.sort_order,
        models.ReportingReportCategory.id,
    ).all()
    return _build_category_tree(items)


@router.get("/api/reporting/report-categories/{category_id}")
def get_reporting_report_category(
    category_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    item = db.query(models.ReportingReportCategory).filter(
        models.ReportingReportCategory.id == category_id
    ).first()
    if not item:
        raise HTTPException(status_code=404, detail="Category not found")

    all_items = db.query(models.ReportingReportCategory).all()
    serialized = _serialize_category(item)
    serialized["path"] = _build_category_path(all_items, item.id)
    return serialized


@router.post("/api/reporting/report-categories", response_model=schemas.ReportingReportCategoryResponse)
def create_reporting_report_category(
    payload: schemas.ReportingReportCategoryCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    if payload.parent_id is not None:
        parent = db.query(models.ReportingReportCategory).filter(
            models.ReportingReportCategory.id == payload.parent_id
        ).first()
        if not parent:
            raise HTTPException(status_code=400, detail="Parent category not found")

    category = models.ReportingReportCategory(
        name=payload.name,
        parent_id=payload.parent_id,
        sort_order=payload.sort_order or 0,
        status=payload.status or 1,
        description=payload.description,
    )
    db.add(category)
    db.commit()
    db.refresh(category)

    all_items = db.query(models.ReportingReportCategory).all()
    result = _serialize_category(category)
    result["path"] = _build_category_path(all_items, category.id)
    return result


@router.put("/api/reporting/report-categories/{category_id}", response_model=schemas.ReportingReportCategoryResponse)
def update_reporting_report_category(
    category_id: int,
    payload: schemas.ReportingReportCategoryUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    category = db.query(models.ReportingReportCategory).filter(
        models.ReportingReportCategory.id == category_id
    ).first()
    if not category:
        raise HTTPException(status_code=404, detail="Category not found")

    update_data = payload.dict(exclude_unset=True)

    # Prevent circular reference
    if "parent_id" in update_data and update_data["parent_id"] is not None:
        if update_data["parent_id"] == category_id:
            raise HTTPException(status_code=400, detail="Category cannot be its own parent")
        # Check deeper circular references
        check_id = update_data["parent_id"]
        visited = {category_id}
        while check_id is not None and check_id not in visited:
            visited.add(check_id)
            parent_cat = db.query(models.ReportingReportCategory).filter(
                models.ReportingReportCategory.id == check_id
            ).first()
            if not parent_cat:
                break
            check_id = parent_cat.parent_id
        if check_id in visited and check_id != update_data["parent_id"]:
            raise HTTPException(status_code=400, detail="Circular reference detected")

        parent = db.query(models.ReportingReportCategory).filter(
            models.ReportingReportCategory.id == update_data["parent_id"]
        ).first()
        if not parent:
            raise HTTPException(status_code=400, detail="Parent category not found")

    for key, value in update_data.items():
        setattr(category, key, value)

    db.commit()
    db.refresh(category)

    all_items = db.query(models.ReportingReportCategory).all()
    result = _serialize_category(category)
    result["path"] = _build_category_path(all_items, category.id)
    return result


@router.delete("/api/reporting/report-categories/{category_id}")
def delete_reporting_report_category(
    category_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "reporting.manage")
    category = db.query(models.ReportingReportCategory).filter(
        models.ReportingReportCategory.id == category_id
    ).first()
    if not category:
        raise HTTPException(status_code=404, detail="Category not found")

    # Check for children
    children_count = db.query(models.ReportingReportCategory).filter(
        models.ReportingReportCategory.parent_id == category_id
    ).count()
    if children_count > 0:
        raise HTTPException(status_code=400, detail="Cannot delete category with children. Move or delete children first.")

    # Clear category_id on reports referencing this category
    db.query(models.ReportingReport).filter(
        models.ReportingReport.category_id == category_id
    ).update({"category_id": None})

    db.delete(category)
    db.commit()
    return {"message": "Category deleted"}
