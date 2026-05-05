# -*- coding: utf-8 -*-
from importlib import import_module
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

import models
import schemas
from api.dependencies import get_current_user, get_db

router = APIRouter()


def _main_attr(name: str):
    return getattr(import_module("main"), name)


def _require_api_permission(*args, **kwargs):
    return _main_attr("_require_api_permission")(*args, **kwargs)


def build_org_tree(orgs: List[models.Organization], parent_id=None):
    """Build organization tree structure recursively."""
    tree = []
    for org in orgs:
        if org.parent_id == parent_id:
            node = {
                "id": org.id,
                "name": org.name,
                "code": org.code,
                "parent_id": org.parent_id,
                "level": org.level,
                "sort_order": org.sort_order,
                "status": org.status,
                "description": org.description,
                "created_at": org.created_at,
                "updated_at": org.updated_at,
                "children": build_org_tree(orgs, org.id),
            }
            tree.append(node)
    return tree


def build_template_category_tree(
    categories: List[models.VoucherTemplateCategory],
    parent_id=None,
    parent_path: str = "",
):
    tree = []
    for cat in categories:
        if cat.parent_id == parent_id:
            path = f"{parent_path} / {cat.name}" if parent_path else cat.name
            node = {
                "id": cat.id,
                "name": cat.name,
                "parent_id": cat.parent_id,
                "sort_order": cat.sort_order,
                "status": cat.status,
                "description": cat.description,
                "path": path,
                "created_at": cat.created_at,
                "updated_at": cat.updated_at,
                "children": build_template_category_tree(categories, cat.id, path),
            }
            tree.append(node)
    return tree


def build_template_category_path_map(categories: List[models.VoucherTemplateCategory]) -> Dict[int, str]:
    by_id = {c.id: c for c in categories}
    cache: Dict[int, str] = {}

    def resolve(cat_id: int) -> Optional[str]:
        if cat_id in cache:
            return cache[cat_id]
        cat = by_id.get(cat_id)
        if not cat:
            return None
        if cat.parent_id and cat.parent_id in by_id:
            parent_path = resolve(cat.parent_id)
            path = f"{parent_path} / {cat.name}" if parent_path else cat.name
        else:
            path = cat.name
        cache[cat_id] = path
        return path

    for cid in by_id.keys():
        resolve(cid)
    return cache


@router.get("/api/organizations")
def get_organizations(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Get all organizations as flat list."""
    _require_api_permission(db, current_user, "organization.manage")
    orgs = db.query(models.Organization).order_by(models.Organization.sort_order).offset(skip).limit(limit).all()
    return [
        {
            "id": org.id,
            "name": org.name,
            "code": org.code,
            "parent_id": org.parent_id,
            "level": org.level,
            "sort_order": org.sort_order,
            "status": org.status,
            "description": org.description,
            "created_at": org.created_at,
            "updated_at": org.updated_at,
        }
        for org in orgs
    ]


@router.get("/api/organizations/tree")
def get_organizations_tree(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Get organizations as tree structure."""
    _require_api_permission(db, current_user, "organization.manage")
    orgs = db.query(models.Organization).order_by(models.Organization.sort_order).all()
    return build_org_tree(orgs, None)


@router.get("/api/organizations/{org_id}")
def get_organization(
    org_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "organization.manage")
    org = db.query(models.Organization).filter(models.Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    return {
        "id": org.id,
        "name": org.name,
        "code": org.code,
        "parent_id": org.parent_id,
        "level": org.level,
        "sort_order": org.sort_order,
        "status": org.status,
        "description": org.description,
        "created_at": org.created_at,
        "updated_at": org.updated_at,
    }


@router.post("/api/organizations")
def create_organization(
    org_data: schemas.OrganizationCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "organization.manage")
    if org_data.code:
        existing = db.query(models.Organization).filter(models.Organization.code == org_data.code).first()
        if existing:
            raise HTTPException(status_code=400, detail="Organization code already exists")

    org = models.Organization(
        name=org_data.name,
        code=org_data.code,
        parent_id=org_data.parent_id,
        level=org_data.level,
        sort_order=org_data.sort_order,
        status=org_data.status,
        description=org_data.description,
    )
    db.add(org)
    db.commit()
    db.refresh(org)
    return {"id": org.id, "message": "Organization created successfully"}


@router.put("/api/organizations/{org_id}")
def update_organization(
    org_id: int,
    org_data: schemas.OrganizationUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "organization.manage")
    org = db.query(models.Organization).filter(models.Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")

    update_data = org_data.dict(exclude_unset=True)
    if "code" in update_data and update_data["code"] is not None:
        existing = db.query(models.Organization).filter(
            models.Organization.code == update_data["code"],
            models.Organization.id != org_id,
        ).first()
        if existing:
            raise HTTPException(status_code=400, detail="Organization code already exists")

    for key, value in update_data.items():
        setattr(org, key, value)

    db.commit()
    return {"message": "Organization updated successfully"}


@router.delete("/api/organizations/{org_id}")
def delete_organization(
    org_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "organization.manage")
    org = db.query(models.Organization).filter(models.Organization.id == org_id).first()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")

    children = db.query(models.Organization).filter(models.Organization.parent_id == org_id).first()
    if children:
        raise HTTPException(status_code=400, detail="Cannot delete organization with children")

    users = db.query(models.User).filter(models.User.org_id == org_id).first()
    if users:
        raise HTTPException(status_code=400, detail="Cannot delete organization with users")

    db.delete(org)
    db.commit()
    return {"message": "Organization deleted successfully"}


@router.get("/api/vouchers/template-categories")
def get_voucher_template_categories(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "voucher_template.manage")
    categories = db.query(models.VoucherTemplateCategory).order_by(
        models.VoucherTemplateCategory.sort_order.asc(),
        models.VoucherTemplateCategory.id.asc(),
    ).all()
    path_map = build_template_category_path_map(categories)
    return [
        {
            "id": c.id,
            "name": c.name,
            "parent_id": c.parent_id,
            "sort_order": c.sort_order,
            "status": c.status,
            "description": c.description,
            "path": path_map.get(c.id),
            "created_at": c.created_at,
            "updated_at": c.updated_at,
        }
        for c in categories
    ]


@router.get("/api/vouchers/template-categories/tree")
def get_voucher_template_categories_tree(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "voucher_template.manage")
    categories = db.query(models.VoucherTemplateCategory).order_by(
        models.VoucherTemplateCategory.sort_order.asc(),
        models.VoucherTemplateCategory.id.asc(),
    ).all()
    return build_template_category_tree(categories, None, "")


@router.post("/api/vouchers/template-categories")
def create_voucher_template_category(
    payload: schemas.VoucherTemplateCategoryCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "voucher_template.manage")
    if payload.parent_id is not None:
        parent = db.query(models.VoucherTemplateCategory).filter(models.VoucherTemplateCategory.id == payload.parent_id).first()
        if not parent:
            raise HTTPException(status_code=400, detail="Parent category not found")

    category = models.VoucherTemplateCategory(
        name=payload.name,
        parent_id=payload.parent_id,
        sort_order=payload.sort_order,
        status=payload.status,
        description=payload.description,
    )
    db.add(category)
    db.commit()
    db.refresh(category)
    return {"id": category.id, "message": "Template category created successfully"}


@router.put("/api/vouchers/template-categories/{category_id}")
def update_voucher_template_category(
    category_id: int,
    payload: schemas.VoucherTemplateCategoryUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "voucher_template.manage")
    category = db.query(models.VoucherTemplateCategory).filter(models.VoucherTemplateCategory.id == category_id).first()
    if not category:
        raise HTTPException(status_code=404, detail="Template category not found")

    update_data = payload.dict(exclude_unset=True)
    if "parent_id" in update_data:
        next_parent_id = update_data["parent_id"]
        if next_parent_id == category_id:
            raise HTTPException(status_code=400, detail="Category cannot be its own parent")
        if next_parent_id is not None:
            parent = db.query(models.VoucherTemplateCategory).filter(models.VoucherTemplateCategory.id == next_parent_id).first()
            if not parent:
                raise HTTPException(status_code=400, detail="Parent category not found")

            cursor = parent
            while cursor and cursor.parent_id is not None:
                if cursor.parent_id == category_id:
                    raise HTTPException(status_code=400, detail="Invalid parent category (cycle detected)")
                cursor = db.query(models.VoucherTemplateCategory).filter(models.VoucherTemplateCategory.id == cursor.parent_id).first()

    for key, value in update_data.items():
        setattr(category, key, value)

    db.commit()
    return {"message": "Template category updated successfully"}


@router.delete("/api/vouchers/template-categories/{category_id}")
def delete_voucher_template_category(
    category_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    _require_api_permission(db, current_user, "voucher_template.manage")
    category = db.query(models.VoucherTemplateCategory).filter(models.VoucherTemplateCategory.id == category_id).first()
    if not category:
        raise HTTPException(status_code=404, detail="Template category not found")

    has_children = db.query(models.VoucherTemplateCategory).filter(models.VoucherTemplateCategory.parent_id == category_id).first()
    if has_children:
        raise HTTPException(status_code=400, detail="Cannot delete category with children")

    bound_template = db.query(models.VoucherTemplate).filter(models.VoucherTemplate.category_id == category_id).first()
    if bound_template:
        raise HTTPException(status_code=400, detail="Cannot delete category with existing voucher templates")

    db.delete(category)
    db.commit()
    return {"message": "Template category deleted successfully"}
