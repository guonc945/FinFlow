# -*- coding: utf-8 -*-
from importlib import import_module
from typing import List

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

import models
import schemas
from api.dependencies import get_current_user, get_db

router = APIRouter()


def _main_attr(name: str):
    return getattr(import_module("main"), name)


@router.get("/api/vouchers/source-fields")
def get_voucher_source_fields(source_type: str = Query("bills")):
    return _main_attr("get_voucher_source_fields")(source_type)


@router.get("/api/vouchers/source-modules")
def get_voucher_source_modules():
    return _main_attr("get_voucher_source_modules")()


@router.get("/api/vouchers/templates", response_model=List[schemas.VoucherTemplateResponse])
def get_voucher_templates(
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    return _main_attr("get_voucher_templates")(db, current_user)


@router.get("/api/vouchers/templates/{template_id}", response_model=schemas.VoucherTemplateResponse)
def get_voucher_template(
    template_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    return _main_attr("get_voucher_template")(template_id, db, current_user)


@router.post("/api/vouchers/templates", response_model=schemas.VoucherTemplateResponse)
def create_voucher_template(
    template: schemas.VoucherTemplateCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    return _main_attr("create_voucher_template")(template, db, current_user)


@router.put("/api/vouchers/templates/{template_id}", response_model=schemas.VoucherTemplateResponse)
def update_voucher_template(
    template_id: str,
    template: schemas.VoucherTemplateUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    return _main_attr("update_voucher_template")(template_id, template, db, current_user)


@router.delete("/api/vouchers/templates/{template_id}")
def delete_voucher_template(
    template_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    return _main_attr("delete_voucher_template")(template_id, db, current_user)


@router.post("/api/vouchers/resolve-fields")
def resolve_voucher_fields(
    payload: dict,
    db: Session = Depends(get_db),
):
    return _main_attr("resolve_voucher_fields")(payload, db)
