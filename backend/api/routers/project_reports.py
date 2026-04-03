# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from typing import List

from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session

import models
from api.dependencies import (
    _require_api_permission,
    get_allowed_community_ids,
    get_current_user,
    get_db,
)
from scripts.fetch_projects import main as fetch_projects_main

router = APIRouter()
logger = logging.getLogger("project_sync")


@router.post("/api/projects/sync")
def sync_projects_endpoint(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    """Trigger project synchronization in background."""
    _require_api_permission(db, current_user, "project.manage")

    def run_sync():
        try:
            logger.info("Project sync started")
            fetch_projects_main()
            logger.info("Project sync completed successfully")
        except Exception as e:
            logger.error(f"Project sync failed: {e}")
            raise

    background_tasks.add_task(run_sync)
    return {"detail": "Project synchronization started"}


@router.get("/api/reports/income-trend")
def get_income_trend(
    period: str = "month",
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    if not allowed_community_ids:
        return {"labels": [], "data": []}

    if period == "month":
        rows = db.query(
            models.Bill.pay_time,
            models.Bill.amount,
        ).filter(
            models.Bill.pay_status_str == "已缴",
            models.Bill.pay_time != None,
            models.Bill.community_id.in_(allowed_community_ids),
        ).all()

        months = {i: 0 for i in range(1, 13)}
        for row in rows:
            try:
                month_value = datetime.fromtimestamp(int(row.pay_time)).month
            except Exception:
                continue
            months[month_value] += float(row.amount or 0)

        return {"labels": list(range(1, 13)), "data": list(months.values())}

    return {"labels": [], "data": []}


@router.get("/api/reports/charge-items-ranking")
def get_charge_items_ranking(
    limit: int = 10,
    db: Session = Depends(get_db),
    allowed_community_ids: List[int] = Depends(get_allowed_community_ids),
):
    if not allowed_community_ids:
        return []

    data = db.query(
        models.Bill.charge_item_name,
        func.sum(models.Bill.amount).label("total"),
        func.count(models.Bill.id).label("count"),
    ).filter(
        models.Bill.charge_item_name != None,
        models.Bill.amount != None,
        models.Bill.community_id.in_(allowed_community_ids),
    ).group_by(
        models.Bill.charge_item_name
    ).order_by(
        func.sum(models.Bill.bill_amount).desc()
    ).limit(limit).all()

    total = sum(float(row.total) if row.total else 0 for row in data)

    return [
        {
            "item_name": row.charge_item_name,
            "amount": float(row.total) if row.total else 0,
            "count": row.count,
            "percentage": (float(row.total) / total * 100) if total > 0 else 0,
        }
        for row in data
    ]
