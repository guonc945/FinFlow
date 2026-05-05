# -*- coding: utf-8 -*-
from fastapi import APIRouter

from api.routers import reporting_apps, reporting_categories, reporting_connections, reporting_datasets


router = APIRouter()
router.include_router(reporting_connections.router)
router.include_router(reporting_datasets.router)
router.include_router(reporting_apps.router)
router.include_router(reporting_categories.router)
