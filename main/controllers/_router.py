# mypy: ignore-errors

from fastapi import APIRouter

from main.controllers import coda_controller

from . import probe


router = APIRouter()

router.include_router(probe.router, tags=["probe"])
router.include_router(coda_controller.router)
