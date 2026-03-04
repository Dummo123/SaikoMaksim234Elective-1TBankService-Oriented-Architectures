from datetime import datetime, timezone

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_async_session
from src.models.db import PromoCodeDB
from src.routers.auth import RoleChecker, _auth_guard

router = APIRouter()

allow_sellers_admins = RoleChecker(["SELLER", "ADMIN"])


def error_resp(code: int, err_code: str, msg: str):
    return JSONResponse(status_code=code, content={"error_code": err_code, "message": msg})


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_promo_code(
    body:    dict,
    session: AsyncSession = Depends(get_async_session),
    user:    dict         = Depends(allow_sellers_admins),
):
    if (err := _auth_guard(user)):
        return err

    # Check for duplicate code
    existing = (await session.execute(select(PromoCodeDB).where(PromoCodeDB.code == body.get("code")))).scalars().first()
    if existing:
        return error_resp(409, "PROMO_CODE_EXISTS", "A promo code with that code already exists")

    valid_from  = datetime.fromisoformat(body["valid_from"]).replace(tzinfo=timezone.utc)
    valid_until = datetime.fromisoformat(body["valid_until"]).replace(tzinfo=timezone.utc)

    if valid_until <= valid_from:
        return error_resp(400, "VALIDATION_ERROR", "valid_until must be after valid_from")

    promo = PromoCodeDB(
        code=body["code"],
        discount_type=body["discount_type"],
        discount_value=body["discount_value"],
        min_order_amount=body.get("min_order_amount", 0),
        max_uses=body["max_uses"],
        valid_from=valid_from,
        valid_until=valid_until,
        active=body.get("active", True),
    )
    session.add(promo)
    await session.commit()
    await session.refresh(promo)

    return {
        "id":               str(promo.id),
        "code":             promo.code,
        "discount_type":    promo.discount_type,
        "discount_value":   float(promo.discount_value),
        "min_order_amount": float(promo.min_order_amount),
        "max_uses":         promo.max_uses,
        "current_uses":     promo.current_uses,
        "valid_from":       promo.valid_from.isoformat(),
        "valid_until":      promo.valid_until.isoformat(),
        "active":           promo.active,
    }
