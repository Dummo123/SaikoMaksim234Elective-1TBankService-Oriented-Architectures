import uuid
from typing import Optional

from fastapi import APIRouter, Depends, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_async_session
from src.models.db import ProductDB
from src.models.generated import (
    PaginatedProductResponse,
    ProductCreate,
    ProductResponse,
    ProductStatus,
    ProductUpdate,
)
from src.routers.auth import RoleChecker

router = APIRouter()

allow_all            = RoleChecker(["USER", "SELLER", "ADMIN"])
allow_sellers_admins = RoleChecker(["SELLER", "ADMIN"])


def _not_found():
    return JSONResponse(
        status_code=404,
        content={"error_code": "PRODUCT_NOT_FOUND", "message": "Product not found", "details": None},
    )


def _forbidden():
    return JSONResponse(
        status_code=403,
        content={"error_code": "FORBIDDEN", "message": "Access denied", "details": None},
    )


def _auth_guard(user):
    """Return a JSONResponse error if RoleChecker rejected the request."""
    if user is None:
        return JSONResponse(status_code=401, content={"error_code": "UNAUTHORIZED", "message": "Authentication required"})
    if user is False:
        return _forbidden()
    return None


@router.get("", response_model=PaginatedProductResponse)
async def list_products(
    page:     int                    = Query(0,  ge=0),
    size:     int                    = Query(20, ge=1),
    status:   Optional[ProductStatus] = None,
    category: Optional[str]          = None,
    session:  AsyncSession           = Depends(get_async_session),
    user:     dict                   = Depends(allow_all),
):
    if (err := _auth_guard(user)):
        return err

    query = select(ProductDB)
    if status:
        query = query.where(ProductDB.status == status.value)
    if category:
        query = query.where(ProductDB.category == category)

    total = (await session.execute(select(func.count()).select_from(query.subquery()))).scalar_one()
    items = (await session.execute(query.offset(page * size).limit(size))).scalars().all()

    return PaginatedProductResponse(items=items, totalElements=total, page=page, size=size)


@router.post("", response_model=ProductResponse, status_code=status.HTTP_201_CREATED)
async def create_product(
    body:    ProductCreate,
    session: AsyncSession = Depends(get_async_session),
    user:    dict         = Depends(allow_sellers_admins),
):
    if (err := _auth_guard(user)):
        return err

    product = ProductDB(
        name=body.name,
        description=body.description,
        price=body.price,
        stock=body.stock,
        category=body.category,
        status=body.status.value,
        seller_id=uuid.UUID(user["sub"]) if user["role"] == "SELLER" else None,
    )
    session.add(product)
    await session.commit()
    await session.refresh(product)
    return product


@router.get("/{id}", response_model=ProductResponse)
async def get_product(
    id:      uuid.UUID,
    session: AsyncSession = Depends(get_async_session),
    user:    dict         = Depends(allow_all),
):
    if (err := _auth_guard(user)):
        return err

    product = (await session.execute(select(ProductDB).where(ProductDB.id == id))).scalar_one_or_none()
    if not product:
        return _not_found()
    return product


@router.put("/{id}", response_model=ProductResponse)
async def update_product(
    id:      uuid.UUID,
    body:    ProductUpdate,
    session: AsyncSession = Depends(get_async_session),
    user:    dict         = Depends(allow_sellers_admins),
):
    if (err := _auth_guard(user)):
        return err

    product = (await session.execute(select(ProductDB).where(ProductDB.id == id))).scalar_one_or_none()
    if not product:
        return _not_found()
    if user["role"] == "SELLER" and product.seller_id != uuid.UUID(user["sub"]):
        return _forbidden()

    product.name        = body.name
    product.description = body.description
    product.price       = body.price
    product.stock       = body.stock
    product.category    = body.category
    product.status      = body.status.value

    await session.commit()
    await session.refresh(product)
    return product


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def archive_product(
    id:      uuid.UUID,
    session: AsyncSession = Depends(get_async_session),
    user:    dict         = Depends(allow_sellers_admins),
):
    """Soft-delete: transitions status to ARCHIVED, row is preserved."""
    if (err := _auth_guard(user)):
        return err

    product = (await session.execute(select(ProductDB).where(ProductDB.id == id))).scalar_one_or_none()
    if not product:
        return _not_found()
    if user["role"] == "SELLER" and product.seller_id != uuid.UUID(user["sub"]):
        return _forbidden()

    product.status = ProductStatus.ARCHIVED.value
    await session.commit()
