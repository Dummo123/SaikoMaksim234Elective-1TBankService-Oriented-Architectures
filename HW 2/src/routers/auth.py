import jwt
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_async_session
from src.models.db import UserDB
from src.models.generated import RefreshRequest, TokenResponse, UserLogin, UserRegister

router   = APIRouter()
_bearer  = HTTPBearer()
_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")

SECRET_KEY             = "shop_api_secret"
ALGORITHM              = "HS256"
ACCESS_TOKEN_MINUTES   = 15
REFRESH_TOKEN_DAYS     = 30


# ── shared helpers ────────────────────────────────────────────────────────────

def error_resp(code: int, err_code: str, msg: str):
    return JSONResponse(status_code=code, content={"error_code": err_code, "message": msg})


def build_token(payload: dict, ttl: timedelta) -> str:
    data = payload.copy()
    data["exp"] = datetime.now(timezone.utc) + ttl
    return jwt.encode(data, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(
    creds: HTTPAuthorizationCredentials = Depends(_bearer),
) -> dict | None:
    try:
        return jwt.decode(creds.credentials, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


class RoleChecker:
    """Dependency that validates the JWT and enforces role access.

    Returns the user payload dict on success, None if unauthenticated,
    False if the role is not permitted — callers must check the return value.
    """

    def __init__(self, permitted_roles: list[str]):
        self.permitted_roles = permitted_roles

    def __call__(self, user: dict | None = Depends(get_current_user)):
        if user is None:
            return None
        if user.get("role") not in self.permitted_roles:
            return False
        return user


# ── endpoints ─────────────────────────────────────────────────────────────────

@router.post("/register", status_code=201)
async def register(
    body:    UserRegister,
    session: AsyncSession = Depends(get_async_session),
):
    taken = (
        await session.execute(select(UserDB).where(UserDB.username == body.username))
    ).scalars().first()

    if taken:
        return error_resp(409, "USERNAME_TAKEN", "That username is already registered")

    session.add(UserDB(
        username=body.username,
        password_hash=_pwd_ctx.hash(body.password),
        role=body.role.value,
    ))
    await session.commit()
    return {"status": "registered"}


@router.post("/login", response_model=TokenResponse)
async def login(
    body:    UserLogin,
    session: AsyncSession = Depends(get_async_session),
):
    user = (
        await session.execute(select(UserDB).where(UserDB.username == body.username))
    ).scalars().first()

    if not user or not _pwd_ctx.verify(body.password, user.password_hash):
        return error_resp(401, "BAD_CREDENTIALS", "Wrong username or password")

    access  = build_token({"sub": str(user.id), "role": user.role, "kind": "access"},  timedelta(minutes=ACCESS_TOKEN_MINUTES))
    refresh = build_token({"sub": str(user.id), "kind": "refresh"}, timedelta(days=REFRESH_TOKEN_DAYS))
    return TokenResponse(access_token=access, refresh_token=refresh)


@router.post("/refresh", response_model=TokenResponse)
async def refresh_tokens(
    body:    RefreshRequest,
    session: AsyncSession = Depends(get_async_session),
):
    try:
        payload = jwt.decode(body.refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
    except Exception:
        return error_resp(401, "REFRESH_TOKEN_INVALID", "Token is invalid or expired")

    if payload.get("kind") != "refresh":
        return error_resp(401, "WRONG_TOKEN_TYPE", "Expected a refresh token")

    user = (
        await session.execute(select(UserDB).where(UserDB.id == payload.get("sub")))
    ).scalars().first()

    if not user:
        return error_resp(401, "USER_NOT_FOUND", "Token subject does not exist")

    access  = build_token({"sub": str(user.id), "role": user.role, "kind": "access"},  timedelta(minutes=ACCESS_TOKEN_MINUTES))
    refresh = build_token({"sub": str(user.id), "kind": "refresh"}, timedelta(days=REFRESH_TOKEN_DAYS))
    return TokenResponse(access_token=access, refresh_token=refresh)
