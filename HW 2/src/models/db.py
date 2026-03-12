import uuid

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, Numeric, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from src.database import Base
from sqlalchemy.dialects import postgresql as pg


class UserDB(Base):
    __tablename__ = "users"

    id            = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username      = Column(String(50), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    role          = Column(pg.ENUM('ADMIN', 'SELLER', 'USER', name='user_role', create_type=False), nullable=False, default="USER")
    created_at    = Column(DateTime(timezone=True), server_default=func.now())


class ProductDB(Base):
    __tablename__ = "products"

    id          = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name        = Column(String(255), nullable=False)
    description = Column(String(4000), nullable=True)
    price       = Column(Numeric(12, 2), nullable=False)
    stock       = Column(Integer, nullable=False)
    category    = Column(String(100), nullable=False)
    status      = Column(pg.ENUM('ACTIVE', 'ARCHIVED', name='product_status', create_type=False), nullable=False)
    seller_id   = Column(UUID(as_uuid=True), nullable=True)
    created_at  = Column(DateTime(timezone=True), server_default=func.now())
    updated_at  = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class PromoCodeDB(Base):
    __tablename__ = "promo_codes"

    id               = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    code             = Column(String(20), unique=True, nullable=False)
    discount_type    = Column(pg.ENUM('PERCENTAGE', 'FIXED_AMOUNT', name='discount_type', create_type=False), nullable=False)
    discount_value   = Column(Numeric(12, 2), nullable=False)
    min_order_amount = Column(Numeric(12, 2), nullable=False, default=0)
    max_uses         = Column(Integer, nullable=False)
    current_uses     = Column(Integer, nullable=False, default=0)
    valid_from       = Column(DateTime(timezone=True), nullable=False)
    valid_until      = Column(DateTime(timezone=True), nullable=False)
    active           = Column(Boolean, nullable=False, default=True)


class OrderDB(Base):
    __tablename__ = "orders"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id         = Column(UUID(as_uuid=True), nullable=False)
    status          = Column(pg.ENUM('CREATED', 'PAYMENT_PENDING', 'PAID', 'SHIPPED', 'COMPLETED', 'CANCELED', name='order_status', create_type=False), nullable=False, default="CREATED")
    promo_code_id   = Column(UUID(as_uuid=True), ForeignKey("promo_codes.id"), nullable=True)
    total_amount    = Column(Numeric(12, 2), nullable=False)
    discount_amount = Column(Numeric(12, 2), nullable=False, default=0)
    created_at      = Column(DateTime(timezone=True), server_default=func.now())
    updated_at      = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class OrderItemDB(Base):
    __tablename__ = "order_items"

    id             = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id       = Column(UUID(as_uuid=True), ForeignKey("orders.id"), nullable=False)
    product_id     = Column(UUID(as_uuid=True), ForeignKey("products.id"), nullable=False)
    quantity       = Column(Integer, nullable=False)
    price_at_order = Column(Numeric(12, 2), nullable=False)


class UserOperationDB(Base):
    __tablename__ = "user_operations"

    id             = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id        = Column(UUID(as_uuid=True), nullable=False)
    operation_type = Column(pg.ENUM('CREATE_ORDER', 'UPDATE_ORDER', name='user_op_type', create_type=False), nullable=False)
    created_at     = Column(DateTime(timezone=True), server_default=func.now())
