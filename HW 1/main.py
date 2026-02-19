from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import uuid

app = FastAPI(title="Order Service", version="1.0.0")

# In-memory store (for demo purposes)
orders: dict = {}


class OrderRequest(BaseModel):
    user_id: str
    product_id: str
    quantity: int
    price: float


class OrderResponse(BaseModel):
    order_id: str
    user_id: str
    product_id: str
    quantity: int
    total: float
    status: str


@app.get("/health")
async def health_check():
    return {"status": "OK", "service": "Order Service"}


@app.post("/orders", response_model=OrderResponse, status_code=201)
async def create_order(order: OrderRequest):
    if order.quantity <= 0:
        raise HTTPException(status_code=400, detail="Quantity must be greater than 0")

    order_id = str(uuid.uuid4())
    new_order = {
        "order_id": order_id,
        "user_id": order.user_id,
        "product_id": order.product_id,
        "quantity": order.quantity,
        "total": round(order.quantity * order.price, 2),
        "status": "pending",
    }
    orders[order_id] = new_order
    return new_order


@app.get("/orders/{order_id}", response_model=OrderResponse)
async def get_order(order_id: str):
    if order_id not in orders:
        raise HTTPException(status_code=404, detail="Order not found")
    return orders[order_id]
