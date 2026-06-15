from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


ExchangeName = Literal["binance", "okx", "gateio"]
OrderSide = Literal["buy", "sell"]
OrderType = Literal["limit", "market"]
HedgeMode = Literal["off", "paper", "auto"]
OwnershipMode = Literal["manual_only", "unmanaged"]


class PaperOrderRequest(BaseModel):
    exchange: ExchangeName
    symbol: str = Field(min_length=1)
    side: OrderSide
    order_type: OrderType = "limit"
    quantity: float = Field(gt=0)
    price: float | None = Field(default=None, gt=0)
    reduce_only: bool = False
    hedge_mode: HedgeMode = "off"


class LiveLimitOrderRequest(BaseModel):
    exchange: ExchangeName
    symbol: str = Field(min_length=1)
    side: OrderSide
    quantity: float = Field(gt=0)
    price: float = Field(gt=0)
    reduce_only: bool = False
    hedge_mode: HedgeMode = "off"
    hedge_exchange: ExchangeName | None = None
    ownership_mode: OwnershipMode = "manual_only"
    wait_sec: float = Field(default=0.5, ge=0.0, le=10.0)


class LiveMarketOrderRequest(BaseModel):
    exchange: ExchangeName
    symbol: str = Field(min_length=1)
    side: OrderSide
    quantity: float = Field(gt=0)
    hedge_mode: HedgeMode = "off"
    hedge_exchange: ExchangeName | None = None
    ownership_mode: OwnershipMode = "manual_only"
    wait_sec: float = Field(default=0.5, ge=0.0, le=10.0)


class CancelSymbolRequest(BaseModel):
    exchange: ExchangeName
    symbol: str = Field(min_length=1)
    wait_sec: float = Field(default=0.5, ge=0.0, le=10.0)


class FlattenSymbolRequest(BaseModel):
    symbol: str = Field(min_length=1)
    exchange: ExchangeName | None = None
    confirm: bool = False
    wait_sec: float = Field(default=0.8, ge=0.0, le=10.0)


class RebalanceHedgeRequest(BaseModel):
    symbol: str = Field(min_length=1)
    confirm: bool = False
    wait_sec: float = Field(default=0.8, ge=0.0, le=10.0)


class EntryPauseRequest(BaseModel):
    reason: str = "manual_ops_console"


class EntryResumeRequest(BaseModel):
    reason: str = "manual_ops_console_resume"
