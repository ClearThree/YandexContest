from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, validator


class Courier(BaseModel):
    class Config:
        extra = 'forbid'
        validate_all = True

    courier_id: int
    courier_type: str
    regions: List[int]
    working_hours: List[str]

    @validator('courier_type')
    def check_courier_type(cls, courier_type):
        if courier_type not in ['foot', 'bike', 'car']:
            raise ValueError("Type of courier not understood. Options are: 'foot', 'bike', 'car'")
        return courier_type

    @validator('working_hours')
    def time_format_correctness(cls, working_hours):
        if any(len(time_str) != 11 for time_str in working_hours):
            raise ValueError('Time must be in HH:MM-HH:MM format.')
        return working_hours


class CouriersInput(BaseModel):
    class Config:
        extra = 'forbid'

    data: List[Courier]


class CourierOrderOutput(BaseModel):
    id: int


class CouriersOutput(BaseModel):
    couriers: List[CourierOrderOutput]


class PatchCourier(BaseModel):
    class Config:
        extra = 'forbid'

    courier_type: Optional[str] = None
    regions: Optional[List[int]] = None
    working_hours: Optional[List[str]] = None


class Order(BaseModel):
    class Config:
        extra = 'forbid'

    order_id: int
    weight: float
    region: int
    delivery_hours: List[str]

    @validator('weight')
    def weight_correctness(cls, weight):
        if not 0.01 <= weight <= 50:
            raise ValueError('Weight must be between 0.01 and 50.')
        return weight

    @validator('delivery_hours')
    def time_format_correctness(cls, delivery_hours):
        if any(len(time_str) != 11 for time_str in delivery_hours):
            raise ValueError('Time must be in HH:MM-HH:MM format.')
        return delivery_hours


class OrdersInput(BaseModel):
    class Config:
        extra = 'forbid'

    data: List[Order]


class OrdersOutput(BaseModel):
    orders: List[CourierOrderOutput]


class AssignOrdersInput(BaseModel):
    courier_id: int


class AssignOrdersOutput(BaseModel):
    orders: List[CourierOrderOutput]
    assign_time: Optional[str] = None


class OrderCompleteInput(BaseModel):
    courier_id: int
    order_id: int
    complete_time: str

    @validator('complete_time')
    def time_format_correctness(cls, complete_time):
        try:
            temp = datetime.strptime(complete_time, "%Y-%m-%dT%H:%M:%S.%fZ")
            return f'{datetime.strftime(temp, "%Y-%m-%dT%H:%M:%S.%f")[:-3]}Z'
        except ValueError:
            try:
                temp = datetime.strptime(complete_time, "%Y-%m-%dT%H:%M:%S.%f%z")
                return f'{datetime.strftime(temp, "%Y-%m-%dT%H:%M:%S.%f")[:-3]}Z'
            except ValueError:
                raise ValueError('Time must be in ISO format. Zulu time marker or tz offset is obligatory.')


class OrderCompleteOutput(BaseModel):
    order_id: int


class CourierInfo(BaseModel):
    courier_id: int
    courier_type: str
    regions: List[int]
    working_hours: List[str]
    rating: Optional[float] = None
    earnings: int
