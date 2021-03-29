from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from models import *
from utils import DatabaseConnector
from sqlite3 import IntegrityError

app = FastAPI()
db = DatabaseConnector()


@app.post('/couriers', status_code=201, response_model=CouriersOutput)
async def create_couriers(payload: CouriersInput):
    await db.insert_couriers(payload.data)
    response = {'couriers': [{'id': courier.courier_id} for courier in payload.data]}
    return response


@app.patch('/couriers/{courier_id}', status_code=200, response_model=Courier)
async def patch_courier(courier_id: int, payload: PatchCourier):
    await db.patch_courier(courier_id, payload.dict(exclude_unset=True))
    response = await db.get_courier_data(courier_id)
    return response


@app.post('/orders', status_code=201, response_model=OrdersOutput)
async def create_orders(payload: OrdersInput):
    await db.insert_orders(payload.data)
    response = {'orders': [{'id': order.order_id} for order in payload.data]}
    return response


@app.post('/orders/assign', status_code=200, response_model=AssignOrdersOutput, response_model_exclude_unset=True)
async def assign_orders(payload: AssignOrdersInput):
    orders, assign_time = await db.assign_orders_to_courier(payload.courier_id)
    orders = [{'id': order_id} for order_id in orders]
    response = {'orders': orders}
    if assign_time:
        response['assign_time'] = assign_time
    return response


@app.post('/orders/complete', status_code=200, response_model=OrderCompleteOutput)
async def complete_order(payload: OrderCompleteInput):
    order_id = await db.complete_order(payload)
    return {"order_id": order_id}


@app.get('/couriers/{courier_id}', status_code=200, response_model=CourierInfo, response_model_exclude_unset=True)
async def get_courier_info(courier_id):
    return await db.calculate_couriers_rating(courier_id)

# Exception handlers


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    error_message = {}
    request_path = request['path'][1:]
    error_message['status_code'] = 400
    if request_path == 'couriers' or request_path == 'orders':
        error_message_content = {'validation_error': {request_path: []}}
        for error in exc.errors():
            incorrect_id = \
                {'id': exc.body['data'][error['loc'][2]]['courier_id' if request_path == 'couriers' else 'order_id']}
            if incorrect_id not in error_message_content['validation_error'][request_path]:
                error_message_content['validation_error'][request_path].append(incorrect_id)
            error_message_content['message'] = list(exc.errors())
        error_message['content'] = jsonable_encoder(error_message_content)
    else:
        error_message['content'] = jsonable_encoder({"detail": exc.errors()})
    return JSONResponse(**error_message)


@app.exception_handler(TypeError)
async def value_error_handler(request: Request, exc: TypeError):
    print(f"{request['path']} : {exc}")
    error_message = {'status_code': 400, 'content': jsonable_encoder({'messages': list(exc.args)})}
    return JSONResponse(**error_message)


@app.exception_handler(IntegrityError)
async def integrity_error_handler(request: Request, exc: IntegrityError):
    print(f"{request['path']} : {exc}")
    error_message = {'status_code': 400, 'content': jsonable_encoder({'messages': list(exc.args)})}
    return JSONResponse(**error_message)


@app.exception_handler(Exception)
async def free_mutex_if_unhandled_error(request: Request, exc: Exception):
    print(f"Unhandled Exception at {request['path']} :  {exc}")
    db.mutex = False
    error_message = {'status_code': 500, 'content': jsonable_encoder({'messages': 'Internal error'})}
    return JSONResponse(**error_message)
