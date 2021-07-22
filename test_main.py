import datetime
import os

from fastapi.testclient import TestClient

from main import app
from utils import DATETIME_FORMAT

client = TestClient(app)


def test_add_couriers_correct():
    json_couriers = \
        {
            "data": [
                {
                    "courier_id": 1,
                    "courier_type": "foot",
                    "regions": [1, 12, 22],
                    "working_hours": ["11:35-14:05", "09:00-11:00"]
                },
                {
                    "courier_id": 2,
                    "courier_type": "bike",
                    "regions": [22],
                    "working_hours": ["09:00-18:00"],
                },
                {
                    "courier_id": 3,
                    "courier_type": "car",
                    "regions": [12, 22, 23, 33],
                    "working_hours": []
                },

            ]
        }
    response = client.post('/couriers', json=json_couriers)
    assert response.status_code == 201
    assert response.json() == {'couriers': [{'id': 1}, {'id': 2}, {'id': 3}]}


def test_add_couriers_incorrect():
    json_couriers = \
        {
            "data": [
                {
                    "courier_id": 1,
                    "courier_type": "foot",
                    "regions": ['foot'],
                    "working_hours": ["11:35-14:05", "09:00-11:00"]
                },
                {
                    "courier_id": 2,
                    "courier_type": "bike",
                    "regions": [22],
                    "working_hours": ["09:00:30-18:00"],
                },
                {
                    "courier_id": 3,
                    "courier_type": "motorcycle",
                    "regions": [12, 22, 23, 33],
                    "working_hours": []
                },

            ]
        }
    response = client.post('/couriers', json=json_couriers)
    assert response.status_code == 400
    assert response.json()['validation_error'] == {'couriers': [{'id': 1}, {'id': 2}, {'id': 3}]}
    assert response.json()['message'] == [{'loc': ['body', 'data', 0, 'regions', 0],
                                           'msg': 'value is not a valid integer',
                                           'type': 'type_error.integer'},
                                          {'loc': ['body', 'data', 1, 'working_hours'],
                                           'msg': 'Time must be in HH:MM-HH:MM format.',
                                           'type': 'value_error'},
                                          {'loc': ['body', 'data', 2, 'courier_type'],
                                           'msg': "Type of courier not understood. Options are: 'foot', 'bike', 'car'",
                                           'type': 'value_error'}]


def test_add_orders_correct():
    json_orders = \
        {
            "data": [
                {
                    "order_id": 1,
                    "weight": 0.23,
                    "region": 12,
                    "delivery_hours": ["09:00-18:00"]
                },
                {
                    "order_id": 2,
                    "weight": 15,
                    "region": 1,
                    "delivery_hours": ["09:00-18:00"]
                },
                {
                    "order_id": 3,
                    "weight": 0.01,
                    "region": 22,
                    "delivery_hours": ["09:00-12:00", "16:00-21:30"]
                },
                {
                    "order_id": 4,
                    "weight": 0.01,
                    "region": 22,
                    "delivery_hours": ["09:00-12:00", "16:00-21:30"]
                },
                {
                    "order_id": 5,
                    "weight": 16,
                    "region": 22,
                    "delivery_hours": ["09:00-12:00", "16:00-21:30"]
                },
                {
                    "order_id": 6,
                    "weight": 14.8,
                    "region": 22,
                    "delivery_hours": ["09:00-12:00", "16:00-21:30"]
                },
            ]
        }
    response = client.post('/orders', json=json_orders)
    assert response.status_code == 201
    assert response.json() == {'orders': [{'id': 1}, {'id': 2}, {'id': 3}, {'id': 4}, {'id': 5}, {'id': 6}]}


def test_add_orders_incorrect():
    json_orders = \
        {
            "data": [
                {
                    "order_id": 1,
                    "weight": 56,
                    "region": 12,
                    "delivery_hours": ["09:00-18:00"]
                },
                {
                    "order_id": 2,
                    "weight": 0.001,
                    "region": 1,
                    "delivery_hours": []
                },
                {
                    "order_id": 3,
                    "weight": 0.01,
                    "region": 'region',
                    "delivery_hours": ["09:00-12:00", "16:00-21:30"]
                },
                {
                    "order_id": 4,
                    "weight": 0.01,
                    "region": 22,
                    "delivery_hours": ["09:00-12:00", "16:00-21:30"]
                },
                {
                    "order_id": 5,
                    "weight": 16,
                    "region": 22,
                    "delivery_hours": ["09:00-12:00", "16:00-21:30"]
                },
                {
                    "order_id": 5,
                    "weight": 14.8,
                    "region": 22,
                    "delivery_hours": ["09:00-12:00", "16:00-21:30"]
                },
            ]
        }
    response = client.post('/orders', json=json_orders)
    assert response.status_code == 400
    assert response.json()['validation_error'] == {'orders': [{'id': 1}, {'id': 2}, {'id': 3}]}


def test_assign_orders():
    json_assign = \
        {
            "courier_id": 2
        }
    time = datetime.datetime.utcnow()
    response = client.post('/orders/assign', json=json_assign)
    resp_time = datetime.datetime.strptime(response.json()['assign_time'], DATETIME_FORMAT)
    assert response.status_code == 200
    assert response.json()['orders'] == [{'id': 3}, {'id': 4}, {'id': 6}]
    assert resp_time - time <= datetime.timedelta(seconds=5)
    json_assign = \
        {
            "courier_id": 1
        }
    time = datetime.datetime.utcnow()
    response = client.post('/orders/assign', json=json_assign)
    resp_time = datetime.datetime.strptime(response.json()['assign_time'], DATETIME_FORMAT)
    assert response.status_code == 200
    assert response.json()['orders'] == [{'id': 1}]
    assert resp_time - time <= datetime.timedelta(seconds=5)
    json_assign = \
        {
            "courier_id": 3
        }
    response = client.post('/orders/assign', json=json_assign)
    assert response.status_code == 200
    assert response.json() == {'orders': []}


def test_assign_orders_incorrect():
    json_assign = \
        {
            "courier_id": 1337
        }
    response = client.post('/orders/assign', json=json_assign)
    assert response.status_code == 400
    assert response.json() == {'messages': ['Courier with courier_id = 1337 is not found.']}


def test_patch_courier():
    json_patch = \
        {
            "working_hours": ['19:00-20:00']
        }
    response = client.patch('/couriers/3', json=json_patch)
    assert response.status_code == 200
    assert response.json() == {'courier_id': 3,
                               'courier_type': 'car',
                               'regions': [12, 22, 23, 33],
                               'working_hours': ['19:00-20:00']}
    json_patch = \
        {
            "courier_type": 'car',
            "regions": [12, 22]
        }
    response = client.patch('/couriers/2', json=json_patch)
    assert response.status_code == 200
    assert response.json() == {'courier_id': 2,
                               'courier_type': 'car',
                               'regions': [12, 22],
                               'working_hours': ['09:00-18:00']}


def test_patch_courier_incorrect():
    json_patch = \
        {
            "courier_type": 'bike',
            "clearthree": 1337
        }
    response = client.patch('/couriers/2', json=json_patch)
    assert response.status_code == 400
    assert response.json() == {'detail': [{'loc': ['body', 'clearthree'],
                                           'msg': 'extra fields not permitted',
                                           'type': 'value_error.extra'}]}

    # Check if id of non-existing courier passed.
    json_patch = \
        {
            "courier_type": 'bike',
        }
    response = client.patch('/couriers/1337', json=json_patch)
    assert response.status_code == 400
    assert response.json() == {'messages': ['Courier with courier_id = 1337 is not found.']}


def test_complete_order():
    json_complete = \
        {
            "courier_id": 2,
            "order_id": 6,
            "complete_time": datetime.datetime.utcnow().isoformat()[:-3] + 'Z'
        }
    response = client.post('/orders/complete', json=json_complete)
    assert response.status_code == 200
    assert response.json() == {'order_id': 6}


def test_complete_order_incorrect():
    json_complete = \
        {
            "courier_id": 1337,
            "order_id": 6,
            "complete_time": datetime.datetime.utcnow().isoformat()[:-3] + 'Z'
        }
    response = client.post('/orders/complete', json=json_complete)
    assert response.status_code == 400
    assert response.json() == {'messages': ['Order with id 6 was not assigned to courier with id 1337']}
    json_complete = \
        {
            "courier_id": 2,
            "order_id": 1337,
            "complete_time": datetime.datetime.utcnow().isoformat()[:-3] + 'Z'
        }
    response = client.post('/orders/complete', json=json_complete)
    assert response.status_code == 400
    assert response.json() == {'messages': ['No order with id 1337 found']}
    json_complete = \
        {
            "courier_id": 1,
            "order_id": 1,
            "complete_time": datetime.datetime.utcnow().isoformat()[:-3] + 'ZZZ'
        }
    response = client.post('/orders/complete', json=json_complete)
    assert response.status_code == 400
    assert response.json() == {
        'detail': [{'loc': ['body', 'complete_time'],
                    'msg': 'Time must be in ISO format. Zulu time marker or tz offset is obligatory.',
                    'type': 'value_error'}]}
    json_complete = \
        {
            "courier_id": 1,
            "order_id": 2,
            "complete_time": datetime.datetime.utcnow().isoformat()[:-3] + 'Z'
        }
    response = client.post('/orders/complete', json=json_complete)
    assert response.status_code == 400
    assert response.json() == {'messages': ['Order with id 2 was not assigned yet']}


def test_courier_rating():
    response = client.get('/couriers/2')
    assert response.status_code == 200
    assert response.json() == {'courier_id': 2,
                               'courier_type': 'car',
                               'regions': [12, 22],
                               'working_hours': ['09:00-18:00'],
                               'earnings': 0}

    # Since the whole delivery was not completed at the moment of request, earnings = 0 and no rating available.
    # Let's complete the delivery for courier with id = 2
    json_complete = \
        {
            "courier_id": 2,
            "order_id": 3,
            "complete_time": datetime.datetime.utcnow().isoformat()[:-3] + 'Z'
        }
    client.post('/orders/complete', json=json_complete)
    json_complete = \
        {
            "courier_id": 2,
            "order_id": 4,
            "complete_time": datetime.datetime.utcnow().isoformat()[:-3] + 'Z'
        }
    client.post('/orders/complete', json=json_complete)

    # Now his rating and earnings must be calculated.
    response = client.get('/couriers/2')
    assert response.status_code == 200
    # His earnings is 2500 (500*C, where C=5) since at the moment of delivery assignment his type was 'bike'.
    assert response.json()['earnings'] == 2500
    # His rating also should be calculated, but it depends on time when his orders were completed.
    # So let's just check if some value exists for 'rating' field. Hope I know how to calculate mean values...
    assert 'rating' in list(response.json().keys())
    assert response.json()['rating'] is not None


def test_remove_database():
    os.remove(f"{os.getcwd()}/sweetdelivery.db")
    assert not os.path.isfile(f"{os.getcwd()}/sweetdelivery.db")
