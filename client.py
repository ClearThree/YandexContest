from requests import post
from numpy.random import choice
from random import randint, uniform
from datetime import datetime, timedelta

courier_types = ["foot", "bike", "car"]
regions = [i for i in range(1, 11)]
possible_hours = [(datetime(year=1970, month=1, day=1, hour=00, minute=00, second=0)
                   + timedelta(minutes=30*i)) for i in range(48)]


def join_hours(start:datetime, end: datetime) -> str:
    if start < end:
        return f"{start.strftime('%H:%M')}-{end.strftime('%H:%M')}"
    else:
        return f"{end.strftime('%H:%M')}-{start.strftime('%H:%M')}"

# Fill couriers


def fill(route: str, start_id: int = 1, quantity: int = 1000, verbose: bool = False):
    data = []

    if route == 'couriers':
        for i in range(start_id, quantity):
            ch = sorted(choice(possible_hours, size=2 * randint(1, 3), replace=False))
            data.append(
                {
                    "courier_id": i,
                    "courier_type": courier_types[randint(0, 2)],
                    "regions": list(map(int, choice(regions, size=randint(1, 5), replace=False))),
                    "working_hours": list(map(join_hours, ch[::2], ch[1::2]))
                }
            )
    elif route == 'orders':
        for i in range(start_id, quantity):
            ch = sorted(choice(possible_hours, size=2 * randint(1, 3), replace=False))
            data.append(
                {
                    "order_id": i,
                    "weight": uniform(0.01, 50.0),
                    "region": int(choice(regions, size=1)),
                    "delivery_hours": list(map(join_hours, ch[::2], ch[1::2]))
                }
            )
    else:
        raise ValueError('Route is not valid. Use "couriers" or "orders"')

    json_payload = {"data": data}

    print(f"Sending the {route} post")
    re = post(f'http://localhost:8000/{route}', json=json_payload)
    print(f'Request by the /{route} route performed with {re.status_code} status code.')
    if verbose:
        print(f'Response payload: {re.json()}')


print("Setting 1000 couriers and 2000 orders.")
fill('couriers', 1, 1000)
fill('orders', 1, 2000)
print("Assigning orders.")
# Тут сделать цикл ассайнов по айдишникам, чекнуть повторяемость
re = post('http://localhost:8000/orders/assign', json={"courier_id": 1})
print(f'Request by the /assign route performed with {re.status_code} status code.')
print(f'Raw response: {re.json()}')


print("Setting 2000 couriers and 4000 orders.")
fill('couriers', 1000,  2000)
fill('orders', 2000, 4000)
print("Assigning orders.")
re = post('http://localhost:8000/orders/assign', json={"courier_id": 2})
print(f'Request by the /assign route performed with {re.status_code} status code.')
print(f'Raw response: {re.json()}')

print("Setting 3000 couriers and 6000 orders.")
fill('couriers', 3000, 3000)
fill('orders', 6000, 6000)
print("Assigning orders.")
re = post('http://localhost:8000/orders/assign', json={"courier_id": 3})
print(f'Request by the /assign route performed with {re.status_code} status code.')
print(f'Raw response: {re.json()}')

print("Setting 5000 couriers and 10000 orders.")
fill('couriers', 6000, 5000)
fill('orders', 12000, 10000)
print("Assigning orders.")
re = post('http://localhost:8000/orders/assign', json={"courier_id": 4})
print(f'Request by the /assign route performed with {re.status_code} status code.')
print(f'Raw response: {re.json()}')

print("Setting 10000 couriers and 20000 orders.")
fill('couriers', 11000, 10000)
fill('orders', 22000, 20000)
print("Assigning orders.")
re = post('http://localhost:8000/orders/assign', json={"courier_id": 5})
print(f'Request by the /assign route performed with {re.status_code} status code.')
print(f'Raw response: {re.json()}')

print("Setting 100000 couriers and 200000 orders.")
fill('couriers', 21000, 100000)
fill('orders', 42000, 200000)
print("Assigning orders.")
for i in range(6, 26):
    re = post('http://localhost:8000/orders/assign', json={"courier_id": i})
    print(f'Request by the /assign route performed with {re.status_code} status code.')
    print(f'Raw response: {re.json()}')
