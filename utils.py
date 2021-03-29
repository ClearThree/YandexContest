import asyncio
import sqlite3
import time
from models import *
from itertools import chain

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def unpack_list(nested_list):
    return tuple(chain.from_iterable(nested_list))


def unpack_list_to_list(nested_list):
    return list(chain.from_iterable(nested_list))


def unpack_delivery_hours(nested_list):
    result_dict = {}
    for item in nested_list:
        order_id, delivery_hours = item
        if order_id not in list(result_dict.keys()):
            result_dict[order_id] = [delivery_hours]
        else:
            result_dict[order_id].append(delivery_hours)
    return result_dict


def unpack_orders(orders_q):
    return {order[0]: order[1] for order in orders_q}


def unpack_completed_orders(completed_orders):
    return [
        {'order_id': order[0],
         'region': order[1],
         'date_assigned': datetime.strptime(order[2], DATETIME_FORMAT),
         'date_finished': datetime.strptime(order[3], DATETIME_FORMAT) if order[3] else None,
         'type_when_assigned': order[4]
         } for order in completed_orders
    ]


def hours_intersect(working_h: List[str], delivery_h: List[str]) -> bool:
    for working_h_step in working_h:
        working_h_start, working_h_end = transform_to_dt(working_h_step)
        for delivery_h_step in delivery_h:
            delivery_h_start, delivery_h_end = transform_to_dt(delivery_h_step)
            if (working_h_start <= delivery_h_start) and (working_h_end >= delivery_h_end):
                return True
    return False


def transform_to_dt(bounds):
    bounds_split = bounds.split('-')
    bounds_start = time.strptime(bounds_split[0], '%H:%M')
    bounds_end = time.strptime(bounds_split[1], '%H:%M')
    return bounds_start, bounds_end


class DatabaseConnector:
    def __init__(self):
        self.conn = sqlite3.connect('sweetdelivery.db')
        self.cursor = self.conn.cursor()
        self.mutex = False
        tables = self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        if len(tables) == 0:
            print("No tables found, creating.")
            self.create_tables()
        self.couriers_load = {'foot': 10, 'bike': 15, 'car': 50}
        self.coefficient = {'foot': 2, 'bike': 5, 'car': 9}

    def create_tables(self):
        self.cursor.execute(
            "CREATE TABLE couriers (id INTEGER PRIMARY KEY, type VARCHAR(5));"),
        self.cursor.execute(
            "CREATE TABLE regions (region_id INTEGER, courier_id INTEGER);"),
        self.cursor.execute(
            "CREATE TABLE working_hours (courier_id INTEGER, working_hours VARCHAR(20));")
        self.cursor.execute(
            "CREATE TABLE delivery_hours (order_id INTEGER, delivery_hours VARCHAR(20));")
        self.cursor.execute("""
              CREATE TABLE orders (order_id INTEGER PRIMARY KEY, 
                                   weight FLOAT, 
                                   region INTEGER, 
                                   status INTEGER,
                                   date_created DATETIME,
                                   date_assigned DATETIME,
                                   date_finished DATETIME,
                                   courier_id INTEGER,
                                   type_when_assigned VARCHAR(5));
              """)
        self.conn.commit()

    async def insert_couriers(self, couriers: List[Courier]):
        self.mutex = True
        for courier in couriers:
            self.cursor.execute(
                f"INSERT INTO couriers(id, type) VALUES ({courier.courier_id}, '{courier.courier_type}');")
            for region in courier.regions:
                self.cursor.execute(
                    f"INSERT INTO regions(region_id, courier_id) VALUES ({region}, {courier.courier_id});")
            for working_hours_ in courier.working_hours:
                self.cursor.execute(
                    "INSERT INTO working_hours(courier_id, working_hours) "
                    f"VALUES ({courier.courier_id}, '{working_hours_}');")
        self.mutex = False
        self.conn.commit()

    async def insert_orders(self, orders: List[Order]):
        self.mutex = True
        for order in orders:
            # in orders table 'status' column has three possible values:
            # 0 - order is not assigned, 1 - order is assigned, 2 - order is completed
            try:
                self.cursor.execute(
                    "INSERT INTO orders(order_id, weight, region, status, date_created) "
                    f"VALUES ({order.order_id},{order.weight}, {order.region}, "
                    f"0, '{datetime.utcnow().isoformat()[:-3] + 'Z'}');")
            except sqlite3.IntegrityError:
                self.mutex = False
                raise sqlite3.IntegrityError(f'Order with id = {order.order_id} already exists')
            for delivery_hours_ in order.delivery_hours:
                self.cursor.execute(
                    "INSERT INTO delivery_hours(order_id, delivery_hours) "
                    f"VALUES ({order.order_id}, '{delivery_hours_}');")
        self.mutex = False
        self.conn.commit()

    async def assign_orders_to_courier(self, courier_id):
        courier_type, courier_max_load, courier_regions, courier_working_hours, courier_current_orders = \
            await self.get_actual_courier_status(courier_id)
        courier_rest_load = courier_max_load - sum(courier_current_orders.values())
        regions_tuple = str(courier_regions)[:-2] + ')' if len(courier_regions) == 1 else str(courier_regions)
        possible_orders = unpack_orders(self.cursor.execute(
            "SELECT order_id, weight FROM orders "
            "WHERE status = 0 "
            f"AND region IN {regions_tuple} ").fetchall())  # possible_orders = {id: weight}
        if len(possible_orders) == 0:
            return [], None
        order_ids_tuple = tuple(possible_orders.keys())
        order_ids_tuple = str(order_ids_tuple)[:-2] + ')' if len(order_ids_tuple) == 1 else str(order_ids_tuple)

        delivery_time = unpack_delivery_hours(
            self.cursor.execute("SELECT order_id, delivery_hours FROM delivery_hours "
                                f"WHERE order_id IN {order_ids_tuple} "
                                ).fetchall())
        possible_orders_timefiltered = {}
        for possible_order in possible_orders:
            if hours_intersect(courier_working_hours, delivery_time[possible_order]):
                possible_orders_timefiltered[possible_order] = possible_orders[possible_order]

        # выбрать по подходящему весу, назначить куре
        if len(possible_orders_timefiltered) == 0:
            return [], None
        # Sort orders by weight to give courier the maximum number of orders
        possible_orders_timefiltered = dict(sorted(possible_orders_timefiltered.items(), key=lambda item: item[1]))
        valid_orders = []
        for order in possible_orders_timefiltered:
            delta = courier_rest_load - possible_orders_timefiltered[order]
            if delta < 0:
                break
            else:
                valid_orders.append(order)
                courier_rest_load = delta
        dt = None
        if not valid_orders:
            return [], dt
        self.mutex = True
        dt = datetime.utcnow().isoformat()[:-3] + 'Z'
        for valid_order in valid_orders:
            self.cursor.execute(f"UPDATE orders SET status = 1, date_assigned = '{dt}', courier_id = {courier_id}, "
                                f"type_when_assigned = '{courier_type}' "
                                f"WHERE order_id = {valid_order}")
        self.conn.commit()
        self.mutex = False
        if len(courier_current_orders):
            valid_orders = list(courier_current_orders.keys()) + valid_orders
            dt = self.cursor.execute("SELECT min(date_assigned) FROM orders "
                                     f"WHERE courier_id = {courier_id} AND status = 1 ").fetchone()[0]
        return valid_orders, dt

    async def patch_courier(self, courier_id, patch):
        patch_keys = list(patch.keys())
        while self.mutex:
            await asyncio.sleep(0.1)
        self.mutex = True
        if 'courier_type' in patch_keys:
            self.cursor.execute(f"UPDATE couriers SET type = '{patch['courier_type']}' "
                                f"WHERE id = {courier_id}")
        if 'regions' in patch_keys:
            self.cursor.execute(f"DELETE FROM regions WHERE courier_id = {courier_id}")
            for region in patch['regions']:
                self.cursor.execute(
                    f"INSERT INTO regions(region_id, courier_id) VALUES ({region}, {courier_id});")
        if 'working_hours' in patch_keys:
            self.cursor.execute(f"DELETE FROM working_hours WHERE courier_id = {courier_id}")
            for working_hours_ in patch['working_hours']:
                self.cursor.execute(
                    "INSERT INTO working_hours(courier_id, working_hours) "
                    f"VALUES ({courier_id}, '{working_hours_}');")
        self.conn.commit()
        self.mutex = False
        await self.validate_existing_orders(courier_id, patch_keys)

    async def validate_existing_orders(self, courier_id, changed_fields):
        courier_type, courier_max_load, courier_regions, courier_working_hours, courier_current_orders = \
            await self.get_actual_courier_status(courier_id)
        if 'regions' in changed_fields:
            order_ids_tuple = tuple(courier_current_orders.keys())
            order_ids_tuple = str(order_ids_tuple)[:-2] + ')' if len(order_ids_tuple) == 1 else str(order_ids_tuple)
            regions = unpack_list_to_list(self.cursor.execute("SELECT region FROM orders "
                                                              f"WHERE order_id IN {order_ids_tuple} ").fetchall())
            invalid_regions = set(regions) - set(courier_regions)
            if len(invalid_regions) != 0:
                invalid_regions = tuple(invalid_regions)
                invalid_regions = str(invalid_regions)[:-2] + ')' if len(invalid_regions) == 1 else str(invalid_regions)
                invalid_ids = unpack_list_to_list(self.cursor.execute("SELECT order_id FROM orders "
                                                                      f"WHERE region IN {invalid_regions} "
                                                                      f"AND courier_id = {courier_id}").fetchall())
                self.mutex = True
                for invalid_id in invalid_ids:
                    courier_current_orders.pop(invalid_id)
                    self.cursor.execute(
                        f"UPDATE orders SET status = 0, date_assigned = null, courier_id = null, "
                        f"type_when_assigned = null "
                        f"WHERE order_id = {invalid_id}")
                self.conn.commit()
                self.mutex = False
        if ('working_hours' in changed_fields) and (len(courier_current_orders) > 0):
            order_ids_tuple = tuple(courier_current_orders.keys())
            order_ids_tuple = str(order_ids_tuple)[:-2] + ')' if len(order_ids_tuple) == 1 else str(order_ids_tuple)
            delivery_time = unpack_delivery_hours(
                self.cursor.execute("SELECT order_id, delivery_hours FROM delivery_hours "
                                    f"WHERE order_id IN {order_ids_tuple} "
                                    ).fetchall())
            invalid_orders = []
            for order in courier_current_orders:
                print(hours_intersect(courier_working_hours, delivery_time[order]))
                if not hours_intersect(courier_working_hours, delivery_time[order]):
                    invalid_orders.append(order)
            self.mutex = True
            for invalid_id in invalid_orders:
                courier_current_orders.pop(invalid_id)
                self.cursor.execute(
                    f"UPDATE orders SET status = 0, date_assigned = null, courier_id = null, "
                    f"type_when_assigned = null "
                    f"WHERE order_id = {invalid_id}")
            self.conn.commit()
            self.mutex = False

        if 'courier_type' in changed_fields and (len(courier_current_orders) > 0):
            courier_rest_load = courier_max_load - sum(courier_current_orders.values())
            if courier_rest_load < 0:
                dropped_orders = []
                courier_current_orders = dict(
                    reversed(sorted(courier_current_orders.items(), key=lambda item: item[1])))
                for order_id, order_weight in courier_current_orders.items():
                    delta = courier_rest_load + order_weight
                    dropped_orders.append(order_id)
                    if delta >= 0:
                        break
                for dropped_id in dropped_orders:
                    self.cursor.execute(
                        f"UPDATE orders SET status = 0, date_assigned = null, courier_id = null, "
                        f"type_when_assigned = null "
                        f"WHERE order_id = {dropped_id}")
                self.conn.commit()
                self.mutex = False

    async def get_actual_courier_status(self, courier_id: int):
        while self.mutex:
            await asyncio.sleep(0.1)
        try:
            courier_type = self.cursor.execute(f"SELECT type FROM couriers WHERE id = {courier_id}").fetchone()[0]
        except TypeError:
            raise TypeError(f'Courier with courier_id = {courier_id} is not found.')
        courier_max_load = self.couriers_load[courier_type]
        courier_regions = unpack_list(self.cursor.execute("SELECT region_id FROM regions "
                                                          f"WHERE courier_id = {courier_id}").fetchall())
        courier_working_hours = unpack_list_to_list(self.cursor.execute("SELECT working_hours FROM working_hours "
                                                                        f"WHERE courier_id = {courier_id}").fetchall())
        courier_current_orders = unpack_orders(self.cursor.execute("SELECT order_id, weight FROM orders "
                                                                   f"WHERE courier_id = {courier_id} "
                                                                   "AND status = 1").fetchall())
        return courier_type, courier_max_load, courier_regions, courier_working_hours, courier_current_orders

    async def get_courier_data(self, courier_id):
        while self.mutex:
            await asyncio.sleep(0.1)
        try:
            courier_type = self.cursor.execute(f"SELECT type FROM couriers WHERE id = {courier_id}").fetchone()[0]
        except TypeError:
            raise TypeError(f'Courier with courier_id = {courier_id} was not found.')
        courier_regions = unpack_list_to_list(self.cursor.execute("SELECT region_id FROM regions "
                                                                  f"WHERE courier_id = {courier_id}").fetchall())
        courier_working_hours = unpack_list_to_list(self.cursor.execute("SELECT working_hours FROM working_hours "
                                                                        f"WHERE courier_id = {courier_id}").fetchall())
        return {'courier_id': courier_id,
                'courier_type': courier_type,
                'regions': courier_regions,
                'working_hours': courier_working_hours}

    async def complete_order(self, completed_order: OrderCompleteInput):
        while self.mutex:
            await asyncio.sleep(0.1)
        try:
            order = self.cursor.execute("SELECT order_id, status, courier_id FROM orders "
                                        f"WHERE order_id = {completed_order.order_id} ").fetchone()
        except TypeError:
            raise TypeError(f'No order with id {completed_order.order_id} found')
        if order[1] == 0:
            raise TypeError(f'Order with id {completed_order.order_id} was not assigned yet')
        elif order[2] != completed_order.courier_id:
            raise TypeError(f'Order with id {completed_order.order_id} was not assigned to courier with '
                            f'id {completed_order.courier_id}')
        elif order[1] == 2:
            raise TypeError(f'Order with id {completed_order.order_id} was already completed.')
            # return completed_order.order_id  # Don't know if we should return 400 with the message that the order
            # # was already completed, or return 200 OK with id ...
        else:
            self.mutex = True
            self.cursor.execute(f"UPDATE orders SET status = 2, date_finished = '{completed_order.complete_time}' "
                                f"WHERE order_id = {completed_order.order_id}")
            self.conn.commit()
            self.mutex = False
            return completed_order.order_id

    async def calculate_couriers_rating(self, courier_id):
        courier_data = await self.get_courier_data(courier_id)
        earnings = 0
        courier_data["earnings"] = earnings
        completed_orders = unpack_completed_orders(self.cursor.execute(
            "SELECT order_id, region, date_assigned, date_finished, type_when_assigned "
            f"FROM orders WHERE courier_id = {courier_id} AND status = 2 "
            "ORDER BY date_finished DESC ").fetchall())

        # Deliveries calculation

        assigned_orders = unpack_completed_orders(self.cursor.execute(
            "SELECT order_id, region, date_assigned, date_finished, type_when_assigned "
            f"FROM orders WHERE courier_id = {courier_id} AND status != 0 "
            "ORDER BY date_assigned DESC ").fetchall())

        all_assign_dates = []
        for order in assigned_orders:
            all_assign_dates.append(order['date_assigned'])

        deliveries_types = []
        for assign_date in set({order['date_assigned'] for order in assigned_orders}):
            temp = []
            for order in assigned_orders:
                if order['date_assigned'] == assign_date:
                    temp.append(order)
            if all(order['date_finished'] for order in temp):
                deliveries_types.append(temp[0]['type_when_assigned'])
        if not len(deliveries_types):
            return courier_data
        for delivery_type in deliveries_types:
            earnings += self.coefficient[delivery_type] * 500
        courier_data["earnings"] = earnings

        # Rating calculation
        average_time = []
        for i, order in enumerate(completed_orders[:-1]):
            order['delivery_time'] = (order['date_finished'] - completed_orders[i + 1]['date_finished']).total_seconds()
        completed_orders[-1]['delivery_time'] = \
            (completed_orders[-1]['date_finished'] - completed_orders[-1]['date_assigned']).total_seconds()
        for region in courier_data['regions']:
            temp_sum = 0
            temp_number = 0
            for order in completed_orders:
                if order['region'] == region:
                    temp_sum += order['delivery_time']
                    temp_number += 1
            if temp_number != 0:
                average_time.append(temp_sum / temp_number)
        t = min(average_time)
        courier_data["rating"] = round(((60 * 60 - min(t, 60 * 60)) / (60 * 60) * 5), 2)

        return courier_data
