import requests
import json
from config import *
import time
import ray

PAPER_URL = 'https://paper-api.alpaca.markets'
LIVE_URL = "https://api.alpaca.markets"
BASE_URL = PAPER_URL

url_suffix = {
    'ACCOUNT_URL': "{}/v2/account",
    'ORDERS_URL': "{}/v2/orders",
    'ACTIVITIES_URL': "{}/v2/account/activities",
    'CONFIGURATIONS_URL': "{}/v2/account/configurations",
    'POSITIONS_URL': "{}/v2/positions",
    'ASSETS_URL': "{}/v2/assets",
    'CALENDAR_URL': "{}/v2/calendar",
    'CLOCK_URL': "{}/v2/clock",
}


def headers_constructor(live):
    if live:
        headers = {'APCA-API-KEY-ID': LIVE_API_KEY, 'APCA-API-SECRET-KEY': LIVE_SECRET_KEY}
    else:
        headers = {'APCA-API-KEY-ID': PAPER_API_KEY, 'APCA-API-SECRET-KEY': PAPER_SECRET_KEY}
    return headers


def url_constructor(live, suffix):
    if live:
        url = url_suffix[suffix].format(LIVE_URL)
    else:
        url = url_suffix[suffix].format(PAPER_URL)
    return url


def get_account(live=False):
    url = url_constructor(live, 'ACCOUNT_URL')
    headers = headers_constructor(live)
    r = requests.get(url, headers=headers)
    return json.loads(r.content)


def get_activity(live=False):
    url = url_constructor(live, 'ACTIVITIES_URL')
    headers = headers_constructor(live)
    r = requests.get(url, headers=headers)
    return json.loads(r.content)


def get_configuration(live=False):
    url = url_constructor(live, 'CONFIGURATIONS_URL')
    headers = headers_constructor(live)
    r = requests.get(url, headers=headers)
    return json.loads(r.content)


@ray.remote
def create_order(symbol, qty, side, type, time_in_force, live=False):
    url = url_constructor(live, 'ORDERS_URL')
    headers = headers_constructor(live)
    data = dict(symbol=symbol, qty=qty, side=side, type=type, time_in_force=time_in_force)
    r = requests.post(url, json=data, headers=headers)
    return json.loads(r.content)


def get_orders(live=False):
    url = url_constructor(live, 'ORDERS_URL')
    headers = headers_constructor(live)
    r = requests.get(url, headers=headers)
    return json.loads(r.content)


def get_positions(live=False):
    url = url_constructor(live, 'POSITIONS_URL')
    headers = headers_constructor(live)
    r = requests.get(url, headers=headers)
    return json.loads(r.content)


def get_asset_taxonomy(live=False):
    url = url_constructor(live, 'ASSETS_URL')
    headers = headers_constructor(live)
    r = requests.get(url, headers=headers)
    return json.loads(r.content)


def get_market_calendar(live=False):
    url = url_constructor(live, 'CALENDAR_URL')
    headers = headers_constructor(live)
    r = requests.get(url, headers=headers)
    return json.loads(r.content)


def get_clock(live=False):
    url = url_constructor(live, 'CLOCK_URL')
    headers = headers_constructor(live)
    r = requests.get(url, headers=headers)
    return json.loads(r.content)


def delete_all_orders(live=False):
    url = url_constructor(live, 'ORDERS_URL')
    headers = headers_constructor(live)
    r = requests.delete(url, headers=headers)
    return json.loads(r.content)


@ray.remote
def delete_order(orderid, live=False):
    url = url_constructor(live, 'ORDERS_URL') + '/' + orderid
    headers = headers_constructor(live)
    r = requests.delete(url, headers=headers)
    return json.loads(r.content)


@ray.remote
def close_position(symbol, live=False):
    url = url_constructor(live, 'POSITIONS_URL') + '/' + symbol
    headers = headers_constructor(live)
    r = requests.delete(url, headers=headers)
    return json.loads(r.content)


def close_all_positions(live=False):
    url = url_constructor(live, 'POSITIONS_URL')
    headers = headers_constructor(live)
    r = requests.delete(url, headers=headers)
    return json.loads(r.content)
