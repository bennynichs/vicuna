from alpaca import *
import time
from pymongo import MongoClient
import schedule

BUY = True
COUNT = 0

def get_equities_from_file():
    """
    Get the equities from the equities JSON config
    Return a list fo equities to transact
    """
    equities_doc = open('equities.json')
    with equities_doc as e:
        equities = json.loads(e.read())
    return equities['equities']


def to_mongo(orders_list:list, db_collection, live=False):
    """
    Sends a list of dictionaries/JSON to the appropriate MongoDB database.
    """
    client = MongoClient('localhost', 27017)
    if live: db = client.live
    else: db = client.papertrade

    collections = {'orders': db.orders,
                   'positions': db.positions}

    collection = collections[db_collection]
    return collection.insert_many(orders_list)


def bulk_order(equity_list):
    """
    bulk execute either:
    - buying single quantities of equities from a list
    - closing all positions of an equity
    the order operation executes over the Ray multiprocessing API, presumes ray.init
    """
    global BUY
    if BUY:
        print('buying....')
        bulk_orders = [create_order.remote(eq, 1, 'buy', 'market', 'gtc') for eq in equity_list]
        print('...done buying')
        BUY = not BUY
        orders = [ray.get(order) for order in bulk_orders]

    else:
        print('selling....')
        orders = close_all_positions()
        print('...done selling')
        BUY = not BUY

    # dump the results to mongo db
    if len(orders) > 0:
        to_mongo(orders, 'orders')
    # the bulk order will alternate the execution buy/sell

    global COUNT
    COUNT += 1
    print('Order #: '+str(COUNT))
    return


if __name__ == '__main__':
    """
    """
    # load the equities JSON document and return a list
    equities = get_equities_from_file()[:50]
    ray.init()

    schedule.every(1).minutes.do(bulk_order, equities)

    while time.localtime().tm_hour < 15 and time.localtime().tm_min < 50:
        schedule.run_pending()
        time.sleep(1)

    BUY = False
    bulk_order(equities)
