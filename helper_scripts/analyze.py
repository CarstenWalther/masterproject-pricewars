"""
Analyzes the dump of kafka data, that was created by benchmark.py.
Results (e.g. a merchant's profit and revenue) are saved to a CSV file.
"""

import argparse
import csv
import datetime
import itertools
import os
import json
from collections import defaultdict

import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator


def load_merchant_id_mapping(directory):
    with open(os.path.join(directory, 'merchant_id_mapping.json')) as file:
        return json.load(file)


def analyze_kafka_dump(directory):
    merchant_id_mapping = load_merchant_id_mapping(directory)

    revenue = defaultdict(float)
    with open(os.path.join(directory, 'kafka', 'buyOffer')) as file:
        for event in json.load(file):
            revenue[event['merchant_id']] += event['amount'] * event['price']

    holding_cost = defaultdict(float)
    with open(os.path.join(directory, 'kafka', 'holding_cost')) as file:
        for event in json.load(file):
            holding_cost[event['merchant_id']] += event['cost']

    order_cost = defaultdict(float)
    with open(os.path.join(directory, 'kafka', 'producer')) as file:
        for event in json.load(file):
            order_cost[event['merchant_id']] += event['billing_amount']

    profit = {merchant_id: revenue[merchant_id] - holding_cost[merchant_id] - order_cost[merchant_id]
              for merchant_id in merchant_id_mapping}

    with open(os.path.join(directory, 'results.csv'), 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['name', 'revenue', 'holding_cost', 'order_cost', 'profit'])
        for merchant_id in sorted(merchant_id_mapping, key=merchant_id_mapping.get):
            writer.writerow([merchant_id_mapping[merchant_id], revenue[merchant_id], holding_cost[merchant_id],
                             order_cost[merchant_id], profit[merchant_id]])

    create_inventory_graph(directory, merchant_id_mapping)
    #create_price_graph(directory, merchant_id_mapping)
    create_price_graph_reduced(directory, merchant_id_mapping)


def create_inventory_graph(directory, merchant_id_mapping):
    """
    Calculates inventory levels from orders and sales and generates a graph from it.
    """
    sales = json.load(open(os.path.join(directory, 'kafka', 'buyOffer')))
    orders = json.load(open(os.path.join(directory, 'kafka', 'producer')))
    sales = [sale for sale in sales if sale['http_code'] == 200]
    convert_timestamps(sales)
    convert_timestamps(orders)
    sale_index = 0
    order_index = 0
    inventory_progressions = defaultdict(list)

    while sale_index < len(sales) and order_index < len(orders):
        if sale_index >= len(sales):
            order = orders[order_index]
            inventory_progressions[order['merchant_id']].append((order['timestamp'], order['amount']))
            order_index += 1
        elif order_index >= len(orders):
            sale = sales[sale_index]
            inventory_progressions[sale['merchant_id']].append((sale['timestamp'], -1 * sale['amount']))
            sale_index += 1
        elif orders[order_index]['timestamp'] <= sales[sale_index]['timestamp']:
            order = orders[order_index]
            inventory_progressions[order['merchant_id']].append((order['timestamp'], order['amount']))
            order_index += 1
        else: # orders[order_index]['timestamp'] > sales[sale_index]['timestamp']
            sale = sales[sale_index]
            inventory_progressions[sale['merchant_id']].append((sale['timestamp'], -1 * sale['amount']))
            sale_index += 1

    fig, ax = plt.subplots()
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    for merchant_id in inventory_progressions:
        dates, inventory_changes = zip(*inventory_progressions[merchant_id])
        inventory_levels = list(itertools.accumulate(inventory_changes))
        ax.step(dates, inventory_levels, where='post', label=merchant_id_mapping[merchant_id])
    ax.set_ylim(ymin=0)
    plt.ylabel('Inventory Level')
    plt.xlabel('Time')
    fig.legend()
    fig.autofmt_xdate()
    fig.savefig(os.path.join(directory, 'inventory_levels.svg'))


def create_price_graph(directory, merchant_id_mapping):
    offer_updates = json.load(open(os.path.join(directory, 'kafka', 'addOffer')))
    offer_updates += json.load(open(os.path.join(directory, 'kafka', 'updateOffer')))
    offer_updates = [offer for offer in offer_updates if offer['http_code'] == 200]
    convert_timestamps(offer_updates)
    offer_updates.sort(key=lambda offer: offer['timestamp'])

    # TODO: comment
    color_cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']
    merchants_visited = 0
    merchant_colors = {}
    merchant_by_offer = {}

    prices_over_times_by_offer_id = defaultdict(list)
    for offer in offer_updates:
        offer_id = offer['offer_id']
        prices_over_times_by_offer_id[offer_id].append((offer['price'], offer['timestamp']))
        if offer_id not in merchant_by_offer:
            merchant_id = offer['merchant_id']
            if merchant_id not in merchant_colors:
                merchant_colors[merchant_id] = color_cycle[merchants_visited % len(color_cycle)]
                merchants_visited += 1
            merchant_by_offer[offer_id] = merchant_id

    fig, ax = plt.subplots()
    for offer_id, prices_over_time in prices_over_times_by_offer_id.items():
        prices, times = zip(*prices_over_time)
        merchant_id = merchant_by_offer[offer_id]
        color = merchant_colors[merchant_id]
        label = 'Offer {} ({})'.format(offer_id, merchant_id_mapping[merchant_id])
        ax.step(times, prices, where='post', color=color, label=label)
    ax.set_ylim(ymin=0)
    plt.ylabel('Price')
    plt.xlabel('Time')
    fig.legend()
    fig.autofmt_xdate()
    fig.savefig(os.path.join(directory, 'prices.svg'))


def create_price_graph_reduced(directory, merchant_id_mapping):
    offer_updates = json.load(open(os.path.join(directory, 'kafka', 'addOffer')))
    offer_updates += json.load(open(os.path.join(directory, 'kafka', 'updateOffer')))
    offer_updates = [offer for offer in offer_updates if offer['http_code'] == 200]
    convert_timestamps(offer_updates)
    offer_updates.sort(key=lambda offer: offer['timestamp'])

    prices_over_times_by_merchant = defaultdict(list)
    for offer in offer_updates:
        merchant_id = offer['merchant_id']
        prices_over_times_by_merchant[merchant_id].append((offer['price'], offer['timestamp']))

    fig, ax = plt.subplots()
    for merchant_id, prices_over_time in prices_over_times_by_merchant.items():
        prices, times = zip(*prices_over_time)
        ax.step(times, prices, where='post', label=merchant_id_mapping[merchant_id])
    ax.set_ylim(ymin=0)
    plt.ylabel('Price')
    plt.xlabel('Time')
    fig.legend()
    fig.autofmt_xdate()
    fig.savefig(os.path.join(directory, 'prices_reduced.svg'))


def convert_timestamps(events):
    for event in events:
        # TODO: use better conversion; strptime discards timezone
        event['timestamp'] = datetime.datetime.strptime(event['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')


def main():
    parser = argparse.ArgumentParser(description='Analyzes the data generated by benchmark.py')
    parser.add_argument('--directory', '-d', type=str, required=True)
    args = parser.parse_args()
    analyze_kafka_dump(args.directory)


if __name__ == '__main__':
    main()
