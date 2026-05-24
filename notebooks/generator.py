import csv
import random
import time

import requests

def load_product_ids():
    with open("data/product_catalog.csv", newline="", encoding="utf-8") as f:
        return [row["product_id"] for row in csv.DictReader(f)]


def build_order(product_ids):
    n = random.randint(1, 4)
    chosen = random.sample(product_ids, n)
    items = [{"product_id": pid, "quantity": random.randint(1, 3)} for pid in chosen]
    user_id = f"user_{random.randint(1, 100):04d}"
    return {"user_id": user_id, "items": items}


def main():
    product_ids = load_product_ids()
    print(f"[generator] wczytano {len(product_ids)} produktow")

    while True:
        order = build_order(product_ids)
        resp = requests.post("http://localhost:5000/order", json=order)
        body = resp.json()
        print(f"[generator] user_id={order['user_id']} items={len(order['items'])} "
              f"-> {resp.status_code} order_id={body.get('order_id')}")
        time.sleep(1 / 2)


if __name__ == "__main__":
    main()
