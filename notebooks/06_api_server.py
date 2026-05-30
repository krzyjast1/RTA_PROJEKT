"""
ZADANIE 6 - Dashboard API.
W tle czyta agg.revenue.by.category i agg.orders.by.product (od latest),
buforuje ostatnie okno per klucz i wystawia REST API na porcie 5001.
"""
import json
import time
import threading
from datetime import datetime, timezone
from flask import Flask, jsonify, request
from flask_cors import CORS
from confluent_kafka import Consumer

KAFKA_BROKER              = "broker:9092"
TOPIC_REVENUE_BY_CATEGORY = "agg.revenue.by.category"
TOPIC_ORDERS_BY_PRODUCT   = "agg.orders.by.product"

_revenue_store = {}
_orders_store  = {}
_lock = threading.Lock()

def _make_consumer(group_id):
    return Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })

def _consume(topic, store, key_field, group_prefix):
    # Unikalna grupa przy kazdym starcie -> czysty start od latest
    c = _make_consumer(f"{group_prefix}-{int(time.time())}")
    c.subscribe([topic])
    while True:
        msg = c.poll(timeout=1.0)
        if msg is None or msg.error():
            continue
        try:
            rec = json.loads(msg.value().decode("utf-8"))
            k = rec.get(key_field)
            if k:
                with _lock:
                    ex = store.get(k)
                    if ex is None or rec.get("window_end", "") >= ex.get("window_end", ""):
                        store[k] = rec
        except Exception:
            pass

app = Flask(__name__)
CORS(app)

@app.get("/health")
def health():
    return jsonify({
        "status": "ok",
        "categories_tracked": len(_revenue_store),
        "products_tracked": len(_orders_store),
    })

@app.get("/top-categories")
def top_categories():
    n = max(1, min(int(request.args.get("n", 5)), 50))
    with _lock:
        rev = sorted(_revenue_store.values(), key=lambda r: r.get("total_revenue", 0), reverse=True)
    return jsonify({
        "n": n,
        "top_categories": [
            {"rank": i + 1, "category": r.get("category"), "total_revenue": r.get("total_revenue"),
             "window_start": r.get("window_start"), "window_end": r.get("window_end")}
            for i, r in enumerate(rev[:n])
        ],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

@app.get("/top-products")
def top_products():
    m = max(1, min(int(request.args.get("m", 10)), 100))
    with _lock:
        ords = sorted(_orders_store.values(), key=lambda r: r.get("order_count", 0), reverse=True)
    return jsonify({
        "m": m,
        "top_products": [
            {"rank": i + 1, "product_id": r.get("product_id"), "product_name": r.get("product_name"),
             "order_count": r.get("order_count"), "window_start": r.get("window_start"), "window_end": r.get("window_end")}
            for i, r in enumerate(ords[:m])
        ],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

@app.get("/summary")
def summary():
    n = max(1, min(int(request.args.get("n", 5)), 50))
    m = max(1, min(int(request.args.get("m", 10)), 100))
    with _lock:
        rev = sorted(_revenue_store.values(), key=lambda r: r.get("total_revenue", 0), reverse=True)
        ords = sorted(_orders_store.values(), key=lambda r: r.get("order_count", 0), reverse=True)
    return jsonify({
        "top_categories": [
            {"rank": i + 1, "category": r.get("category"), "total_revenue": r.get("total_revenue")}
            for i, r in enumerate(rev[:n])
        ],
        "top_products": [
            {"rank": i + 1, "product_id": r.get("product_id"), "product_name": r.get("product_name"), "order_count": r.get("order_count")}
            for i, r in enumerate(ords[:m])
        ],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

if __name__ == "__main__":
    threading.Thread(target=_consume, args=(TOPIC_REVENUE_BY_CATEGORY, _revenue_store, "category", "api-revenue"), daemon=True).start()
    threading.Thread(target=_consume, args=(TOPIC_ORDERS_BY_PRODUCT, _orders_store, "product_id", "api-orders"), daemon=True).start()
    print("[api] serwer startuje na porcie 5001")
    app.run(host="0.0.0.0", port=5001, threaded=True)
