import json
import uuid
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from kafka import KafkaProducer

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers="broker:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)


@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.post("/order")
def order():
    data = request.get_json(silent=True) or {}

    user_id = data.get("user_id")
    if not user_id:
        return jsonify({"error": "brak user_id"}), 400

    items = data.get("items")
    if not isinstance(items, list) or len(items) == 0:
        return jsonify({"error": "pusta lista produktow"}), 400

    for item in items:
        if not isinstance(item, dict) or "product_id" not in item:
            return jsonify({"error": "brak product_id"}), 400
        quantity = item.get("quantity")
        if not isinstance(quantity, int) or quantity <= 0:
            return jsonify({"error": "quantity musi być > 0"}), 400

    order = {
        "order_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "items": items,
    }

    producer.send("orders.raw", key=user_id, value=order)
    print(f"[orders] published order_id={order['order_id']} user_id={user_id} items={len(items)}")

    return jsonify({"order_id": order["order_id"]}), 202


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
