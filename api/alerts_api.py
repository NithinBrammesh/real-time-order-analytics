from flask import Flask, jsonify
from flask_cors import CORS

from flask_socketio import SocketIO
import threading
import redis
import json
import os
from dotenv import load_dotenv

# =========================
# 🔹 FLASK APP
# =========================
app = Flask(__name__)

socketio = SocketIO(
    app,
    cors_allowed_origins="*"
)

CORS(app)


# =========================
# 🔹 LOAD ENV VARIABLES
# =========================
load_dotenv()

REDIS_HOST = os.getenv(
    "REDIS_HOST",
    "redis"
)

REDIS_PORT = os.getenv(
    "REDIS_PORT",
    6379
)

FLASK_HOST = os.getenv(
    "FLASK_HOST",
    "0.0.0.0"
)

FLASK_PORT = os.getenv(
    "FLASK_PORT",
    5000
)

# =========================
# 🔹 REDIS CONNECTION
# =========================
try:

    r = redis.Redis(
        host=REDIS_HOST,
        port=int(REDIS_PORT),
        decode_responses=True
    )

    r.ping()

    print("✅ Redis Connected")

except Exception as e:

    print(f"❌ Redis Connection Failed: {e}")


# =========================
# 🔹 REDIS PUBSUB LISTENER
# =========================
def redis_listener():

    pubsub = r.pubsub()

    pubsub.subscribe("live_alerts")

    print("🚀 WebSocket Listener Started")

    for message in pubsub.listen():

        try:

            if message["type"] != "message":
                continue

            data = json.loads(
                message["data"]
            )

            print(
                f"📡 Sending Live Alert: {data}"
            )

            # EMIT TO FRONTEND
            socketio.emit(
                "new_alert",
                data
            )

        except Exception as e:

            print(
                f"❌ WebSocket Error: {e}"
            )

# =========================
# 🔹 HEALTH CHECK
# =========================
@app.route("/")
def home():

    return jsonify({

        "success": True,

        "message":
        "Realtime Ecommerce Analytics API Running"

    })


# =========================
# 🔹 CITY ALERTS API
# =========================
@app.route("/alerts")
def get_alerts():

    try:

        # =========================
        # 🔹 READ REDIS STREAM
        # =========================
        data = r.xrevrange(
            "orders_stream",
            count=100
        )

        city_map = {}

        for message_id, values in data:

            city = values.get(
                "city",
                "UNKNOWN"
            )

            amount = float(
                values.get("amount", 0)
            )

            avg = float(
                values.get("avg", 0)
            )

            severity = values.get(
                "severity",
                "NORMAL"
            )

            # =========================
            # 🔹 CREATE CITY ENTRY
            # =========================
            if city not in city_map:

                city_map[city] = {

                    "city": city,

                    "count": 0,

                    "total_amount": 0,

                    "avg": 0,

                    "severity": severity
                }

            # =========================
            # 🔹 UPDATE ANALYTICS
            # =========================
            city_map[city]["count"] += 1

            city_map[city]["total_amount"] += amount

            city_map[city]["avg"] = avg

        # =========================
        # 🔹 CONVERT TO LIST
        # =========================
        alerts = list(city_map.values())

        # =========================
        # 🔹 SORT
        # =========================
        alerts.sort(
            key=lambda x:
            x["total_amount"],
            reverse=True
        )

        return jsonify({

            "success": True,

            "count": len(alerts),

            "alerts": alerts

        })

    except Exception as e:

        return jsonify({

            "success": False,

            "error": str(e)

        })


# =========================
# 🔹 RECENT ORDERS API
# =========================
@app.route("/recent-orders")
def recent_orders():

    try:

        data = r.xrevrange(
            "orders_stream",
            count=20
        )

        orders = []

        for message_id, values in data:

            orders.append({

                "id": message_id,

                "order_id":
                values.get("order_id"),

                "customer_name":
                values.get("customer_name"),

                "product_name":
                values.get("product_name"),

                "payment_method":
                values.get("payment_method"),

                "city":
                values.get("city"),

                "amount":
                float(values.get("amount", 0)),

                "severity":
                values.get("severity"),

                "avg":
                float(values.get("avg", 0))
            })

        return jsonify({

            "success": True,

            "orders": orders

        })

    except Exception as e:

        return jsonify({

            "success": False,

            "error": str(e)

        })


# =========================
# 🔹 METRICS API
# =========================
@app.route("/metrics")
def metrics():

    try:

        data = r.xrevrange(
            "orders_stream",
            count=100
        )

        total_orders = 0

        total_revenue = 0

        critical_orders = 0

        high_orders = 0

        cities = set()

        for message_id, values in data:

            total_orders += 1

            amount = float(
                values.get("amount", 0)
            )

            total_revenue += amount

            cities.add(
                values.get("city")
            )

            severity = values.get(
                "severity",
                "NORMAL"
            )

            if severity == "CRITICAL":
                critical_orders += 1

            if severity in ["HIGH", "MEDIUM"]:
                high_orders += 1

        avg_order = (
            total_revenue / total_orders
            if total_orders > 0
            else 0
        )

        return jsonify({

            "success": True,

            "metrics": {

                "total_orders":
                total_orders,

                "total_revenue":
                round(total_revenue, 2),

                "average_order":
                round(avg_order, 2),

                "critical_orders":
                critical_orders,

                "high_orders":
                high_orders,

                "total_cities":
                len(cities)
            }
        })

    except Exception as e:

        return jsonify({

            "success": False,

            "error": str(e)

        })


# =========================
# 🔹 HIGH SEVERITY ALERTS
# =========================
@app.route("/high-severity")
def high_severity():

    try:

        data = r.xrevrange(
            "orders_stream",
            count=50
        )

        alerts = []

        for message_id, values in data:

            severity = values.get(
                "severity",
                "NORMAL"
            )

            if severity in [
                "HIGH",
                "CRITICAL"
            ]:

                alerts.append(values)

        return jsonify({

            "success": True,

            "count": len(alerts),

            "alerts": alerts

        })

    except Exception as e:

        return jsonify({

            "success": False,

            "error": str(e)

        })


# =========================
# 🔹 START SERVER
# =========================
if __name__ == "__main__":
    
    listener_thread = threading.Thread(
    target=redis_listener
    )

    listener_thread.daemon = True

    listener_thread.start()

    print("🚀 Flask API Started")

    socketio.run(
        app,
        host=FLASK_HOST,
        port=int(FLASK_PORT),
        debug=True
    )