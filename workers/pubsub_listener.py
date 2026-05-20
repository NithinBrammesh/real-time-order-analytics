import redis
import json

# =========================
# 🔹 REDIS CONNECTION
# =========================
r = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

# =========================
# 🔹 PUBSUB
# =========================
pubsub = r.pubsub()

pubsub.subscribe("live_alerts")

print("🚀 Listening For Live Alerts...")


# =========================
# 🔹 CONTINUOUS LISTENER
# =========================
for message in pubsub.listen():

    try:

        if message["type"] != "message":
            continue

        data = json.loads(
            message["data"]
        )

        print("\n==============================")
        print("📢 LIVE ALERT RECEIVED")
        print("==============================")

        print(f"🏙️ City       : {data.get('city')}")
        print(f"💰 Amount     : ₹{data.get('amount')}")
        print(f"⚠️ Severity   : {data.get('severity')}")
        print(f"📦 Product    : {data.get('product_name')}")
        print(f"👤 Customer   : {data.get('customer_name')}")

    except Exception as e:

        print(f"❌ PUBSUB ERROR: {e}")