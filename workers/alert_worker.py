import redis
import json
from datetime import datetime

# =========================
# 🔹 REDIS CONNECTION
# =========================
r = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

print("🚀 Alert Worker Started")


# =========================
# 🔹 QUEUE CONSUMER
# =========================
while True:

    try:

        # WAIT FOR ALERT
        task = r.brpop(
            "high_value_orders_queue"
        )

        # JSON → OBJECT
        data = json.loads(task[1])

        print("\n==============================")
        print("🚨 HIGH VALUE ALERT RECEIVED")
        print("==============================")

        print(f"🆔 Order ID      : {data.get('order_id')}")
        print(f"👤 Customer      : {data.get('customer_name')}")
        print(f"📦 Product       : {data.get('product_name')}")
        print(f"🏙️ City          : {data.get('city')}")
        print(f"💰 Amount        : ₹{data.get('amount')}")
        print(f"📊 Average       : ₹{data.get('avg')}")
        print(f"⚠️ Severity      : {data.get('severity')}")
        print(f"💳 Payment       : {data.get('payment_method')}")
        print(f"🕒 Time          : {datetime.now()}")

        # =========================
        # 🔹 SIMULATED ACTION
        # =========================
        if data.get("severity") == "CRITICAL":

            print("📢 Sending Admin Notification...")

        elif data.get("severity") == "HIGH":

            print("📧 Sending Email Alert...")

        else:

            print("ℹ️ Medium Priority Alert")

    except Exception as e:

        print(f"❌ ALERT WORKER ERROR: {e}")