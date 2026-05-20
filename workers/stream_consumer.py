import redis

# =========================
# 🔹 REDIS CONNECTION
# =========================
r = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

print("🚀 Redis Stream Consumer Started")

# =========================
# 🔹 STARTING STREAM ID
# =========================
last_id = "0-0"


# =========================
# 🔹 STREAM READER
# =========================
while True:

    try:

        messages = r.xread(
            {"orders_stream": last_id},
            block=5000,
            count=10
        )

        if not messages:
            continue

        for stream_name, stream_data in messages:

            for message_id, data in stream_data:

                print("\n==============================")
                print("📩 STREAM EVENT RECEIVED")
                print("==============================")

                print(f"🆔 Stream ID     : {message_id}")
                print(f"🏙️ City          : {data.get('city')}")
                print(f"💰 Amount        : ₹{data.get('amount')}")
                print(f"📊 Avg           : ₹{data.get('avg')}")
                print(f"⚠️ Severity      : {data.get('severity')}")
                print(f"📦 Product       : {data.get('product_name')}")
                print(f"👤 Customer      : {data.get('customer_name')}")

                # UPDATE OFFSET
                last_id = message_id

    except Exception as e:

        print(f"❌ STREAM ERROR: {e}")