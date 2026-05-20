from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource

from pyflink.common import Types
from pyflink.common.watermark_strategy import WatermarkStrategy

from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

import redis
import json
import xml.etree.ElementTree as ET
import re


# =========================
# 🔹 FLINK ENVIRONMENT
# =========================
env = StreamExecutionEnvironment.get_execution_environment()

env.add_jars(
    "file:///home/nithin/Downloads/real-time-order-analytics/lib/flink-connector-kafka-3.0.2-1.18.jar",
    "file:///home/nithin/Downloads/real-time-order-analytics/lib/kafka-clients-3.5.1.jar"
)


# =========================
# 🔹 KAFKA SOURCE
# =========================
try:

    source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("orders_events_v2") \
        .set_group_id("flink-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    print("✅ Kafka Source Connected")

except Exception as e:

    print(f"❌ Kafka Source Failed: {e}")
    raise e


# =========================
# 🔹 CREATE STREAM
# =========================
stream = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Source"
)


# =========================
# 🔹 CLEAN CITY
# =========================
def clean_city(city_raw):

    if not city_raw:
        return "UNKNOWN"

    city = str(city_raw)

    city = re.sub(r'\d+', '', city)

    city = re.sub(r'\s+', ' ', city)

    city = city.strip().upper()

    return city


# =========================
# 🔹 PARSER
# =========================
def parse_input(msg):

    try:

        msg = msg.strip()

        # =========================
        # 🔹 JSON PARSER
        # =========================
        try:

            data = json.loads(msg)

            if isinstance(data, dict):

                return data

        except:
            pass


        # =========================
        # 🔹 CSV PARSER
        # =========================
        try:

            parts = msg.split(",")

            if len(parts) < 14:
                return None

            return {

                "order_id": parts[0].strip(),

                "customer_name": parts[1].strip(),

                "product_name": parts[2].strip(),

                "category": parts[3].strip(),

                "payment_method": parts[4].strip(),

                "city": parts[5].strip(),

                "state": parts[6].strip(),

                "quantity": int(parts[7]),

                "amount": float(parts[8]),

                "shipping_fee": float(parts[9]),

                "order_status": parts[10].strip(),

                "order_source": parts[11].strip(),

                "order_date": parts[12].strip(),

                "timestamp": parts[13].strip()
            }

        except Exception as e:

            print(f"❌ CSV PARSE ERROR: {e}")

            pass


        # =========================
        # 🔹 XML PARSER
        # =========================
        try:

            root = ET.fromstring(msg)

            return {

                "order_id": root.findtext("order_id"),

                "customer_name": root.findtext("customer_name"),

                "product_name": root.findtext("product_name"),

                "payment_method": root.findtext("payment_method"),

                "city": root.findtext("city"),

                "state": root.findtext("state"),

                "quantity": int(root.findtext("quantity", 0)),

                "amount": float(root.findtext("amount", 0)),

                "shipping_fee": float(
                    root.findtext("shipping_fee", 0)
                ),

                "order_status": root.findtext("order_status")
            }

        except Exception as e:

            print(f"❌ XML PARSE ERROR: {e}")

            pass


        # =========================
        # 🔹 UNSUPPORTED FORMAT
        # =========================
        print(f"❌ Unsupported Format: {msg}")

        return None

    except Exception as e:

        print(f"❌ GENERAL PARSER ERROR: {e}")

        return None


# =========================
# 🔹 STATEFUL ANALYTICS
# =========================
class AvgPerCity(KeyedProcessFunction):

    def open(self, runtime_context):

        # =========================
        # 🔹 STATE
        # =========================
        self.sum_state = runtime_context.get_state(
            ValueStateDescriptor(
                "sum",
                Types.FLOAT()
            )
        )

        self.count_state = runtime_context.get_state(
            ValueStateDescriptor(
                "count",
                Types.INT()
            )
        )

        # =========================
        # 🔹 REDIS CONNECTION
        # =========================
        self.redis_client = redis.Redis(
            host="localhost",
            port=6379,
            decode_responses=True
        )

        print("✅ Redis Connected")


    # =========================
    # 🔹 PROCESS EVENT
    # =========================
    def process_element(self, value, ctx):

        try:

            if not value:
                return

            # =========================
            # 🔹 EXTRACT FIELDS
            # =========================
            amount = float(value["amount"])

            city = clean_city(value["city"])

            quantity = int(
                value.get("quantity", 0)
            )

            payment_method = value.get(
                "payment_method",
                "UNKNOWN"
            )


            # =========================
            # 🔹 CURRENT STATE
            # =========================
            current_sum = self.sum_state.value() or 0.0

            current_count = self.count_state.value() or 0


            # =========================
            # 🔹 UPDATE STATE
            # =========================
            current_sum += amount

            current_count += 1


            self.sum_state.update(current_sum)

            self.count_state.update(current_count)


            # =========================
            # 🔹 AVG CALCULATION
            # =========================
            avg = (
                current_sum / current_count
                if current_count > 0
                else 0
            )


            # =========================
            # 🔹 SEVERITY LOGIC
            # =========================
            severity = "NORMAL"

            if amount >= 300000:
                severity = "CRITICAL"

            elif amount >= 100000:
                severity = "HIGH"

            elif amount >= 50000:
                severity = "MEDIUM"


            # =========================
            # 🔹 FLAGS
            # =========================
            is_high_value = amount >= 100000

            bulk_order = quantity >= 5

            suspicious_payment = (
                payment_method == "COD"
                and amount >= 150000
            )


            # =========================
            # 🔹 FINAL ALERT OBJECT
            # =========================
            alert_data = {

                "order_id": value.get("order_id"),

                "customer_name": value.get(
                    "customer_name"
                ),

                "product_name": value.get(
                    "product_name"
                ),

                "category": value.get(
                    "category"
                ),

                "payment_method": payment_method,

                "city": city,

                "state": value.get("state"),

                "quantity": quantity,

                "amount": amount,

                "shipping_fee": value.get(
                    "shipping_fee"
                ),

                "order_status": value.get(
                    "order_status"
                ),

                "order_source": value.get(
                    "order_source"
                ),

                "avg": round(avg, 2),

                "total_orders": current_count,

                "severity": severity,

                "is_high_value": is_high_value,

                "bulk_order": bulk_order,

                "suspicious_payment": suspicious_payment
            }


            # =========================
            # 🔹 DEBUG LOG
            # =========================
            print(
                f"📊 {city} | "
                f"Amount: {amount} | "
                f"Avg: {avg} | "
                f"Severity: {severity}"
            )


            # =========================
            # 🔹 STORE CITY ANALYTICS
            # =========================
            self.redis_client.hset(
                f"city_avg:{city}",
                mapping={

                    "city": city,

                    "total_amount": round(
                        current_sum,
                        2
                    ),

                    "count": current_count,

                    "avg": round(avg, 2)
                }
            )


            # =========================
            # 🔹 STORE ALERTS
            # =========================
            if severity != "NORMAL":

                # QUEUE
                self.redis_client.lpush(
                    "high_value_orders_queue",
                    json.dumps(alert_data)
                )

                # PUB SUB
                self.redis_client.publish(
                    "live_alerts",
                    json.dumps(alert_data)
                )

                # STREAMS
                self.redis_client.xadd(
                    "orders_stream",
                    alert_data
                )

                print(
                    f"🚨 ALERT GENERATED | "
                    f"{severity} | "
                    f"{city} | "
                    f"Amount: {amount}"
                )

        except Exception as e:

            print(
                f"❌ PROCESS ERROR | "
                f"Value: {value} | "
                f"Error: {e}"
            )


# =========================
# 🔹 PIPELINE
# =========================
processed_stream = stream \
    .map(lambda msg: parse_input(msg)) \
    .filter(lambda data: data is not None) \
    .filter(
        lambda data:
        data.get("city") is not None and
        data.get("amount") is not None
    ) \
    .map(lambda x: {

        **x,

        "amount": float(x["amount"])
    }) \
    .filter(
        lambda x:
        x["amount"] > 0
    ) \
    .key_by(
        lambda x:
        clean_city(x["city"])
    ) \
    .process(
        AvgPerCity()
    )


# =========================
# 🔹 EXECUTE FLINK JOB
# =========================
try:

    print("🚀 Flink Job Started...")

    env.execute(
        "Realtime Ecommerce Analytics Engine"
    )

except Exception as e:

    print(f"❌ Flink Job Failed: {e}")