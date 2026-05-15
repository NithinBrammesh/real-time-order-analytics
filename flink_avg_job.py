from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema
)
from pyflink.common import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.restart_strategy import RestartStrategies

import redis
import json
import xml.etree.ElementTree as ET
import re


# =========================
# 🔹 ENVIRONMENT
# =========================
env = StreamExecutionEnvironment.get_execution_environment()

env.set_restart_strategy(
    RestartStrategies.fixed_delay_restart(3, 5000)
)

env.add_jars(
    "file:///home/nithin/Downloads/kafka-docker/flink-connector-kafka-3.0.2-1.18.jar",
    "file:///home/nithin/Downloads/kafka-docker/kafka-clients-3.5.1.jar"
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


stream = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Source"
)


# =========================
# 🔹 KAFKA SINK
# =========================
try:

    sink = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("orders0_avg")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()

    print("✅ Kafka Sink Connected")

except Exception as e:

    print(f"❌ Kafka Sink Failed: {e}")
    raise e


# =========================
# 🔹 CLEAN CITY
# =========================
def clean_city(city_raw):

    city = str(city_raw)

    city = re.sub(r'\d+', '', city)
    city = re.sub(r'\s+', ' ', city)

    city = city.strip().upper()

    if len(city) > 0:

        half = len(city) // 2

        if city[:half] == city[half:]:
            city = city[:half]

    return city


# =========================
# 🔹 PARSER
# =========================
def parse_input(msg):

    msg = msg.strip()

    # JSON
    try:

        data = json.loads(msg)

        if isinstance(data, dict) and "order" in data:
            data = data["order"]

        return data

    except:
        pass

    # CSV
    try:

        parts = msg.split(",")

        if len(parts) < 3:
            return None

        return {
            "order_id": parts[0],
            "amount": parts[1],
            "city": parts[2]
        }

    except:
        pass

    # XML
    try:

        root = ET.fromstring(msg)

        return {
            "order_id": root.find("order_id").text,
            "amount": root.find("amount").text,
            "city": root.find("city").text
        }

    except:
        return None


# =========================
# 🔹 STATEFUL FUNCTION
# =========================
class AvgPerCity(KeyedProcessFunction):

    def open(self, runtime_context):

        self.sum_state = runtime_context.get_state(
            ValueStateDescriptor("sum", Types.FLOAT())
        )

        self.count_state = runtime_context.get_state(
            ValueStateDescriptor("count", Types.INT())
        )

        # ✅ REDIS CONNECTION
        self.redis_client = redis.Redis(
            host="localhost",
            port=6379,
            decode_responses=True
        )

    def process_element(self, value, ctx):

        try:

            if not value:
                return

            amount = value["amount"]
            city = clean_city(value["city"])

            current_sum = self.sum_state.value() or 0.0
            current_count = self.count_state.value() or 0

            current_sum += amount
            current_count += 1

            self.sum_state.update(current_sum)
            self.count_state.update(current_count)

            avg = current_sum / current_count if current_count > 0 else 0

            # ✅ DEBUG LOG
            print(
                f"[DEBUG] {city} | "
                f"Total: {current_sum} | "
                f"Count: {current_count} | "
                f"Avg: {avg}"
            )

            # ✅ STORE IN REDIS
            self.redis_client.hset(
                f"city_avg:{city}",
                mapping={
                    "city": city,
                    "total_amount": round(current_sum, 2),
                    "count": current_count,
                    "avg": round(avg, 2)
                }
            )

            # ✅ SEND TO KAFKA SINK
            yield json.dumps({
                "city": city,
                "total_amount": round(current_sum, 2),
                "count": current_count,
                "avg": round(avg, 2)
            })

        except Exception as e:

            print(f"❌ PROCESS ERROR: {value} | {e}")


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
    .filter(lambda x: x["amount"] > 0) \
    .key_by(lambda x: clean_city(x["city"])) \
    .process(
        AvgPerCity(),
        output_type=Types.STRING()
    )


# =========================
# 🔹 SINK
# =========================
processed_stream.sink_to(sink)


# =========================
# 🔹 EXECUTE
# =========================
try:

    print("🚀 Flink Job Started...")
    env.execute("Flink City-wise Total + Avg")

except Exception as e:

    print(f"❌ Flink Job Failed: {e}")