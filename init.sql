CREATE STREAM ORDERS_STREAM (
  order_id STRING,
  order_date STRING,
  buyer_name STRING,
  city STRING,
  state STRING,
  sku STRING,
  description STRING,
  quantity INT,
  amount DOUBLE,
  event_time BIGINT
)
WITH (
  KAFKA_TOPIC='orders_events_v2',
  VALUE_FORMAT='JSON',
  KEY_FORMAT='JSON',
  PARTITIONS=1,
  TIMESTAMP='event_time'
);

CREATE TABLE ORDERS_BY_CITY_NAME AS
SELECT
  city,
  buyer_name,
  LATEST_BY_OFFSET(order_id) AS order_id,
  LATEST_BY_OFFSET(order_date) AS order_date,
  LATEST_BY_OFFSET(state) AS state,
  LATEST_BY_OFFSET(sku) AS sku,
  LATEST_BY_OFFSET(description) AS description,
  LATEST_BY_OFFSET(quantity) AS quantity,
  LATEST_BY_OFFSET(amount) AS amount,
  LATEST_BY_OFFSET(event_time) AS event_time
FROM ORDERS_STREAM
GROUP BY city, buyer_name
EMIT CHANGES;
