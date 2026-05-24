FROM flink:1.18-scala_2.12-java11

WORKDIR /app

# Install Python and all required packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        netcat-openbsd && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir \
        apache-flink==1.18.0 \
        redis \
        kafka-python \
        python-dotenv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy entire project into container
COPY . .

# Tell PyFlink which Python binary to use
ENV PYFLINK_CLIENT_EXECUTABLE=python3

# Startup script that waits for Kafka topic before launching Flink job
COPY docker/flink-entrypoint.sh /flink-entrypoint.sh
RUN chmod +x /flink-entrypoint.sh

ENTRYPOINT ["/flink-entrypoint.sh"]