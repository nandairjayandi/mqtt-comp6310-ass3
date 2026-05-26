FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y \
    gcc \
    make \
    cmake \
    libpaho-mqtt-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY analyser/src/ ./src/
COPY shared/ ./shared/
COPY analyser/Makefile .

RUN make

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libpaho-mqtt1.3 \
    libcurl4 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/analyser .

CMD ["./analyser"]