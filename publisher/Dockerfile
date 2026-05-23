FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y \
    gcc \
    make \
    cmake \
    libpaho-mqtt-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY publisher/src/ ./src/
COPY shared/ ./shared/
COPY publisher/Makefile .

RUN make

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libpaho-mqtt1.3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/publisher .

CMD ["./publisher"]