# Stage 1: Build with SBT inside Docker — no local Java/SBT required
FROM sbtscala/scala-sbt:eclipse-temurin-11.0.17_8_1.8.2_2.12.17 AS builder

WORKDIR /build

# Copy dependency descriptors first for better layer caching
COPY project/ project/
COPY build.sbt ./

# Pre-download dependencies
RUN sbt update

# Copy source and build the distribution zip
COPY . .
RUN sbt dist

# Stage 2: Runtime image
FROM eclipse-temurin:11-jre-jammy

RUN apt-get update && apt-get install -y --no-install-recommends unzip && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/universal/cmak-3.0.1.zip /opt/cmak.zip

WORKDIR /opt
RUN unzip cmak.zip && rm -f cmak.zip

EXPOSE 9000

CMD ["/opt/cmak-3.0.1/bin/cmak"]
