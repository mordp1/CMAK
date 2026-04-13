#!/usr/bin/env bash
# Build CMAK entirely inside Docker — no local Java/SBT needed.
# Usage: ./build-docker.sh

set -euo pipefail

BUILDER_IMAGE="sbtscala/scala-sbt:eclipse-temurin-11.0.17_8_1.8.2_2.12.17"
VERSION="3.0.1"

echo "==> Compiling and packaging with SBT inside Docker..."
docker run --rm -v "$(pwd)":/app -w /app "$BUILDER_IMAGE" sbt dist

echo "==> Building runtime Docker image cmak:$VERSION..."
docker build -t "cmak:$VERSION" -f Dockerfile.runtime .

echo ""
echo "Done! Image cmak:$VERSION is ready."
echo "Run:  docker compose up -d"
