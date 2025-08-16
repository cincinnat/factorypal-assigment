FROM docker.io/bitnami/spark:latest

ADD --chmod=777 healthcheck.sh /usr/bin/

USER root
RUN apt-get update && apt-get install -y curl jq
USER 1001
