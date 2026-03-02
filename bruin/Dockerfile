FROM ghcr.io/bruin-data/bruin:latest

COPY . /workspace
WORKDIR /workspace

USER root
COPY entrypoint.sh /entrypoint.sh
RUN chown -R bruin:bruin /workspace
RUN chmod +x /entrypoint.sh

USER bruin
ENTRYPOINT ["/entrypoint.sh"]