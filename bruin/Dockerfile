FROM ghcr.io/bruin-data/bruin:latest

COPY . /workspace
WORKDIR /workspace

USER root
COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh \
 && chmod +x /entrypoint.sh \
 && chown -R bruin:bruin /workspace \
 && git config --global safe.directory /workspace \
 && cd /workspace \
 && git init \
 && git add -A \
 && git -c user.name="bruin" -c user.email="bruin@local" commit --allow-empty -m "init"

USER bruin
ENTRYPOINT ["/entrypoint.sh"]