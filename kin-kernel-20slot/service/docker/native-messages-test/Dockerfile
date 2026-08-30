# native_messages test VM: bun 1.3.14 + HTTP CONNECT → SOCKS5 bridge.
# Credentials must be injected at runtime. Do not bake tokens into this image.
FROM oven/bun:1.3.14-debian

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=bridge http_to_socks.py /opt/kin/http_to_socks.py
COPY entrypoint.sh /opt/kin/entrypoint.sh
RUN chmod +x /opt/kin/entrypoint.sh /opt/kin/http_to_socks.py

WORKDIR /opt/claude-code-kin
ENTRYPOINT ["/opt/kin/entrypoint.sh"]
CMD ["bash"]
