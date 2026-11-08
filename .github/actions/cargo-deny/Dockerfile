FROM rust:1.85.0-alpine3.20@sha256:f0cef6c65992995b1c7816cb667de95799852e3fbed9d06f95855cbc512a0fd0

ENV deny_version="0.20.2"

COPY download.sh /download.sh
RUN set -eux; \
    apk update; \
    apk add bash curl git tar openssh; \
    sh /download.sh \
      https://api.github.com/repos/EmbarkStudios/cargo-deny/releases/assets/471598214 \
      9f12ed4c49936e09b48bf862b595cde2fe64fcbd9d74dfacac6131ca824c8d5f \
      4936832 /tmp/cargo-deny.tar.gz; \
    tar -xzvf /tmp/cargo-deny.tar.gz -C /usr/bin --strip-components=1; \
    rm /tmp/cargo-deny.tar.gz

COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
