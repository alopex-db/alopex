ARG BASE
FROM ${BASE}

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

COPY deps/ /deps/
RUN printf '#!/bin/sh\nexit 101\n' > /usr/sbin/policy-rc.d \
    && chmod 0755 /usr/sbin/policy-rc.d \
    && mkdir -p /etc/postgresql-common \
    && printf 'create_main_cluster = false\n' > /etc/postgresql-common/createcluster.conf \
    && mkdir -p /tmp/offline-apt-lists/partial \
    && apt-get --no-install-recommends -y \
       -o Dir::Etc::sourcelist=/dev/null -o Dir::Etc::sourceparts=- \
       -o Dir::State::lists=/tmp/offline-apt-lists \
       -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold \
       install /deps/*.deb \
    && rm -rf /deps /tmp/offline-apt-lists /var/cache/apt/archives/*.deb \
    && useradd --create-home --shell /bin/bash alopex \
    && printf 'alopex ALL=(root) NOPASSWD: ALL\n' > /etc/sudoers.d/alopex \
    && chmod 0440 /etc/sudoers.d/alopex \
    && visudo -cf /etc/sudoers.d/alopex

COPY *.py image-manifest.json /tool/
COPY tests/ /tool/tests/
COPY wheelhouse/ /tool/wheelhouse/
RUN chmod -R go-w /tool \
    && python3 /tool/image_verify.py

USER alopex
ENV HOME=/work/home TMPDIR=/work/tmp XDG_CACHE_HOME=/work/cache
ENTRYPOINT ["python3", "/tool/entrypoint.py"]
