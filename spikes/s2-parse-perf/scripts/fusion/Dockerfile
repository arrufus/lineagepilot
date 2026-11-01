# dbt v2 (Fusion engine) for spike S2. Fusion runs ONLY inside this container.
# Installed with the official script from the dbt docs (docs/local/install-dbt?version=2.0).
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl git \
 && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --shell /bin/bash dbt
USER dbt
WORKDIR /home/dbt
ENV SHELL=/bin/bash
RUN curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh -o /tmp/install.sh \
 && sha256sum /tmp/install.sh > /home/dbt/install.sh.sha256 \
 && sh /tmp/install.sh --update \
 && rm /tmp/install.sh
ENV PATH=/home/dbt/.local/bin:$PATH
ENV DBT_SEND_ANONYMOUS_USAGE_STATS=false DO_NOT_TRACK=1
RUN dbt --version > /home/dbt/dbt-version.txt 2>&1 || true
