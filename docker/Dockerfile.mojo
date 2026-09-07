# The iceberg.mojo image.
#
# Built on Debian 12 rather than 13 on purpose: pixi resolves conda packages
# against a minimum glibc, and the runtime stage is distroless/cc-debian12
# (glibc 2.36). Building on trixie would link against 2.41 and the binary would
# not start there.
#
# Two stages. The first is a full Mojo toolchain and every tin in the stack
# compiled from source, which is large and is thrown away. The second holds a
# 2 MB binary and the handful of shared libraries it actually opens.

FROM debian:bookworm-slim AS build

# clang is not optional: `mojo build` shells out to a C compiler to link, and
# without one it fails with "unable to find suitable c compiler for linking".
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates clang curl git \
    && rm -rf /var/lib/apt/lists/*

# The same pixi the repositories pin; a lock written by 0.78 is rejected by
# older versions, so this is not a free choice.
ENV PIXI_HOME=/opt/pixi
ENV PATH=/opt/pixi/bin:$PATH
RUN curl -fsSL https://pixi.sh/install.sh | PIXI_VERSION=v0.78.0 bash

WORKDIR /src

# Dependencies first: the tin chain is the slow part of the build and it only
# has to be redone when the manifest moves, not when a query changes.
COPY pixi.toml pixi.lock ./
RUN pixi install

COPY src ./src
RUN pixi run build

# 1 keeps object-storage support (and its ~50 MB of curl/TLS/Kerberos/ICU);
# 0 builds the local-filesystem-only image. See docker/collect-libs.sh.
ARG TAXIBENCH_OBJECTSTORE=1

COPY docker/collect-libs.sh ./docker/
RUN TAXIBENCH_OBJECTSTORE=$TAXIBENCH_OBJECTSTORE \
    bash docker/collect-libs.sh .pixi/envs/default build/taxibench /opt/taxibench


FROM gcr.io/distroless/cc-debian12

COPY --from=build /opt/taxibench /opt/taxibench

# Every tin finds its C shim at $CONDA_PREFIX/lib. Outside a pixi environment
# that variable is unset and the lookup falls back to a path relative to the
# working directory, which in a container resolves to nothing; pointing it at
# the staged prefix is what makes the binary work here.
ENV CONDA_PREFIX=/opt/taxibench
ENV LD_LIBRARY_PATH=/opt/taxibench/lib

# Mount the warehouse and name it: `docker run -v $PWD/build/warehouse:/data …`
ENTRYPOINT ["/opt/taxibench/bin/taxibench"]
CMD ["/data"]
