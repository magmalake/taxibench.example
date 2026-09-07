#!/usr/bin/env bash
#
# Stage the binary and exactly the shared libraries it needs into one tree.
#
# `ldd` on the binary alone is not enough. The Mojo binary links only the Mojo
# runtime and libc; every C library in the stack — zstd, lz4, brotli, curl — is
# reached through a shim that the tin `dlopen`s by name at runtime, so nothing
# refers to it at link time and nothing shows up in `ldd`. The shims are
# therefore seeded explicitly below, and the closure is taken from there.
#
# Only libraries inside the pixi environment are copied. Anything resolving to
# the base image (glibc, libstdc++) is left where it is.
#
# Usage: collect-libs.sh <pixi-env> <binary> <out-prefix>
set -euo pipefail

ENV_PREFIX="$(cd "$1" && pwd)"
BINARY="$2"
OUT="$3"

mkdir -p "$OUT/bin" "$OUT/lib"
install -m 0755 "$BINARY" "$OUT/bin/taxibench"

# The libraries no `ldd` will mention, because they are opened by name at
# runtime. Each tin looks for its shim at $CONDA_PREFIX/lib, which is why the
# image sets CONDA_PREFIX to this prefix.
#
# objectstore is the expensive one and it is optional. It wraps libcurl, and
# libcurl drags in OpenSSL, Kerberos, libssh2, nghttp2 and libpsl — and libpsl
# links ICU, whose 33 MB character database is by itself a fifth of the image.
# None of it is reachable for a table on a local filesystem. Set
# TAXIBENCH_OBJECTSTORE=0 to leave it out and lose s3://, gs:// and az://.
SHIMS=(libzstdmojo liblz4mojo libbrotlimojo)
if [ "${TAXIBENCH_OBJECTSTORE:-1}" != "0" ]; then
    SHIMS+=(libobjectstoremojo)
fi

SEEDS=("$BINARY")
for shim in "${SHIMS[@]}"; do
    if [ -f "$ENV_PREFIX/lib/$shim.so" ]; then
        SEEDS+=("$ENV_PREFIX/lib/$shim.so")
        install -m 0755 "$ENV_PREFIX/lib/$shim.so" "$OUT/lib/$shim.so"
    else
        echo "warning: no $shim.so in $ENV_PREFIX/lib" >&2
    fi
done

# Breadth-first over the dependency graph. `seen` keeps the walk finite;
# libraries pull in each other (curl -> ssl -> crypto) and repeat.
declare -A seen=()
queue=("${SEEDS[@]}")
copied=0

while [ ${#queue[@]} -gt 0 ]; do
    current="${queue[0]}"
    queue=("${queue[@]:1}")

    # A binary with no dynamic section, or one ldd refuses, is not an error:
    # it simply contributes nothing to the closure.
    while read -r resolved; do
        [ -n "$resolved" ] || continue
        [ -e "$resolved" ] || continue
        real="$(readlink -f "$resolved")"
        case "$real" in
            "$ENV_PREFIX"/*) ;;
            *) continue ;;   # from the base image; not ours to ship
        esac
        name="$(basename "$resolved")"
        if [ -n "${seen[$name]:-}" ]; then
            continue
        fi
        seen[$name]=1
        # The runtime base (distroless/cc) already carries the C++ runtime, and
        # conda's copies are 24 MB between them. Shipping a second one only
        # makes sense if the versions are incompatible, which the smoke test at
        # the end of the build would catch.
        case "$name" in
            libstdc++.so.*|libgcc_s.so.*)
                if [ "${TAXIBENCH_BASE_CXX:-1}" != "0" ]; then
                    continue
                fi
                ;;
        esac
        # Dereference the symlink but keep the name the loader asks for.
        cp -L "$resolved" "$OUT/lib/$name"
        chmod 0644 "$OUT/lib/$name"
        copied=$((copied + 1))
        queue+=("$real")
    done < <(ldd "$current" 2>/dev/null | awk '{for (i = 1; i <= NF; i++) if ($i == "=>") print $(i + 1)}')
done

echo "staged $(basename "$BINARY") and $copied libraries into $OUT"
du -sh "$OUT"
ls -la "$OUT/lib"
