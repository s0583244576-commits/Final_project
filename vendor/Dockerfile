# Tessera tlc-publisher mock.
#
# Single image, two roles (selected by `command:` in compose.yml):
#   * data-init     → python /app/download_and_preprocess.py /cache
#   * tlc-publisher → uvicorn app.main:app --host 0.0.0.0 --port 8000
FROM python:3.13-slim

WORKDIR /app

# System deps + CA tools first so every subsequent layer can do HTTPS.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Trust NetFree's root CAs everywhere that needs HTTPS during the build.
# pip uses pip._vendor.certifi internally — patch it directly so SSL works.
# UV_NATIVE_TLS=1 makes uv delegate TLS to OpenSSL (which uses the system store).
COPY netfree-ca-bundle.crt /usr/local/share/ca-certificates/netfree.crt
RUN update-ca-certificates
ENV UV_NATIVE_TLS=1

# uv for fast, reproducible installs.
# --trusted-host bypasses pip's bundled-certifi SSL check for PyPI hosts;
# package integrity is still verified via SHA-256 hashes.
RUN pip install --no-cache-dir \
    --trusted-host pypi.org \
    --trusted-host pypi.python.org \
    --trusted-host files.pythonhosted.org \
    uv

# Install Python deps (cacheable layer — happens before code changes).
COPY pyproject.toml /app/pyproject.toml
RUN uv pip install --system --no-cache -r /app/pyproject.toml

# Patch certifi so requests can verify HTTPS through NetFree's intercepting proxy.
# Step 1: append the known NetFree root CAs (shipped in the repo).
# Step 2: open one no-verify TLS connection to capture any intermediate CAs that
#         NetFree sends in the handshake but that are not in the root bundle.
#         All certs except the leaf are appended; certifi treats them as trust
#         anchors (via load_verify_locations) so OpenSSL skips Key Usage checks.
COPY netfree-ca-bundle.crt /tmp/netfree-ca-bundle.crt
COPY patch_certifi.py /tmp/patch_certifi.py
RUN python3 /tmp/patch_certifi.py

# Then the code.
COPY download_and_preprocess.py /app/download_and_preprocess.py
COPY app /app/app

# No default CMD — compose picks one per service.
