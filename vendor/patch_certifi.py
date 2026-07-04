"""Patch certifi so requests can verify HTTPS through NetFree's proxy.

Two steps:
1. Append the known NetFree root CAs (shipped in the repo bundle).
2. Open one no-verify TLS connection to capture any intermediate CAs that
   NetFree injects into the handshake but that are absent from the root bundle.
   All non-leaf certs are appended; certifi treats them as trust anchors via
   load_verify_locations, so OpenSSL skips the Key Usage extension check.
"""
import ssl
import socket
import certifi

bundle = certifi.where()

# Step 1: root CAs from the pre-shipped bundle.
with open("/tmp/netfree-ca-bundle.crt") as f, open(bundle, "a") as out:
    out.write(f.read())

# Step 2: intermediate CAs — captured live from the NetFree proxy chain.
# Probe multiple hosts so we catch every intermediate NetFree injects per domain.
PROBE_HOSTS = [
    "pypi.org",
    "d37ci6vzurychx.cloudfront.net",
]

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

seen = set()
total = 0
with open(bundle, "ab") as out:
    for host in PROBE_HOSTS:
        try:
            s = socket.create_connection((host, 443), timeout=30)
            ss = ctx.wrap_socket(s, server_hostname=host)
            chain = ss.get_unverified_chain()
            ss.close()
            s.close()
            for cert_der in chain[1:]:  # skip leaf [0]
                if cert_der not in seen:
                    seen.add(cert_der)
                    out.write(ssl.DER_cert_to_PEM_cert(cert_der).encode())
                    total += 1
        except Exception as e:
            print(f"  warn: could not probe {host}: {e}")

print("certifi patched: root CAs + %d unique intermediates" % total)
