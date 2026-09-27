#!/bin/sh
# Generates a per-machine local CA and a server certificate for the IBKR Client Portal Gateway
# (SANs: localhost, host.docker.internal). Run through `make ibkr-certs`, in a container with openssl and
# keytool. Output directory (default /out, i.e. ./secrets/ibkr on the host, which is gitignored):
#   ca.pem                  CA certificate - the only certificate the realtime gateway trusts for IBKR
#   ca.key                  CA private key (kept for re-issuing; never leaves this machine)
#   cpgw.pem, cpgw.key      server certificate and key
#   cpgw.p12, cpgw.jks      the same as PKCS#12 and Java keystores for the CP Gateway
#   cpgw-keystore-password  keystore password
# Existing files are never overwritten unless FORCE=1.
set -eu

out="${1:-/out}"
if [ -e "$out/ca.pem" ] && [ "${FORCE:-}" != "1" ]; then
  echo "ibkr-certs: $out already contains a CA; set FORCE=1 to replace it (the CP Gateway keystore must then be reinstalled)" >&2
  exit 1
fi
umask 077
cd "$out"
rm -f ca.pem ca.key ca.srl cpgw.pem cpgw.key cpgw.csr cpgw.ext cpgw.p12 cpgw.jks cpgw-keystore-password

openssl req -x509 -newkey rsa:3072 -nodes -sha256 -days 3650 \
  -keyout ca.key -out ca.pem -subj "/CN=Trading Terminal local CP Gateway CA" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign"

openssl req -newkey rsa:3072 -nodes -sha256 -keyout cpgw.key -out cpgw.csr -subj "/CN=localhost"
cat > cpgw.ext << 'EOF'
subjectAltName=DNS:localhost,DNS:host.docker.internal
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
EOF
openssl x509 -req -in cpgw.csr -CA ca.pem -CAkey ca.key -CAcreateserial -sha256 -days 397 \
  -extfile cpgw.ext -out cpgw.pem

openssl rand -hex 24 | tr -d '\n' > cpgw-keystore-password
openssl pkcs12 -export -in cpgw.pem -inkey cpgw.key -certfile ca.pem -name cpgw \
  -out cpgw.p12 -passout file:cpgw-keystore-password
keytool -importkeystore -noprompt \
  -srckeystore cpgw.p12 -srcstoretype PKCS12 -srcstorepass:file cpgw-keystore-password \
  -destkeystore cpgw.jks -deststoretype JKS -deststorepass:file cpgw-keystore-password > /dev/null 2>&1

rm -f cpgw.csr cpgw.ext ca.srl
chmod 600 ca.key cpgw.key cpgw.p12 cpgw.jks cpgw-keystore-password
chmod 644 ca.pem cpgw.pem
echo "ibkr-certs: wrote ca.pem, cpgw.pem, cpgw.p12 and cpgw.jks to the secrets directory"
openssl x509 -in cpgw.pem -noout -subject -ext subjectAltName
