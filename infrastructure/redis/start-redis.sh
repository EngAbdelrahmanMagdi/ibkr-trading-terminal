#!/bin/sh
# Builds the ACL file from the mounted Docker secret, then starts Redis.
#   - the default user is disabled (unauthenticated connections are rejected)
#   - the application user may use normal data commands but no @dangerous commands
#     (e.g. FLUSHALL, FLUSHDB, KEYS, CONFIG, DEBUG, SHUTDOWN)
# The password is stored in the ACL file only as a SHA-256 hash, on an in-memory tmpfs.
set -eu

secret_file=/run/secrets/redis_app_password
if [ ! -s "$secret_file" ]; then
  echo "redis: required secret file $secret_file is missing or empty" >&2
  exit 1
fi

app_user="${REDIS_APP_USER:-app}"
password_hash="$(tr -d '\r\n' < "$secret_file" | sha256sum | cut -d' ' -f1)"

umask 077
acl_file=/tmp/users.acl
{
  echo "user default off"
  echo "user ${app_user} on #${password_hash} ~* &* +@all -@dangerous"
} > "$acl_file"

exec redis-server /usr/local/etc/redis/redis.conf \
  --aclfile "$acl_file" \
  --maxmemory "${REDIS_MAXMEMORY:-256mb}"
