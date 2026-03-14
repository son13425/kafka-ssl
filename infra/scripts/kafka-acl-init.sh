#!/bin/bash
set -euo pipefail

BOOTSTRAP="kafka-kraft-1:9098"
TRUSTSTORE="/certs-source/client/client.truststore.jks"

echo "Check certs..."
ls -laR /certs-source || true

if [ ! -f "$TRUSTSTORE" ]; then
  echo "ERROR: truststore not found at $TRUSTSTORE"
  ls -la /certs-source || true
  exit 1
fi

cat >/tmp/admin.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";
ssl.truststore.location=${TRUSTSTORE}
ssl.truststore.password=password
ssl.endpoint.identification.algorithm=
EOF

echo "Waiting Kafka..."
until /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --command-config /tmp/admin.properties \
  --list >/dev/null 2>&1; do
  sleep 3
done

echo "Create topics if not exists..."
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --command-config /tmp/admin.properties \
  --create --if-not-exists --topic topic-1 --partitions 3 --replication-factor 3

/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --command-config /tmp/admin.properties \
  --create --if-not-exists --topic topic-2 --partitions 3 --replication-factor 3

echo "Apply ACL for producer_app..."
/opt/bitnami/kafka/bin/kafka-acls.sh --bootstrap-server "${BOOTSTRAP}" --command-config /tmp/admin.properties \
  --add --allow-principal User:producer_app --operation WRITE --operation DESCRIBE --topic topic-1 || true

/opt/bitnami/kafka/bin/kafka-acls.sh --bootstrap-server "${BOOTSTRAP}" --command-config /tmp/admin.properties \
  --add --allow-principal User:producer_app --operation WRITE --operation DESCRIBE --topic topic-2 || true

/opt/bitnami/kafka/bin/kafka-acls.sh --bootstrap-server "${BOOTSTRAP}" --command-config /tmp/admin.properties \
  --add --allow-principal User:producer_app --operation IDEMPOTENT_WRITE --cluster || true

echo "Apply ACL for consumer_app (topic-1 only)..."
/opt/bitnami/kafka/bin/kafka-acls.sh --bootstrap-server "${BOOTSTRAP}" --command-config /tmp/admin.properties \
  --add --allow-principal User:consumer_app --operation READ --operation DESCRIBE --topic topic-1 || true

/opt/bitnami/kafka/bin/kafka-acls.sh --bootstrap-server "${BOOTSTRAP}" --command-config /tmp/admin.properties \
  --add --allow-principal User:consumer_app --operation READ --group single_consumer_group || true

echo "Done."
