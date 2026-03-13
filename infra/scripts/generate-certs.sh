#!/bin/bash
set -e

apt-get update && apt-get install -y default-jdk

PASSWORD="password"
VALIDITY=365
CA_CN="KafkaCA"
CERT_DIR="/certs"

BROKERS=("kafka-kraft-1" "kafka-kraft-2" "kafka-kraft-3")

mkdir -p "$CERT_DIR/client"

# Если сертификаты уже сгенерированы — пропускаем
if [ -f "$CERT_DIR/ca.keystore.jks" ] && [ -f "$CERT_DIR/ca-cert.pem" ]; then
  echo "=== Сертификаты уже существуют, пропускаем генерацию ==="
  ls -laR "$CERT_DIR"
  exit 0
fi

echo "=== Генерация CA ==="
keytool -genkeypair \
  -alias ca \
  -keyalg RSA \
  -keysize 2048 \
  -keystore "$CERT_DIR/ca.keystore.jks" \
  -storepass "$PASSWORD" \
  -keypass "$PASSWORD" \
  -dname "CN=$CA_CN" \
  -ext bc=ca:true \
  -validity $VALIDITY

keytool -exportcert \
  -alias ca \
  -keystore "$CERT_DIR/ca.keystore.jks" \
  -storepass "$PASSWORD" \
  -file "$CERT_DIR/ca-cert.pem" \
  -rfc

for BROKER in "${BROKERS[@]}"; do
  echo "=== Генерация для $BROKER ==="
  BROKER_DIR="$CERT_DIR/$BROKER"
  mkdir -p "$BROKER_DIR"

  # 1. Создаём keystore брокера
  keytool -genkeypair \
    -alias "$BROKER" \
    -keyalg RSA \
    -keysize 2048 \
    -keystore "$BROKER_DIR/kafka.keystore.jks" \
    -storepass "$PASSWORD" \
    -keypass "$PASSWORD" \
    -dname "CN=$BROKER" \
    -ext "SAN=DNS:$BROKER,DNS:localhost" \
    -validity $VALIDITY

  # 2. Создаём CSR
  keytool -certreq \
    -alias "$BROKER" \
    -keystore "$BROKER_DIR/kafka.keystore.jks" \
    -storepass "$PASSWORD" \
    -file "$BROKER_DIR/server.csr"

  # 3. Подписываем CSR нашим CA
  keytool -gencert \
    -alias ca \
    -keystore "$CERT_DIR/ca.keystore.jks" \
    -storepass "$PASSWORD" \
    -infile "$BROKER_DIR/server.csr" \
    -outfile "$BROKER_DIR/server-signed.pem" \
    -ext "SAN=DNS:$BROKER,DNS:localhost" \
    -rfc \
    -validity $VALIDITY

  # 4. Импортируем CA-сертификат в keystore брокера
  keytool -importcert \
    -alias ca \
    -keystore "$BROKER_DIR/kafka.keystore.jks" \
    -storepass "$PASSWORD" \
    -file "$CERT_DIR/ca-cert.pem" \
    -noprompt

  # 5. Импортируем подписанный сертификат
  keytool -importcert \
    -alias "$BROKER" \
    -keystore "$BROKER_DIR/kafka.keystore.jks" \
    -storepass "$PASSWORD" \
    -file "$BROKER_DIR/server-signed.pem" \
    -noprompt

  # 6. Создаём truststore брокера с CA-сертификатом
  keytool -importcert \
    -alias ca \
    -keystore "$BROKER_DIR/kafka.truststore.jks" \
    -storepass "$PASSWORD" \
    -file "$CERT_DIR/ca-cert.pem" \
    -noprompt
done

# Truststore для клиентов (backend, schema-registry, kafka-ui и т.д.)
keytool -importcert \
  -alias ca \
  -keystore "$CERT_DIR/client/client.truststore.jks" \
  -storepass "$PASSWORD" \
  -file "$CERT_DIR/ca-cert.pem" \
  -noprompt

echo "=== Готово! Сертификаты в $CERT_DIR ==="
ls -laR "$CERT_DIR"
