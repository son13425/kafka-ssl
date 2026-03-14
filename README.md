# kafka-ssl
Кафка: настройка защищённого соединения и управление доступом

## Описание проекта
Цель проекта: настроить защищённое SSL-соединение для кластера Apache Kafka из трёх брокеров с использованием Docker Compose, создать новый топик и протестировать отправку и получение зашифрованных сообщений.

## Технологии

- Python
- Kafka
- FastApi
- Vue3
- Vite
- Docker
- Nginx

## Запуск проекта

- клонируйте репозиторий на локальную машину и перейдите в созданную папку:

''' git clone git@github.com:son13425/kafka-project.git'''

- установите Docker (Зайдите на официальный сайт https://www.docker.com/products/docker-desktop и скачайте установочный файл Docker Desktop для вашей операционной системы)

- проверьте, что Docker работает:

'''sudo systemctl status docker'''

- создайте файлы /infra/.env.kafka, /backend/.env.backend, /frontend/.env.frontend по шаблонам

- выполните команду в директории /infra:

'''sudo docker-compose up -d --build'''

- приложение разворачивается локально и становится доступным по адресам:

  - http://localhost - фронт приложения для запуска/остановки передачи сообщений в Kafka;

  - http://localhost/api/openapi# - API сервера приложения;

  - http://localhost/kafka-ui/ui/clusters/kraft-cluster/ - UI-кафка;

  - http://localhost/schema-registry/ - UI-ShemaRegistry;


При запуске проекта сервис генерации сертификатов проверяет их наличие в заданной папке и если сертификатов нет - генерирует их и завершает свою работу.

Сервис ACL (одноразовый init-сервис) после старта Kafka создаёт топики (topic-1, topic-2), если их нет, и применяет ACL-права для пользователей (admin, producer_app, consumer_app), чтобы ограничить доступ к операциям чтения/записи по принципу least privilege.

topic-1: Доступен как для продюсеров, так и для консьюмеров.
topic-2:
- Продюсеры могут отправлять сообщения.
- Консьюмеры не имеют доступа к чтению данных.

Для запуска генерации сообщений, отправляемых в Кафка, кликните по кнопке на фронте приложения.

После этого можно посмотреть движение сообщений в сервисе Кафка в UI-кафка.

Для проверки запрета на чтение для консьюмеров из topic-2, в терминале выполните команду:

'''docker exec -it kafka-kraft-1 bash -lc '
cat > /tmp/consumer.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="consumer_app" password="consumer-secret";
ssl.truststore.location=/opt/bitnami/kafka/config/certs/kafka.truststore.jks
ssl.truststore.password=password
ssl.endpoint.identification.algorithm=
EOF

/opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-kraft-1:9098 \
  --consumer.config /tmp/consumer.properties \
  --topic topic-2 \
  --from-beginning \
  --timeout-ms 5000
''''

Ожидаемый вывод: Not authorized to access topics: [topic-2]


## Автор
[Оксана Широкова](https://github.com/son13425)

## Лицензия
Сценарии и документация в этом проекте выпущены под лицензией [MIT](https://github.com/son13425/kafka-ssl/blob/main/LICENSE)
