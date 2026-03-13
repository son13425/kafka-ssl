FROM ubuntu:22.04
RUN apt-get update && apt-get install -y default-jdk && rm -rf /var/lib/apt/lists/*
COPY generate-certs.sh /generate-certs.sh
RUN chmod +x /generate-certs.sh
ENTRYPOINT ["/bin/bash", "/generate-certs.sh"]
