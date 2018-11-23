FROM bitnami/rabbitmq:latest
RUN mkdir -p /bitnami/rabbitmq/certificates
COPY /certificates /bitnami/rabbitmq/certificates/
#COPY server_certificate.pem /bitnami/rabbitmq/conf/
#COPY server_key.pem /bitnami/rabbitmq/conf/
RUN rm -f /bitnami/rabbitmq/conf/rabbitmq.config
COPY conf/rabbitmq.config /bitnami/rabbitmq/conf/