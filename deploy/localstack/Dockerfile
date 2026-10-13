FROM localstack/localstack:4.13.1
COPY deploy/localstack/10-create-queues.sh /etc/localstack/init/ready.d/10-create-queues.sh
RUN sed -i 's/\r$//' /etc/localstack/init/ready.d/10-create-queues.sh \
	&& chmod 755 /etc/localstack/init/ready.d/10-create-queues.sh
