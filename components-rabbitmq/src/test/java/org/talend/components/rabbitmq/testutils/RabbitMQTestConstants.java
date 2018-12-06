package org.talend.components.rabbitmq.testutils;

public class RabbitMQTestConstants {

    public static final String HOSTNAME = "192.168.99.100";

    public static final String PORT = System.getProperty("amqp.port") != null ? System.getProperty("amqp.port") : "32787";

    public static final String USER_NAME = "user";

    public static final String PASSWORD = "bitnami";

    public static final String QUEUE_NAME = "test";

    public static final String DIRECT_EXCHANGE_NAME = "directex";

    public static final String FANOUT_EXCHANGE_NAME = "fanoutex";

    public static final String TEST_MESSAGE = "hello world";

    public static final int MAXIMUM_MESSAGES = 1;

}
