package org.talend.components.activemq.testutils;

public class JmsTestConstants {

    public static final String LOCALHOST = "localhost";

    public static final String PORT;

    public static final String DESTINATION = "test";

    public static final String TEST_MESSAGE = "hello world";

    public static final String TEST_MESSAGE2 = "test message";

    public static final boolean DURABLE_SUBSCRIPTION = false;

    public static final String CLIENT_ID = "testClientId";

    public static final String SUBSCRIBER_NAME = "test";

    public static final int TIMEOUT = 1;

    public static final int NO_MESSAGES = 0;

    public static final int TEN_MESSAGES = 10;

    static {
        String systemPropertyPort = System.getProperty("activemq.port");
        PORT = systemPropertyPort != null ? systemPropertyPort : "61617";
    }
}
