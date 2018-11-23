package org.talend.components.rabbitmq.service;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;
import org.talend.components.rabbitmq.configuration.BasicConfiguration;
import org.talend.components.rabbitmq.configuration.ExchangeType;
import org.talend.components.rabbitmq.configuration.ReceiverType;
import org.talend.components.rabbitmq.datastore.RabbitMQDataStore;
import org.talend.components.rabbitmq.output.OutputConfiguration;
import org.talend.components.rabbitmq.source.InputMapperConfiguration;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.components.rabbitmq.MessageConst.MESSAGE_CONTENT;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.*;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.rabbitmq") // component package
public class RabbitMQServiceTestIT {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private JsonBuilderFactory factory;

    @Test
    public void sendAndReceiveQueueMessage() {
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));

        final String outputConfig = configurationByExample().forInstance(getOutputConfiguration()).configured().toQueryString();
        Job.components().component("rabbitmq-output", "RabbitMQ://Output?" + outputConfig).component("emitter", "test://emitter")
                .connections().from("emitter").to("rabbitmq-output").build().run();

        // Receive message from QUEUE_NAME
        final String inputConfig = configurationByExample().forInstance(getInputConfiguration()).configured().toQueryString();

        Job.components().component("rabbitmq-output", "RabbitMQ://Input?" + inputConfig)
                .component("collector", "test://collector").connections().from("rabbitmq-output").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();
        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void receiveFanoutMessage() {
        Runnable runnable = () -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            OutputConfiguration outputConfiguration = getOutputConfiguration();
            outputConfiguration.getBasicConfig().setReceiverType(ReceiverType.EXCHANGE);
            sendMessageToExchange(outputConfiguration.getBasicConfig().getConnection(), BuiltinExchangeType.FANOUT,
                    FANOUT_EXCHANGE_NAME);
        };

        Thread thread = new Thread(runnable);
        thread.start();

        InputMapperConfiguration inputConfiguration = getInputConfiguration();
        inputConfiguration.getBasicConfig().setReceiverType(ReceiverType.EXCHANGE);
        inputConfiguration.getBasicConfig().setExchangeType(ExchangeType.FANOUT);
        inputConfiguration.getBasicConfig().setExchange(FANOUT_EXCHANGE_NAME);

        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        Job.components().component("rabbitmq-output", "RabbitMQ://Input?" + inputConfig)
                .component("collector", "test://collector").connections().from("rabbitmq-output").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();

        assertTrue(optional.isPresent(), "Message was not received");

        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void receiveDirectMessage() {
        Runnable runnable = () -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            OutputConfiguration outputConfiguration = getOutputConfiguration();
            outputConfiguration.getBasicConfig().setReceiverType(ReceiverType.EXCHANGE);
            sendMessageToExchange(outputConfiguration.getBasicConfig().getConnection(), BuiltinExchangeType.DIRECT,
                    DIRECT_EXCHANGE_NAME);
        };

        Thread thread = new Thread(runnable);
        thread.start();

        InputMapperConfiguration inputConfiguration = getInputConfiguration();
        inputConfiguration.getBasicConfig().setReceiverType(ReceiverType.EXCHANGE);
        inputConfiguration.getBasicConfig().setExchangeType(ExchangeType.DIRECT);
        inputConfiguration.getBasicConfig().setExchange(DIRECT_EXCHANGE_NAME);

        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        Job.components().component("rabbitmq-output", "RabbitMQ://Input?" + inputConfig)
                .component("collector", "test://collector").connections().from("rabbitmq-output").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();

        assertTrue(optional.isPresent(), "Message was not received");

        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    private void sendMessageToExchange(RabbitMQDataStore store, BuiltinExchangeType exchangeType, String exchangeName) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(store.getHostname());
        factory.setUsername(store.getUserName());
        factory.setPassword(store.getPassword());
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(exchangeName, exchangeType);
            channel.basicPublish(exchangeName, "", null, TEST_MESSAGE.getBytes(StandardCharsets.UTF_8));
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    private OutputConfiguration getOutputConfiguration() {
        OutputConfiguration configuration = new OutputConfiguration();
        BasicConfiguration basicConfiguration = getBasicConfiguration();
        configuration.setBasicConfig(basicConfiguration);
        return configuration;
    }

    private BasicConfiguration getBasicConfiguration() {
        BasicConfiguration basicConfiguration = new BasicConfiguration();
        RabbitMQDataStore dataStore = new RabbitMQDataStore();
        dataStore.setHostname(HOSTNAME);
        dataStore.setPort(Integer.valueOf(PORT));
        dataStore.setUserName(USER_NAME);
        dataStore.setPassword(PASSWORD);
        dataStore.setTLS(true);
        basicConfiguration.setQueue(QUEUE_NAME);
        basicConfiguration.setExchange(FANOUT_EXCHANGE_NAME);
        basicConfiguration.setReceiverType(ReceiverType.QUEUE);
        basicConfiguration.setExchangeType(ExchangeType.FANOUT);
        basicConfiguration.setRoutingKey("");
        basicConfiguration.setConnection(dataStore);
        return basicConfiguration;
    }

    private InputMapperConfiguration getInputConfiguration() {
        InputMapperConfiguration configuration = new InputMapperConfiguration();
        BasicConfiguration basicConfiguration = getBasicConfiguration();
        configuration.setBasicConfig(basicConfiguration);
        configuration.setMaximumMessages(MAXIMUM_MESSAGES);
        return configuration;
    }

}
