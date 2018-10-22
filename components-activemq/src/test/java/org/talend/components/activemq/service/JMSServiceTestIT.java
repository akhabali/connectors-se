package org.talend.components.activemq.service;

import org.junit.jupiter.api.Test;
import org.talend.components.activemq.configuration.BasicConfiguration;
import org.talend.components.activemq.configuration.Broker;
import org.talend.components.activemq.configuration.MessageType;
import org.talend.components.activemq.datastore.JmsDataStore;
import org.talend.components.activemq.output.OutputConfiguration;
import org.talend.components.activemq.source.DurableSubscriptionConfiguration;
import org.talend.components.activemq.source.InputMapperConfiguration;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.components.activemq.MessageConst.MESSAGE_CONTENT;
import static org.talend.components.activemq.testutils.JmsTestConstants.*;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.activemq") // component package
public class JMSServiceTestIT {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private JsonBuilderFactory factory;

    @Test
    public void sendAndReceiveJMSMessageQueue() {
        // Send message to QUEUE
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));

        final String outputConfig = configurationByExample().forInstance(getOutputConfiguration()).configured().toQueryString();
        Job.components().component("activemq-output", "ActiveMQ://Output?" + outputConfig).component("emitter", "test://emitter")
                .connections().from("emitter").to("activemq-output").build().run();

        // Receive message from QUEUE
        final String inputConfig = configurationByExample().forInstance(getInputConfiguration()).configured().toQueryString();

        Job.components().component("activemq-input", "ActiveMQ://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("activemq-input").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();
        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void sendAndReceiveJMSMessageDurableTopic() {
        // Subscribe to Durable TOPIC
        InputMapperConfiguration inputConfiguration = getInputConfiguration();
        inputConfiguration.getSubscriptionConfig().setDurableSubscription(true);
        inputConfiguration.getBasicConfig().setMessageType(MessageType.TOPIC);

        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        Job.components().component("activemq-input", "ActiveMQ://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("activemq-input").to("collector").build().run();

        // Send message to TOPIC
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));

        OutputConfiguration outputConfiguration = getOutputConfiguration();
        outputConfiguration.getBasicConfig().setMessageType(MessageType.TOPIC);
        final String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        Job.components().component("activemq-output", "ActiveMQ://Output?" + outputConfig).component("emitter", "test://emitter")
                .connections().from("emitter").to("activemq-output").build().run();

        // Receive message from TOPIC
        Job.components().component("activemq-input", "ActiveMQ://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("activemq-input").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();

        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void sendAndReceiveJMSMessageQueueFailover() {
        // Send message to QUEUE
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));

        OutputConfiguration outputConfiguration = getOutputConfiguration();
        outputConfiguration.getBasicConfig().getConnection().setFailover(true);
        List<Broker> brokers = getBrokers();
        outputConfiguration.getBasicConfig().getConnection().setBrokers(brokers);

        final String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        Job.components().component("activemq-output", "ActiveMQ://Output?" + outputConfig).component("emitter", "test://emitter")
                .connections().from("emitter").to("activemq-output").build().run();

        // Receive message from QUEUE
        InputMapperConfiguration inputConfiguration = getInputConfiguration();
        inputConfiguration.getBasicConfig().getConnection().setBrokers(brokers);
        outputConfiguration.getBasicConfig().getConnection().setFailover(true);

        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        Job.components().component("activemq-input", "ActiveMQ://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("activemq-input").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();
        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void sendAndReceiveJMSMessageDurableTopicFailover() {
        List<Broker> brokers = getBrokers();

        // Subscribe to Durable TOPIC
        InputMapperConfiguration inputConfiguration = getInputConfiguration();
        inputConfiguration.getBasicConfig().getConnection().setBrokers(brokers);
        inputConfiguration.getBasicConfig().getConnection().setFailover(true);
        inputConfiguration.getSubscriptionConfig().setDurableSubscription(true);
        inputConfiguration.getBasicConfig().setMessageType(MessageType.TOPIC);

        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        Job.components().component("activemq-input", "ActiveMQ://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("activemq-input").to("collector").build().run();

        // Send message to TOPIC
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));

        OutputConfiguration outputConfiguration = getOutputConfiguration();
        outputConfiguration.getBasicConfig().getConnection().setFailover(true);

        outputConfiguration.getBasicConfig().getConnection().setBrokers(brokers);
        outputConfiguration.getBasicConfig().setMessageType(MessageType.TOPIC);

        final String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        Job.components().component("activemq-output", "ActiveMQ://Output?" + outputConfig).component("emitter", "test://emitter")
                .connections().from("emitter").to("activemq-output").build().run();

        // Receive message from TOPIC
        Job.components().component("activemq-input", "ActiveMQ://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("activemq-input").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();

        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void sendAndReceiveJMSMessageQueueStaticDiscovery() {
        // Send message to QUEUE
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));

        OutputConfiguration outputConfiguration = getOutputConfiguration();
        outputConfiguration.getBasicConfig().getConnection().setStaticDiscovery(true);
        outputConfiguration.getBasicConfig().getConnection().setBrokers(getBrokers());
        final String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        Job.components().component("activemq-output", "ActiveMQ://Output?" + outputConfig).component("emitter", "test://emitter")
                .connections().from("emitter").to("activemq-output").build().run();

        // Receive message from QUEUE
        InputMapperConfiguration inputConfiguration = getInputConfiguration();
        inputConfiguration.getBasicConfig().getConnection().setBrokers(getBrokers());
        outputConfiguration.getBasicConfig().getConnection().setStaticDiscovery(true);

        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        Job.components().component("activemq-input", "ActiveMQ://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("activemq-input").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();
        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void sendAndReceiveJMSMessageDurableTopicStaticDiscovery() {

        // Subscribe to Durable TOPIC
        InputMapperConfiguration inputConfiguration = getInputConfiguration();
        inputConfiguration.getBasicConfig().getConnection().setBrokers(getBrokers());
        inputConfiguration.getBasicConfig().getConnection().setStaticDiscovery(true);
        inputConfiguration.getSubscriptionConfig().setDurableSubscription(true);
        inputConfiguration.getBasicConfig().setMessageType(MessageType.TOPIC);

        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        Job.components().component("activemq-input", "ActiveMQ://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("activemq-input").to("collector").build().run();

        // Send message to TOPIC
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));

        OutputConfiguration outputConfiguration = getOutputConfiguration();
        outputConfiguration.getBasicConfig().getConnection().setStaticDiscovery(true);

        outputConfiguration.getBasicConfig().getConnection().setBrokers(getBrokers());
        outputConfiguration.getBasicConfig().setMessageType(MessageType.TOPIC);

        final String outputConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        Job.components().component("activemq-output", "ActiveMQ://Output?" + outputConfig).component("emitter", "test://emitter")
                .connections().from("emitter").to("activemq-output").build().run();

        // Receive message from TOPIC
        Job.components().component("activemq-input", "ActiveMQ://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("activemq-input").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();

        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");
    }

    @Test
    public void emptyUrlTest() {
        InputMapperConfiguration inputConfiguration = getInputConfiguration();
        inputConfiguration.getBasicConfig().getConnection().setHost("");
        final String inputConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();

        assertThrows(IllegalStateException.class,
                () -> Job.components().component("activemq-input", "ActiveMQ://Input?" + inputConfig)
                        .component("collector", "test://collector").connections().from("activemq-input").to("collector").build()
                        .run());
    }

    @Test
    public void testMessageSelector() {
        // Send Not Persistent message
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));
        final String outputConfig = configurationByExample().forInstance(getOutputConfiguration()).configured().toQueryString();
        Job.components().component("activemq-output", "ActiveMQ://Output?" + outputConfig).component("emitter", "test://emitter")
                .connections().from("emitter").to("activemq-output").build().run();

        // Send Persistent message
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE2).build()));
        OutputConfiguration outputConfiguration = getOutputConfiguration();
        outputConfiguration.setDeliveryMode(OutputConfiguration.DeliveryMode.PERSISTENT);
        final String outputConfig2 = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        Job.components().component("activemq-output", "ActiveMQ://Output?" + outputConfig2).component("emitter", "test://emitter")
                .connections().from("emitter").to("activemq-output").build().run();

        // Add filter query to receive only PERSISTENT message
        InputMapperConfiguration inputMapperConfiguration = getInputConfiguration();
        inputMapperConfiguration.setMessageSelector("JMSDeliveryMode = 'PERSISTENT'");
        final String inputConfig = configurationByExample().forInstance(inputMapperConfiguration).configured().toQueryString();

        Job.components().component("activemq-input", "ActiveMQ://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("activemq-input").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res.stream().findFirst();
        assertTrue(optional.isPresent(), "Message was not received");
        assertEquals(TEST_MESSAGE2, ((JsonObject) optional.get()).getString((MESSAGE_CONTENT)),
                "Sent and received messages should be equal");

    }

    @Test
    public void testMaximumMessages() {
        // Send message to QUEUE
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add(MESSAGE_CONTENT, TEST_MESSAGE).build()));

        final String outputConfig2 = configurationByExample().forInstance(getOutputConfiguration()).configured().toQueryString();
        Job.components().component("activemq-output", "ActiveMQ://Output?" + outputConfig2).component("emitter", "test://emitter")
                .connections().from("emitter").to("activemq-output").build().run();

        // Forbid message receiving
        InputMapperConfiguration inputMapperConfiguration = getInputConfiguration();
        inputMapperConfiguration.setMaximumMessages(NO_MESSAGES);
        final String inputConfig = configurationByExample().forInstance(inputMapperConfiguration).configured().toQueryString();

        Job.components().component("activemq-input", "ActiveMQ://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("activemq-input").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertTrue(res.isEmpty(), "Component should not process data");

        // Allow message receiving
        InputMapperConfiguration inputMapperConfiguration2 = getInputConfiguration();
        inputMapperConfiguration.setMaximumMessages(TEN_MESSAGES);
        final String inputConfig2 = configurationByExample().forInstance(inputMapperConfiguration2).configured().toQueryString();

        Job.components().component("activemq-input", "ActiveMQ://Input?" + inputConfig2)
                .component("collector", "test://collector").connections().from("activemq-input").to("collector").build().run();

        final List<JsonObject> res2 = componentsHandler.getCollectedData(JsonObject.class);
        Optional optional = res2.stream().findFirst();
        assertTrue(optional.isPresent(), "Message was not received");

    }

    private OutputConfiguration getOutputConfiguration() {
        OutputConfiguration configuration = new OutputConfiguration();
        BasicConfiguration basicConfiguration = new BasicConfiguration();
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setHost(LOCALHOST);
        dataStore.setPort(PORT);
        dataStore.setSSL(true);
        basicConfiguration.setDestination(DESTINATION);
        basicConfiguration.setMessageType(MessageType.QUEUE);
        basicConfiguration.setConnection(dataStore);
        configuration.setBasicConfig(basicConfiguration);
        return configuration;
    }

    private InputMapperConfiguration getInputConfiguration() {
        InputMapperConfiguration configuration = new InputMapperConfiguration();
        BasicConfiguration basicConfiguration = new BasicConfiguration();
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setHost(LOCALHOST);
        dataStore.setPort(PORT);
        dataStore.setSSL(true);
        basicConfiguration.setDestination(DESTINATION);
        basicConfiguration.setMessageType(MessageType.QUEUE);
        DurableSubscriptionConfiguration subsConfig = new DurableSubscriptionConfiguration();
        subsConfig.setDurableSubscription(DURABLE_SUBSCRIPTION);
        subsConfig.setClientId(CLIENT_ID);
        subsConfig.setSubscriberName(SUBSCRIBER_NAME);
        basicConfiguration.setConnection(dataStore);
        configuration.setSubscriptionConfig(subsConfig);
        configuration.setBasicConfig(basicConfiguration);
        configuration.setMaximumMessages(TEN_MESSAGES);
        configuration.setTimeout(TIMEOUT);
        return configuration;
    }

    private List<Broker> getBrokers() {
        List<Broker> brokers = new ArrayList<>();
        Broker broker1 = new Broker();
        Broker broker2 = new Broker();
        broker1.setHost("test");
        broker1.setPort("1234");
        broker2.setHost(LOCALHOST);
        broker2.setPort(PORT);
        brokers.add(broker1);
        brokers.add(broker2);
        return brokers;
    }

}
