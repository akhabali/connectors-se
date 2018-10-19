package org.talend.components.activemq.service;

import org.junit.jupiter.api.Test;
import org.talend.components.activemq.configuration.Broker;
import org.talend.components.activemq.datastore.JmsDataStore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.talend.components.activemq.testutils.JmsTestConstants.LOCALHOST;
import static org.talend.components.activemq.testutils.JmsTestConstants.PORT;

@WithComponents("org.talend.components.activemq")
class ActionServiceTestIT {

    @Service
    private ActionService actionService;

    @Test
    public void testJMSSuccessfulConnection() {
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setHost(LOCALHOST);
        dataStore.setPort(PORT);
        dataStore.setSSL(true);
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);

        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }

    @Test
    public void testJMSNotSuccessfulConnection() {
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setHost("124");
        dataStore.setPort(PORT);
        dataStore.setSSL(true);
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);

        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @Test
    public void testJMSSuccessfulConnectionStaticDiscovery() {
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setStaticDiscovery(true);
        dataStore.setSSL(true);
        List<Broker> brokerList = new ArrayList<>();
        Broker broker1 = new Broker();
        broker1.setHost("test");
        broker1.setPort("1234");
        Broker broker2 = new Broker();
        broker2.setHost(LOCALHOST);
        broker2.setPort(PORT);
        brokerList.add(broker1);
        brokerList.add(broker2);
        dataStore.setBrokers(brokerList);
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);
        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }

    @Test
    public void testJMSNotSuccessfulConnectionStaticDiscovery() {
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setStaticDiscoveryURIParameters("?timeout=3000");
        dataStore.setStaticDiscovery(true);
        dataStore.setSSL(true);
        List<Broker> brokerList = new ArrayList<>();
        Broker broker1 = new Broker();
        broker1.setHost("test");
        broker1.setPort("1234");
        brokerList.add(broker1);
        dataStore.setBrokers(brokerList);
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @Test
    public void testJMSSuccessfulConnectionFailover() {
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setFailover(true);
        dataStore.setSSL(true);
        List<Broker> brokerList = new ArrayList<>();
        Broker broker1 = new Broker();
        broker1.setHost("test");
        broker1.setPort("1234");
        Broker broker2 = new Broker();
        broker2.setHost(LOCALHOST);
        broker2.setPort(PORT);
        brokerList.add(broker1);
        brokerList.add(broker2);
        dataStore.setBrokers(brokerList);
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);

        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }

    @Test
    public void testJMSNotSuccessfulConnectionFailover() {
        JmsDataStore dataStore = new JmsDataStore();
        dataStore.setFailover(true);
        dataStore.setSSL(true);
        List<Broker> brokerList = new ArrayList<>();
        Broker broker1 = new Broker();
        broker1.setHost("test");
        broker1.setPort("1234");
        brokerList.add(broker1);
        dataStore.setBrokers(brokerList);
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);

        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

}
