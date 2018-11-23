package org.talend.components.activemq.service;

import org.junit.jupiter.api.Test;
import org.talend.components.activemq.datastore.JmsDataStore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;


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
}
