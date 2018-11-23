package org.talend.components.rabbitmq.service;

import org.junit.jupiter.api.Test;
import org.talend.components.rabbitmq.datastore.RabbitMQDataStore;
import org.talend.components.rabbitmq.testutils.RabbitMQTestConstants;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.jupiter.api.Assertions.assertEquals;

import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.PASSWORD;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.USER_NAME;

@WithComponents("org.talend.components.rabbitmq")
class ActionServiceTestIT {

    @Service
    private ActionService actionService;

    @Test
    public void testSuccessfulConnection() {
        RabbitMQDataStore dataStore = new RabbitMQDataStore();
        dataStore.setHostname(RabbitMQTestConstants.HOSTNAME);
        dataStore.setPort(5671);
        dataStore.setUserName(USER_NAME);
        dataStore.setPassword(PASSWORD);
        dataStore.setTLS(true);
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);

        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }
}
