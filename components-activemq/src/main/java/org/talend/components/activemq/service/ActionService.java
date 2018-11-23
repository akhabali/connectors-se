// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.activemq.service;

import org.talend.components.activemq.configuration.BasicConfiguration;
import org.talend.components.activemq.datastore.JmsDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.record.Schema;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import static org.talend.components.activemq.MessageConst.MESSAGE_CONTENT;

@Service
public class ActionService {

    public static final String ACTION_BASIC_HEALTH_CHECK = "ACTION_BASIC_HEALTH_CHECK";

    public static final String DISCOVER_SCHEMA = "discoverSchema";

    @Service
    private JmsService jmsService;

    @Service
    private I18nMessage i18n;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @DiscoverSchema(DISCOVER_SCHEMA)
    public Schema guessSchema(BasicConfiguration config) {
        return recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(recordBuilderFactory.newEntryBuilder().withName(MESSAGE_CONTENT).withType(Schema.Type.STRING).build())
                .build();
    }

    @HealthCheck(ACTION_BASIC_HEALTH_CHECK)
    public HealthCheckStatus validateBasicDatastore(@Option final JmsDataStore datastore) {
        if (datastore.getFailover()) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.failoverHealthCheckIsNotSupported());
        }

        if (datastore.getStaticDiscovery()) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.staticDiscoveryHealthCheckIsNotSupported());
        }
        Connection connection = null;
        // create ConnectionFactory
        ConnectionFactory connectionFactory = jmsService.createConnectionFactory(datastore);

        try {
            connection = jmsService.getConnection(connectionFactory, datastore);
            connection.start();
            return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.successConnection());
        } catch (JMSException e) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.errorInvalidConnection());
        } finally {
            jmsService.closeConnection(connection);
        }
    }

}
