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
package org.talend.components.rabbitmq.service;

import com.rabbitmq.client.Connection;
import org.talend.components.rabbitmq.configuration.BasicConfiguration;
import org.talend.components.rabbitmq.datastore.RabbitMQDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;

import static org.talend.components.rabbitmq.MessageConst.MESSAGE_CONTENT;

@Service
public class ActionService {

    public static final String ACTION_BASIC_HEALTH_CHECK = "ACTION_BASIC_HEALTH_CHECK";

    public static final String DISCOVER_SCHEMA = "discoverSchema";

    @Service
    private RabbitMQService rabbitMQService;

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
    public HealthCheckStatus validateBasicDatastore(@Option final RabbitMQDataStore datastore) {
        if (datastore.getHostname() == null || datastore.getHostname().isEmpty()) {
            throw new IllegalArgumentException(i18n.errorEmptyHostname());
        }
        if (datastore.getPort() == null || datastore.getPort() == 0) {
            throw new IllegalArgumentException(i18n.errorEmptyPort());
        }
        if (datastore.getUserName() == null || datastore.getUserName().isEmpty()) {
            throw new IllegalArgumentException(i18n.errorEmptyUserName());
        }
        if (datastore.getPassword() == null || datastore.getPassword().isEmpty()) {
            throw new IllegalArgumentException(i18n.errorEmptyPassword());
        }

        Connection connection = null;
        try {
            connection = rabbitMQService.getConnection(datastore);
        } catch (IllegalStateException e) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.errorInvalidConnection());
        } finally {
            rabbitMQService.closeConnection(connection);
        }

        return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.successConnection());
    }

}
