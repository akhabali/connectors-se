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
package org.talend.components.rabbitmq.datastore;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

import static org.talend.components.rabbitmq.service.ActionService.ACTION_BASIC_HEALTH_CHECK;

@Data
@GridLayout({ @GridLayout.Row({ "hostname", "port" }), @GridLayout.Row({ "userName", "password" }), @GridLayout.Row({ "TLS" }) })
@DataStore("basic")
@Checkable(ACTION_BASIC_HEALTH_CHECK)
@Documentation("A connection to a data base")
public class RabbitMQDataStore implements Serializable {

    @Option
    @Documentation("RabbitMQ server hostname")
    private String hostname;

    @Option
    @Documentation("RabbitMQ server port")
    private Integer port = 5672;

    @Option
    @Documentation("User Name")
    private String userName;

    @Option
    @Credential
    @Documentation("Password")
    private String password;

    @Option
    @Documentation("TLS mode")
    private Boolean TLS = false;
}
