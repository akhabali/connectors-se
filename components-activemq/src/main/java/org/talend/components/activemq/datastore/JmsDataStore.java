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
package org.talend.components.activemq.datastore;

import lombok.Data;
import org.talend.components.activemq.configuration.Broker;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

import static org.talend.components.activemq.service.ActionService.ACTION_BASIC_HEALTH_CHECK;
import static org.talend.sdk.component.api.configuration.condition.ActiveIfs.Operator.AND;
import static org.talend.sdk.component.api.configuration.condition.ActiveIfs.Operator.OR;

@Data
@GridLayout({ @GridLayout.Row({ "failover" }), @GridLayout.Row({ "staticDiscovery" }), @GridLayout.Row({ "SSL" }),
        @GridLayout.Row({ "failoverURIParameters" }), @GridLayout.Row({ "staticDiscoveryURIParameters" }),
        @GridLayout.Row({ "brokers" }), @GridLayout.Row({ "host", "port" }), @GridLayout.Row("userIdentity"),
        @GridLayout.Row({ "userName", "password" }), @GridLayout.Row({ "transacted" }) })
@DataStore("basic")
@Checkable(ACTION_BASIC_HEALTH_CHECK)
@Documentation("A connection to a data base")
public class JmsDataStore implements Serializable {

    @Option
    @ActiveIf(target = "staticDiscovery", value = "false")
    @Documentation("Failover mode checkbox")
    private Boolean failover = false;

    @Option
    @ActiveIf(target = "failover", value = "false")
    @Documentation("Discovery mode checkbox")
    private Boolean staticDiscovery = false;

    @Option
    @Documentation("SSL mode checkbox")
    private Boolean SSL = false;

    @Option
    @ActiveIf(target = "failover", value = "true")
    @Documentation("Failover mode parameters")
    private String failoverURIParameters = "?randomize=false";

    @Option
    @ActiveIf(target = "staticDiscovery", value = "true")
    @Documentation("Static discovery mode parameters")
    private String staticDiscoveryURIParameters = "?transport.maxReconnectDelay=5000&transport.useExponentialBackOff=false";

    @Option
    @ActiveIfs(value = { @ActiveIf(target = "failover", value = "true"),
            @ActiveIf(target = "staticDiscovery", value = "true") }, operator = OR)
    @Documentation("List of brokers for static discovery or failover")
    private List<Broker> brokers;

    @Option
    @ActiveIfs(value = { @ActiveIf(target = "failover", value = "false"),
            @ActiveIf(target = "staticDiscovery", value = "false") }, operator = AND)
    @Documentation("Input for JMS server Broker")
    private String host;

    @Option
    @ActiveIfs(value = { @ActiveIf(target = "failover", value = "false"),
            @ActiveIf(target = "staticDiscovery", value = "false") }, operator = AND)
    @Documentation("Input for JMS server Broker")
    private String port;

    @Option
    @Documentation("Checkbox for User login/password checking")
    private boolean userIdentity = false;

    @Option
    @Documentation("Input for User Name")
    @ActiveIf(target = "userIdentity", value = "true")
    private String userName;

    @Option
    @Credential
    @Documentation("Input for password")
    @ActiveIf(target = "userIdentity", value = "true")
    private String password;

    @Option
    @Documentation("Checkbox for transactions support")
    private Boolean transacted = false;

}
