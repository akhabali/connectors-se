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
package org.talend.components.rabbitmq.configuration;

import lombok.Data;
import org.talend.components.rabbitmq.datastore.RabbitMQDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

import static org.talend.components.rabbitmq.service.ActionService.DISCOVER_SCHEMA;

@DataSet("RabbitMQDataSet")
@Data
@GridLayout(value = { @GridLayout.Row({ "connection" }), @GridLayout.Row({ "receiverType" }), @GridLayout.Row({ "queue" }),
        @GridLayout.Row({ "exchangeType" }), @GridLayout.Row({ "exchange" }), @GridLayout.Row({ "routingKey" }),
        @GridLayout.Row({ "schema" }) }, names = GridLayout.FormType.MAIN)
@GridLayout(value = { @GridLayout.Row({ "durable" }), @GridLayout.Row({ "autoDelete" }) }, names = GridLayout.FormType.ADVANCED)
public class BasicConfiguration implements Serializable {

    @Option
    @Documentation("RabbitMQ connection information")
    private RabbitMQDataStore connection;

    @Option
    @Documentation("Message Type list")
    private ReceiverType receiverType = ReceiverType.QUEUE;

    @Option
    @ActiveIf(target = "receiverType", value = "QUEUE")
    @Documentation("QUEUE Name")
    private String queue;

    @Option
    @ActiveIf(target = "receiverType", value = "EXCHANGE")
    @Documentation("EXCHANGE Type")
    private ExchangeType exchangeType = ExchangeType.FANOUT;

    @Option
    @ActiveIf(target = "receiverType", value = "EXCHANGE")
    @Documentation("EXCHANGE Name")
    private String exchange;

    @Option
    @ActiveIfs(value = { @ActiveIf(target = "exchangeType", value = { "DIRECT", "TOPIC" }),
            @ActiveIf(target = "receiverType", value = { "EXCHANGE" }) }, operator = ActiveIfs.Operator.AND)
    @Documentation("Routing key")
    private String routingKey;

    @Option
    @Documentation("true if we are declaring a durable queue (the queue will survive a server restart)")
    private Boolean durable = false;

    @Option
    @Documentation("true if we are declaring an autodelete queue (server will delete it when no longer in use)")
    private Boolean autoDelete = false;

    @Option
    @Structure(type = Structure.Type.OUT, discoverSchema = DISCOVER_SCHEMA)
    @Documentation("Describes record structure")
    private List<String> schema;

    public String getRoutingKey() {
        return (ExchangeType.DIRECT == exchangeType || ExchangeType.TOPIC == exchangeType)
                && ReceiverType.EXCHANGE == receiverType ? routingKey : "";
    }
}
