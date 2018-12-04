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
package org.talend.components.rabbitmq.output;

import lombok.Data;
import org.talend.components.rabbitmq.configuration.BasicConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@GridLayout(value = { @GridLayout.Row({ "basicConfig" }), @GridLayout.Row({ "actionOnExchange" }),
        @GridLayout.Row({ "actionOnQueue" }) }, names = GridLayout.FormType.MAIN)
@GridLayout(value = { @GridLayout.Row({ "basicConfig" }) }, names = GridLayout.FormType.ADVANCED)
@Documentation("Main configuration class for RabbitMQOutput component")
@Data
public class OutputConfiguration implements Serializable {

    @Option
    @Documentation("Common basicConfig")
    private BasicConfiguration basicConfig;

    @Option
    @Documentation("Drop down with actions on RabbitMQ queue")
    @ActiveIf(target = "basicConfig/receiverType", value = "EXCHANGE")
    private ActionOnExchange actionOnExchange = ActionOnExchange.CREATE_EXCHANGE;

    @Option
    @Documentation("Drop down with actions on RabbitMQ exchange")
    @ActiveIf(target = "basicConfig/receiverType", value = "QUEUE")
    private ActionOnQueue actionOnQueue = ActionOnQueue.CREATE_QUEUE;

}
