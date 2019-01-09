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

import java.io.IOException;
import java.io.Serializable;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import javax.json.JsonObject;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.talend.components.rabbitmq.service.I18nMessage;
import org.talend.components.rabbitmq.service.RabbitMQService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;

import static org.talend.components.rabbitmq.MessageConst.MESSAGE_CONTENT;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "RabbitMQOutput")
@Processor(name = "Output")
@Documentation("Main class for ActiveMQOutput records processing")
public class Output implements Serializable {

    private final OutputConfiguration configuration;

    private final I18nMessage i18n;

    private final RabbitMQService service;

    private Connection connection;

    private Channel channel;

    public Output(@Option("configuration") final OutputConfiguration configuration, final RabbitMQService service,
            final I18nMessage i18nMessage) {
        this.configuration = configuration;
        this.service = service;
        this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() {
        connection = service.getConnection(configuration.getBasicConfig().getConnection());
        try {
            channel = connection.createChannel();

            switch (configuration.getBasicConfig().getReceiverType()) {
            case QUEUE:
                service.onQueue(channel, configuration.getActionOnQueue(), configuration.getBasicConfig().getQueue());
                channel.queueDeclare(configuration.getBasicConfig().getQueue(), configuration.getBasicConfig().getDurable(),
                        false, configuration.getBasicConfig().getAutoDelete(), null);
                break;
            case EXCHANGE:
                service.onExchange(channel, configuration.getActionOnExchange(), configuration.getBasicConfig().getExchange());
                channel.exchangeDeclare(configuration.getBasicConfig().getExchange(),
                        configuration.getBasicConfig().getExchangeType().getType(), configuration.getBasicConfig().getDurable(),
                        configuration.getBasicConfig().getAutoDelete(), null);
                break;
            }

        } catch (IOException e) {
            throw new IllegalStateException(i18n.errorCreateRabbitMQInstance());
        }

    }

    @ElementListener
    public void onNext(@Input final JsonObject record) {
        try {
            switch (configuration.getBasicConfig().getReceiverType()) {
            case QUEUE:
                channel.basicPublish("", configuration.getBasicConfig().getQueue(), null, getMessage(record).getBytes());
                break;
            case EXCHANGE:
                channel.basicPublish(configuration.getBasicConfig().getExchange(), configuration.getBasicConfig().getRoutingKey(),
                        null, getMessage(record).getBytes());
                break;
            }

        } catch (IOException e) {
            throw new IllegalStateException(i18n.errorCantSendMessage());
        }
    }

    private String getMessage(JsonObject record) {
        return record.getString(MESSAGE_CONTENT);
    }

    @PreDestroy
    public void release() {
        service.closeConnection(connection);

    }
}