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

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface I18nMessage {

    String errorEmptyHostname();

    String errorEmptyPort();

    String errorEmptyUserName();

    String errorEmptyPassword();

    String errorInvalidConnection();

    String errorTLS();

    String errorCreateRabbitMQInstance();

    String errorCantSendMessage();

    String errorCantRemoveExchange();

    String errorCantRemoveQueue();

    String errorCantReceiveMessage();

    String warnConnectionCantBeClosed();

    String successConnection();
}
