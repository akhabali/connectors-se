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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.talend.components.activemq.configuration.Broker;
import org.talend.components.activemq.configuration.MessageType;
import org.talend.components.activemq.datastore.JmsDataStore;
import org.talend.sdk.component.api.service.Service;
import lombok.extern.slf4j.Slf4j;

import java.util.StringJoiner;

@Slf4j
@Service
public class JmsService {

    @Service
    private I18nMessage i18n;

    public Destination getDestination(Session session, String destination, MessageType messageType) throws JMSException {
        return (MessageType.QUEUE == messageType) ? session.createQueue(destination) : session.createTopic(destination);
    }

    public ConnectionFactory createConnectionFactory(JmsDataStore dataStore) {
        String url;
        if (!dataStore.getFailover() && !dataStore.getStaticDiscovery()) {
            url = getBrokerURL(dataStore.getSSL(), dataStore.getHost(), dataStore.getPort());
        } else {
            StringJoiner brokerURLs = new StringJoiner(",");
            for (Broker brokerBroker : dataStore.getBrokers()) {
                brokerURLs.add(getBrokerURL(dataStore.getSSL(), brokerBroker.getHost(), brokerBroker.getPort()));
            }
            url = getTransport(dataStore) + ":(" + brokerURLs + ")" + getURIParameters(dataStore);
        }
        log.info(url);
        return new ActiveMQConnectionFactory(url);
    }

    private String getURIParameters(JmsDataStore dataStore) {
        String URIParameters = "";
        if (dataStore.getFailover()) {
            URIParameters = dataStore.getFailoverURIParameters();
        }
        if (dataStore.getStaticDiscovery()) {
            URIParameters = dataStore.getStaticDiscoveryURIParameters();
        }
        return URIParameters;
    }

    private String getBrokerURL(Boolean isSSLUsed, String host, String port) {
        return isSecured(isSSLUsed) + "://" + host + ":" + port;
    }

    private String getTransport(JmsDataStore dataStore) {
        String transport = null;
        if (dataStore.getFailover()) {
            transport = "failover";
        }
        if (dataStore.getStaticDiscovery()) {
            transport = "discovery://static";
        }
        return transport;
    }

    private String isSecured(boolean sslTransport) {
        return sslTransport ? "ssl" : "tcp";
    }

    public Session getSession(Connection connection, Boolean transacted) throws JMSException {
        int acknowledge = transacted ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE;
        return connection.createSession(transacted, acknowledge);
    }

    public Connection getConnection(ConnectionFactory connectionFactory, boolean isUserIdentity, String userName, String password)
            throws JMSException {
        return isUserIdentity ? connectionFactory.createConnection(userName, password) : connectionFactory.createConnection();
    }

    public void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException e) {
                log.warn(i18n.warnConnectionCantBeClosed(), e);
            }
        }
    }

    public void closeSession(Session session) {
        if (session != null) {
            try {
                session.close();
            } catch (JMSException e) {
                log.warn(i18n.warnSessionCantBeClosed(), e);
            }
        }
    }

    public void closeProducer(MessageProducer producer) {
        if (producer != null) {
            try {
                producer.close();
            } catch (JMSException e) {
                log.warn(i18n.warnProducerCantBeClosed(), e);
            }
        }
    }

    public void closeConsumer(MessageConsumer consumer) {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (JMSException e) {
                log.warn(i18n.warnConsumerCantBeClosed(), e);
            }
        }
    }

    public void commit(Session session) {
        try {
            if (session.getTransacted()) {
                session.commit();
            }
        } catch (JMSException e) {

            try {
                session.rollback();
            } catch (JMSException e1) {
                log.error("Can't rollback", e);
            }
        }
    }

}
