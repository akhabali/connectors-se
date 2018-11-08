package org.talend.components.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

@Service
public class KafkaService {

    @HealthCheck("healthCheck")
    public HealthCheckStatus healthCheck(@Option("configuration.dataset.connection") final KafkaConnectionConfiguration conn) {
        try {
            KafkaConsumer consumer = new KafkaConsumer(createHealthCheckProps(conn));
            consumer.listTopics();
        } catch (Throwable exception) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, exception.getMessage());
        }
        return new HealthCheckStatus(HealthCheckStatus.Status.OK, "Connection ok");
    }

    @Suggestions("KafkaTopics")
    public SuggestionValues listTopics(@Option("configuration.dataset.connection") final KafkaConnectionConfiguration conn) {
        final KafkaConsumer consumer = new KafkaConsumer(createHealthCheckProps(conn));
        return new SuggestionValues(true,
                (Collection<SuggestionValues.Item>) StreamSupport.stream(consumer.listTopics().keySet().spliterator(), false)
                        .map(topic -> new SuggestionValues.Item(topic.toString(), topic.toString())).collect(toList()));
    }

    public static Map<String, Object> createInputMaps(KafkaInputConfiguration input, boolean isBeam) {
        Map<String, Object> props = new HashMap<>();
        props.putAll(createConnMaps(input.getDataset().getConnection(), isBeam));

        if (input.hasGroupId()) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, input.getGroupId());
            if (!input.isBoundedSource()) {
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            }
        }
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, input.getAutoOffsetReset().toString().toLowerCase());

        // props.putAll(createConfigurationTable(input.configurations));

        return props;
    }

    public static Map<String, Object> createOutputMaps(KafkaOutputConfiguration output) {
        Map<String, Object> props = new HashMap<>();
        props.putAll(createConnMaps(output.getDataset().getConnection(), true));

        if (output.isUseCompress()) {
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, output.getCompressType().toString().toLowerCase());
        }

        // props.putAll(createConfigurationTable(output.configurations));

        return props;
    }

    // private static Map<String, Object> createConfigurationTable(KafkaConfTableProperties table) {
    // Map<String, Object> props = new HashMap<>();
    // List<String> configKeys = table.keyCol.getValue();
    // List<String> configValues = table.valueCol.getValue();
    // if (configKeys != null && !configKeys.isEmpty() && configValues != null && !configValues.isEmpty()) {
    // for (int i = 0; i < configKeys.size(); i++) {
    // props.put(configKeys.get(i), configValues.get(i));
    // }
    // }
    // return props;
    // }

    private static Properties createConnProps(KafkaConnectionConfiguration conn) {
        Properties props = new Properties();
        Map<String, String> consumerMaps = createConnMaps(conn, false);
        for (String key : consumerMaps.keySet()) {
            props.setProperty(key, consumerMaps.get(key));
        }
        return props;
    }

    private static Properties createHealthCheckProps(KafkaConnectionConfiguration conn) {
        Properties props = createConnProps(conn);
        // decrease REQUEST_TIMEOUT_MS to avoid wait for longtime with failed SSL/Kerberos connect
        props.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000"); // default is 5min 305000ms
        return props;
    }

    private static Map<String, String> createConnMaps(KafkaConnectionConfiguration conn, boolean isBeam) {
        Map<String, String> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conn.getBrokers());
        if (!isBeam) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        }
        if (conn.isUseSsl()) {
            props.put("security.protocol", "SSL");
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, conn.getTrustStoreType().toString());
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, conn.getTrustStorePath());
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, conn.getTrustStorePassword());
            if (conn.isNeedClientAuth()) {
                props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, conn.getKeyStoreType().toString());
                props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, conn.getKeyStorePath());
                props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, conn.getKeyStorePassword());
            }
        }
        return props;
    }

}
