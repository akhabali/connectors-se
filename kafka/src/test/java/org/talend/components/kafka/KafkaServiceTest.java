package org.talend.components.kafka;

import net.manub.embeddedkafka.EmbeddedK;
import net.manub.embeddedkafka.EmbeddedKafka$;
import net.manub.embeddedkafka.EmbeddedKafkaConfig$;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;
import scala.collection.Map$;

import static org.junit.jupiter.api.Assertions.assertEquals;

@WithComponents("org.talend.components.kafka")
public class KafkaServiceTest {

    @Service
    private KafkaService service;

    private String broker;

    @BeforeEach
    void setup() {
        EmbeddedK brk = EmbeddedKafka$.MODULE$.start(EmbeddedKafkaConfig$.MODULE$.apply(0, 0, //
                Map$.MODULE$.empty(), //
                Map$.MODULE$.empty(), //
                Map$.MODULE$.empty()));
        broker = "localhost:" + brk.config().kafkaPort();
    }

    @Test
    void validateBasicConnectionOK() {
        KafkaConnectionConfiguration conn = new KafkaConnectionConfiguration();
        conn.setBrokers(broker);
        HealthCheckStatus status = service.healthCheck(conn);
        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }

    @Test
    void validateBasicConnectionKO() {
        KafkaConnectionConfiguration conn = new KafkaConnectionConfiguration();
        conn.setBrokers(broker + "wrong");
        HealthCheckStatus status = service.healthCheck(conn);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

}
