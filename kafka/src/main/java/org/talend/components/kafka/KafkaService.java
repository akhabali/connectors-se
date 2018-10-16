package org.talend.components.kafka;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

@Service
public class KafkaService {

    @HealthCheck("healthCheck")
    public HealthCheckStatus healthCheck(@Option("configuration.dataset.connection") final KafkaConnectionConfiguration conn) {
        return new HealthCheckStatus(HealthCheckStatus.Status.OK, "Connection ok");
    }

}
