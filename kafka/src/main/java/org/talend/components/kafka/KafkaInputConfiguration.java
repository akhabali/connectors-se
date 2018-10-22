package org.talend.components.kafka;

import java.io.Serializable;

import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Version(1)
@Data
@GridLayout({ @GridLayout.Row("dataset"), // hidden?
        @GridLayout.Row("groupId"), //
        @GridLayout.Row("autoOffsetReset"), //
        @GridLayout.Row({ "useMaxReadTime", "maxReadTime" }), //
        @GridLayout.Row({ "useMaxNumRecords", "maxNumRecords" }) })
@Documentation("TODO fill the documentation for this configuration")
public class KafkaInputConfiguration implements Serializable {

    @Option
    @Documentation("")
    private KafkaDatasetConfiguration dataset;

    @Option
    @Documentation("Consumer group ID to fetch or create offsets for.")
    // https://github.com/apache/kafka/blob/1.0.0/core/src/main/scala/kafka/common/Config.scala#L26
    // KAFKA-3417
    @Pattern("^[a-zA-Z0-9\\._\\-]+$")
    private String groupId;

    @Option
    @Documentation("If offsets don't already exist, where to start reading in the topic.")
    private OffsetResetStrategy autoOffsetReset = OffsetResetStrategy.LATEST;

    @Option
    @Documentation("")
    private boolean useMaxReadTime = false;

    @Option
    @ActiveIf(target = "useMaxReadTime", value = "true")
    @Documentation("Stop reading after this length of time (ms).")
    private Long maxReadTime = 60000L;

    @Option
    @Documentation("")
    private boolean useMaxNumRecords = false;

    @Option
    @ActiveIf(target = "useMaxNumRecords", value = "true")
    @Documentation("Stop reading after this many records.")
    private Long maxNumRecords = 5000L;

    // TODO: Kafka Configuration Table
    // KafkaConfTableProperties configurations

    // sync with org.apache.kafka.clients.consumer.OffsetResetStrategy
    public enum OffsetResetStrategy {
        LATEST,
        EARLIEST,
        NONE
    }
}
