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
        @GridLayout.Row("isStreaming"), //
        @GridLayout.Row("groupId"), //
        @GridLayout.Row("autoOffsetReset"), //
        @GridLayout.Row({ "useMaxReadTime", "maxReadTime" }), //
        @GridLayout.Row({ "useMaxNumRecords", "maxNumRecords" }) })
@Documentation("TODO fill the documentation for this configuration")
public class KafkaInputConfiguration implements Serializable {

    @Option
    @Required
    @Documentation("TODO fill the documentation for this parameter")
    private KafkaDatasetConfiguration dataset;

    @Option
    @Required
    @Documentation("Hidden property used to specify that this component generates unbounded input. ")
    private boolean isStreaming = true;

    @Option
    @Required
    @Documentation("TODO fill the documentation for this parameter")
    // https://github.com/apache/kafka/blob/1.0.0/core/src/main/scala/kafka/common/Config.scala#L26
    // KAFKA-3417
    @Pattern("^[a-zA-Z0-9\\._\\-]+$")
    private String groupId;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private OffsetType autoOffsetReset = OffsetType.LATEST;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private boolean useMaxReadTime = false;

    @Option
    @ActiveIf(target = "useMaxReadTime", value = "true")
    @Documentation("TODO fill the documentation for this parameter")
    private Long maxReadTime = 60000L;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private boolean useMaxNumRecords = false;

    @Option
    @ActiveIf(target = "useMaxNumRecords", value = "true")
    @Documentation("TODO fill the documentation for this parameter")
    private Long maxNumRecords = 5000L;

    // TODO: Kafka Configuration Table
    // KafkaConfTableProperties configurations

    // sync with org.apache.kafka.clients.consumer.OffsetResetStrategy
    public enum OffsetType {
        LATEST,
        EARLIEST
    }
}