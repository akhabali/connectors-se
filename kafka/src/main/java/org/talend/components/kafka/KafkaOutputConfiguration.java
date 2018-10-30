package org.talend.components.kafka;

import java.io.Serializable;

import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Version(1)
@Data
@GridLayout({ @GridLayout.Row("dataset"), //
        @GridLayout.Row({ "partitionType", "keyColumn" }), //
        @GridLayout.Row({ "useCompress", "compressType" }) })
@Documentation("TODO fill the documentation for this configuration")
public class KafkaOutputConfiguration implements Serializable {

    @Option
    @Documentation("")
    private KafkaDatasetConfiguration dataset;

    @Option
    @Required
    @Documentation("Strategy for assigning partitions")
    private PartitionType partitionType = PartitionType.ROUND_ROBIN;

    @Option
    @ActiveIf(target = "partitionType", value = "COLUMN")
    @Documentation("Partition by hash value of this key column")
    private String keyColumn; // TODO provide incoming schema promote, like "widget.type.datalist"

    @Option
    @Required
    @Documentation("")
    private boolean useCompress = false;

    @Option
    @ActiveIf(target = "useCompress", value = "true")
    @Documentation("Produce compressed message")
    // https://cwiki.apache.org/confluence/display/KAFKA/Compression
    private CompressType compressType = CompressType.GZIP;

    // TODO: Kafka Configuration Table
    // KafkaConfTableProperties configurations

    public enum CompressType {
        GZIP,
        SNAPPY
    }

    public enum PartitionType {
        // no key provided, kafka produce use round-robin as default partition strategy
        ROUND_ROBIN,
        // use the value of one column in the record as the key, and kafka will use this value to calculate partition
        COLUMN
    }
}
