package org.talend.components.kafka;

import java.util.Collections;

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Icon.IconType;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;

@Version(1)
@Icon(IconType.KAFKA)
@PartitionMapper(name = "KafkaUseless")
@Documentation("Read records from Kafka.")
public class KafkaUseless extends PTransform<PBegin, PCollection<Record>> {

    private final KafkaDatasetConfiguration configuration;

    public KafkaUseless(@Option("configuration") final KafkaDatasetConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public PCollection<Record> expand(PBegin input) {
        return Create.<Record> of(Collections.emptyList()).expand(input);
    }

}