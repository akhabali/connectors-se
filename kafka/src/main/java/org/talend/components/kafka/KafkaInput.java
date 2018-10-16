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

// @Version(1)
// @Icon(IconType.KAFKA)
// @PartitionMapper(name = "KafkaInput")
// @Documentation("Read records from Kafka.")
public class KafkaInput extends PTransform<PBegin, PCollection<Record>> {

    private final KafkaInputConfiguration configuration;

    private final KafkaService service;

    public KafkaInput(@Option("configuration") final KafkaInputConfiguration configuration, final KafkaService service) {
        this.configuration = configuration;
        this.service = service;
    }

    @Override
    public PCollection<Record> expand(PBegin input) {
        return Create.<Record> of(Collections.emptyList()).expand(input);
    }

}