package org.talend.components.kafka;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;

@Version(1)
@Documentation("Write records to Kafka.")
public class KafkaOutput extends PTransform<PCollection<Record>, PDone> {

    private final KafkaOutputConfiguration configuration;

    private final KafkaService service;

    public KafkaOutput(@Option("configuration") final KafkaOutputConfiguration configuration, final KafkaService service) {
        this.configuration = configuration;
        this.service = service;
    }

    @Override
    public PDone expand(PCollection<Record> input) {
        return PDone.in(input.getPipeline());
    }

}