package org.talend.components.kafka;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.Duration;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Icon.IconType;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.runtime.beam.coder.record.FullSerializationRecordCoder;
import org.talend.sdk.component.runtime.beam.spi.record.AvroRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Version(1)
@Icon(IconType.KAFKA)
@PartitionMapper(name = "KafkaInput")
@Documentation("Read records from Kafka.")
public class KafkaInput extends PTransform<PBegin, PCollection<Record>> {

    private final KafkaInputConfiguration configuration;

    public KafkaInput(@Option("configuration") final KafkaInputConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public PCollection<Record> expand(PBegin input) {
        KafkaIO.Read<byte[], byte[]> kafkaRead = KafkaIO.<byte[], byte[]> read()
                .withBootstrapServers(configuration.getDataset().getConnection().getBrokers())
                .withTopics(Arrays.asList(new String[] { configuration.getDataset().getTopic() }))
                .updateConsumerProperties(KafkaService.createInputMaps(configuration))
                .withKeyDeserializer(ByteArrayDeserializer.class).withValueDeserializer(ByteArrayDeserializer.class);

        if (configuration.isUseMaxReadTime()) {
            kafkaRead = kafkaRead.withMaxReadTime(new Duration(configuration.getMaxReadTime()));
        }
        if (configuration.isUseMaxNumRecords()) {
            kafkaRead = kafkaRead.withMaxNumRecords(configuration.getMaxNumRecords());
        }
        // only consider value of kafkaRecord no matter which format selected
        PCollection<byte[]> kafkaRecords = input.apply(kafkaRead) //
                .apply(ParDo.of(new ExtractRecord())) //
                .apply(Values.<byte[]> create());
        switch (configuration.getDataset().getValueFormat()) {
        case AVRO: {
            return kafkaRecords.apply(ParDo.of(new ByteArrayToAvroRecord(configuration.getDataset().getAvroSchema())))
                    .setCoder(FullSerializationRecordCoder.of());
        }
        case CSV: {
            return kafkaRecords.apply(ParDo.of(new ExtractCsvSplit(configuration.getDataset().getFieldDelimiter())))
                    .apply(ParDo.of(new StringArrayToAvroRecord())).setCoder(FullSerializationRecordCoder.of());
        }
        default:
            throw new RuntimeException("To be implemented: " + configuration.getDataset().getValueFormat());
        }
    }

    public static class ExtractRecord extends DoFn<KafkaRecord<byte[], byte[]>, KV<byte[], byte[]>> {

        @DoFn.ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().getKV());
        }
    }

    public static class ByteArrayToAvroRecord extends DoFn<byte[], Record> {

        private final String schemaStr;

        private transient Schema schema;

        private transient DatumReader<GenericRecord> datumReader;

        private transient BinaryDecoder decoder;

        ByteArrayToAvroRecord(String schemaStr) {
            this.schemaStr = schemaStr;
        }

        @DoFn.ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            if (schema == null) {
                schema = new Schema.Parser().parse(schemaStr);
                datumReader = new GenericDatumReader<GenericRecord>(schema);
            }
            decoder = DecoderFactory.get().binaryDecoder(c.element(), decoder);
            GenericRecord record = datumReader.read(null, decoder);
            c.output(new AvroRecord(record));
        }
    }

    public static class StringArrayToAvroRecord extends DoFn<String[], Record> {

        public static final String RECORD_NAME = "StringArrayRecord";

        public static final String FIELD_PREFIX = "field";

        private transient Schema schema;

        private Schema inferStringArray(String[] in) {
            List<Schema.Field> fields = new ArrayList<>();

            SchemaBuilder.FieldAssembler<Schema> fa = SchemaBuilder.record(RECORD_NAME).fields();
            for (int i = 0; i < in.length; i++) {
                fa = fa.name(FIELD_PREFIX + i).type(Schema.create(Schema.Type.STRING)).noDefault();
            }
            return fa.endRecord();
        }

        @DoFn.ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            String[] value = c.element();
            if (schema == null) {
                schema = inferStringArray(value);
            }
            c.output(new AvroRecord(new StringArrayIndexedRecord(schema, value)));
        }
    }

    public static class StringArrayIndexedRecord implements IndexedRecord {

        private final Schema schema;

        private final String[] value;

        public StringArrayIndexedRecord(Schema schema, String[] value) {
            this.schema = schema;
            this.value = value;
        }

        @Override
        public Schema getSchema() {
            return schema;
        }

        @Override
        public Object get(int i) {
            return value[i];
        }

        @Override
        public void put(int i, Object v) {
            value[i] = v == null ? null : String.valueOf(v);
        }
    }

}
