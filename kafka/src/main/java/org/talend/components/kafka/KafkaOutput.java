package org.talend.components.kafka;

import static org.talend.sdk.component.api.component.Icon.IconType.KAFKA;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.runtime.beam.spi.record.AvroRecord;

@Version(1)
@Documentation("Write records to Kafka.")
@Icon(KAFKA)
@Processor(name = "KafkaOutput")
public class KafkaOutput extends PTransform<PCollection<Record>, PDone> {

    private final KafkaOutputConfiguration configuration;

    public KafkaOutput(@Option("configuration") final KafkaOutputConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public PDone expand(PCollection<Record> input) {
        final boolean useAvro = configuration.getDataset().getValueFormat() == KafkaDatasetConfiguration.ValueFormat.AVRO;
        final String kafkaDatasetStringSchema = configuration.getDataset().getAvroSchema();
        final IndexedRecordHelper indexedRecordHelper = new IndexedRecordHelper(kafkaDatasetStringSchema);

        KafkaIO.Write<byte[], byte[]> kafkaWrite = KafkaIO.<byte[], byte[]> write()
                .withBootstrapServers(configuration.getDataset().getConnection().getBrokers())
                .withTopic(configuration.getDataset().getTopic()).withKeySerializer(ByteArraySerializer.class)
                .withValueSerializer(ByteArraySerializer.class)
                .updateProducerProperties(KafkaService.createOutputMaps(configuration));

        switch (configuration.getPartitionType()) {
        case COLUMN: {
            PCollection pc1 = input.apply(WithKeys.of(new ProduceKey(configuration.getKeyColumn())));
            if (useAvro) {
                return ((PCollection<KV<byte[], byte[]>>) pc1.apply(ParDo.of(new AvroKVToByteArrayDoFn(indexedRecordHelper))))
                        .apply(kafkaWrite);
            } else { // csv
                return ((PCollection<KV<byte[], byte[]>>) pc1
                        .apply(MapElements.via(new FormatCsvKV(configuration.getDataset().getFieldDelimiter()))))
                                .apply(kafkaWrite);
            }
        }
        case ROUND_ROBIN: {
            if (useAvro) {
                return input.apply(ParDo.of(new AvroToByteArrayDoFn(indexedRecordHelper))).apply(kafkaWrite.values());
            } else { // csv
                return input.apply(MapElements.via(new FormatCsv(configuration.getDataset().getFieldDelimiter())))
                        .apply(kafkaWrite.values());
            }
        }
        default:
            throw new RuntimeException("To be implemented: " + configuration.getPartitionType());
        }

    }

    public static class FormatCsvKV extends SimpleFunction<KV<byte[], Record>, KV<byte[], byte[]>> {

        public final FormatCsvFunction function;

        public FormatCsvKV(String fieldDelimiter) {
            function = new FormatCsvFunction(fieldDelimiter);
        }

        @Override
        public KV<byte[], byte[]> apply(KV<byte[], Record> input) {
            return KV.of(input.getKey(), function.apply(input.getValue()));
        }
    }

    public static class FormatCsv extends SimpleFunction<Record, byte[]> {

        public final FormatCsvFunction function;

        public FormatCsv(String fieldDelimiter) {
            function = new FormatCsvFunction(fieldDelimiter);
        }

        @Override
        public byte[] apply(Record input) {
            return function.apply(input);
        }
    }

    public static class FormatCsvFunction implements SerializableFunction<Record, byte[]> {

        public final String fieldDelimiter;

        private StringBuilder sb = new StringBuilder();

        public FormatCsvFunction(String fieldDelimiter) {
            this.fieldDelimiter = fieldDelimiter;
        }

        @Override
        public byte[] apply(Record record) {
            IndexedRecord input = new AvroRecord(record).unwrap(IndexedRecord.class);
            int size = input.getSchema().getFields().size();
            for (int i = 0; i < size; i++) {
                if (sb.length() != 0)
                    sb.append(fieldDelimiter);
                sb.append(input.get(i));
            }
            byte[] bytes = sb.toString().getBytes(Charset.forName("UTF-8"));
            sb.setLength(0);
            return bytes;
        }
    }

    public static class ProduceKey implements SerializableFunction<Record, byte[]> {

        private final String keyName;

        public ProduceKey(String keyName) {
            this.keyName = keyName;
        }

        @Override
        public byte[] apply(Record input) {
            return input.getString(keyName).getBytes(Charset.forName("UTF-8"));
        }
    }

    /**
     * An {@link IndexedRecord} wrapper to wrap {@link IndexedRecord} with a custom
     * Kafka Avro schema. Calls are delegated to the wrapped {@link IndexedRecord},
     * with the exception of @getSchema that returns a fixed {@link Schema}
     */
    public static class KafkaIndexedRecordWrapper implements IndexedRecord {

        IndexedRecord incomingIndexedRecord;

        Schema datasetSchema;

        public void setIndexedRecord(IndexedRecord incomingIndexedRecord) {
            this.incomingIndexedRecord = incomingIndexedRecord;
        }

        public void setDatasetSchema(Schema datasetSchema) {
            this.datasetSchema = datasetSchema;
        }

        @Override
        public void put(int i, Object v) {
            incomingIndexedRecord.put(i, v);
        }

        @Override
        public Object get(int i) {
            return incomingIndexedRecord.get(i);
        }

        @Override
        public Schema getSchema() {
            return datasetSchema;
        }
    }

    /**
     * Transform Avro key value {@link IndexedRecord} into a byte array.
     *
     * In case of a dataset that uses a custom Avro schema, the @processElement method
     * uses the {@link IndexedRecordHelper} to wrap incoming records in
     * {@link KafkaIndexedRecordWrapper} and then writes them using the custom Avro schema.
     * Otherwise, the incoming records are written with their original schema.
     */
    public static class AvroKVToByteArrayDoFn extends DoFn<KV<byte[], IndexedRecord>, KV<byte[], byte[]>> {

        IndexedRecordHelper helper;

        AvroKVToByteArrayDoFn(IndexedRecordHelper helper) {
            this.helper = helper;
        }

        @Setup
        public void setup() {
            helper.setup();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                helper.getKafkaIndexedRecordWrapper().setIndexedRecord(c.element().getValue());
                helper.getDatumWriter().write(helper.getKafkaIndexedRecordWrapper(), encoder);
                encoder.flush();
                byte[] result = out.toByteArray();
                out.close();
                c.output(KV.of(c.element().getKey(), result));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Transform Avro {@link IndexedRecord} into a byte array.
     *
     * In case of a dataset that uses a custom Avro schema, the @processElement method
     * uses the {@link IndexedRecordHelper} to wrap incoming records in
     * {@link KafkaIndexedRecordWrapper} and then writes them using the custom Avro schema.
     * Otherwise, the incoming records are written with their original schema.
     */
    public static class AvroToByteArrayDoFn extends DoFn<Record, byte[]> {

        IndexedRecordHelper helper;

        AvroToByteArrayDoFn(IndexedRecordHelper helper) {
            this.helper = helper;
        }

        @Setup
        public void setup() {
            helper.setup();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                helper.getKafkaIndexedRecordWrapper().setIndexedRecord(AvroRecord.class.cast(c.element()).unwrap(IndexedRecord.class));
                helper.getDatumWriter().write(helper.getKafkaIndexedRecordWrapper(), encoder);
                encoder.flush();
                byte[] result = out.toByteArray();
                out.close();
                c.output(result);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * {@link IndexedRecord} helper to setup {@link DoFn} classes for working with incoming records
     * {@link IndexedRecord} to Byte arrays and avoid object instantiation
     * inside @{@link org.apache.beam.sdk.transforms.DoFn.ProcessElement}
     * <p>
     * If @useCustomAvroSchema is set to true, we initialize some helper objects in the @setup().
     */
    public static class IndexedRecordHelper implements Serializable {

        private String kafkaDatasetStringSchema;

        private KafkaIndexedRecordWrapper kafkaIndexedRecordWrapper;

        private DatumWriter<IndexedRecord> datumWriter;

        IndexedRecordHelper(String kafkaDatasetStringSchema) {
            this.kafkaDatasetStringSchema = kafkaDatasetStringSchema;
        }

        public void setup() {
            kafkaIndexedRecordWrapper = new KafkaIndexedRecordWrapper();
            org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
            Schema kafkaDatasetSchema = parser.parse(kafkaDatasetStringSchema);
            kafkaIndexedRecordWrapper.setDatasetSchema(kafkaDatasetSchema);
            datumWriter = new GenericDatumWriter(kafkaDatasetSchema);
        }

        public KafkaIndexedRecordWrapper getKafkaIndexedRecordWrapper() {
            return kafkaIndexedRecordWrapper;
        }

        public DatumWriter<IndexedRecord> getDatumWriter() {
            return datumWriter;
        }

        public void setDatumWriter(DatumWriter<IndexedRecord> datumWriter) {
            this.datumWriter = datumWriter;
        }

        @Override
        public String toString() {
            return "IndexedRecordHelper{" + "kafkaDatasetStringSchema='" + kafkaDatasetStringSchema + '\''
                    + ", kafkaIndexedRecordWrapper=" + kafkaIndexedRecordWrapper + ", datumWriter=" + datumWriter + '}';
        }
    }

}
