package org.talend.components.kafka;

import lombok.extern.slf4j.Slf4j;
import net.manub.embeddedkafka.EmbeddedK;
import net.manub.embeddedkafka.EmbeddedKafka$;
import net.manub.embeddedkafka.EmbeddedKafkaConfig$;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.talend.components.kafka.KafkaDatasetConfiguration.FieldDelimiterType;
import org.talend.components.kafka.KafkaDatasetConfiguration.ValueFormat;
import org.talend.components.kafka.KafkaInputConfiguration.OffsetResetStrategy;
import scala.collection.Map$;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/*-
 * Connection
 *      Basic: [testCase1] [testCase2] [testCase3]
 *      Ssl:
 * Dataset
 *      Csv
 *          SEMICOLON: [testCase1] [testCase4]
 *          Others: [testCase2]
 *      Avro: [testCase3]
 * Input
 *      GroupId: [testCase4]
 *      OffsetReset
 *          LATEST:
 *          EARLIEST: [testCase1] [testCase2] [testCase3] [testCase4]
 *          NONE:
 *      SourceType
 *          unbounded:
 *          maxReadTime: [testCase2]
 *          maxRecord: [testCase1] [testCase3] [testCase4]
 * Output
 *      Partition
 *          round_robin: [testCase1] [testCase3] [testCase4]
 *          by_key: [testCase2]
 *      Compress
 *          none: [testCase1] [testCase4]
 *          gzip: [testCase2]
 *          snappy: [testCase3]
 *
 */
@Slf4j
public class KafkaInputOutputRuntimeTest {

    KafkaConnectionConfiguration datastoreProperties;

    KafkaDatasetConfiguration inputDatasetProperties;

    KafkaDatasetConfiguration outputDatasetProperties;

    Integer maxRecords = 10;

    String fieldDelimiter = KafkaDatasetConfiguration.FieldDelimiterType.SEMICOLON.getDelimiter();

    String otherFieldDelimiter = "=";

    List<Person> expectedPersons = new ArrayList<>();

    String broker = "localhost:6000";

    String zookeeper = "localhost:6001";

    String topicIn = "test-in";

    String topicOut = "test-out";

    String uid = "";

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() {
        EmbeddedK brk = EmbeddedKafka$.MODULE$.start(EmbeddedKafkaConfig$.MODULE$.apply(0, 0, //
                Map$.MODULE$.<String, String>empty(), //
                Map$.MODULE$.<String, String>empty(), //
                Map$.MODULE$.<String, String>empty()));
        broker = "localhost:" + brk.config().kafkaPort();
        zookeeper = "localhost:" + brk.config().zooKeeperPort();

        datastoreProperties = new KafkaConnectionConfiguration();
        datastoreProperties.setBrokers(broker);

        inputDatasetProperties = new KafkaDatasetConfiguration();
        inputDatasetProperties.setConnection(datastoreProperties);
        inputDatasetProperties.setValueFormat(ValueFormat.CSV);
        // no schema defined

        outputDatasetProperties = new KafkaDatasetConfiguration();
        outputDatasetProperties.setConnection(datastoreProperties);
        outputDatasetProperties.setValueFormat(ValueFormat.CSV);
        // no schema defined

        uid = testName.getMethodName();
    }

    @After
    public void teardown() {
        EmbeddedKafka$.MODULE$.stop();
    }

    /*-
     * Connection
     *      Basic: [x]
     *      Ssl:
     * Dataset
     *      Csv
     *          SEMICOLON: [x]
     *          Others:
     *      Avro:
     * Input
     *      GroupId:
     *      OffsetReset
     *          LATEST:
     *          EARLIEST: [x]
     *          NONE:
     *      SourceType
     *          unbounded:
     *          maxReadTime:
     *          maxRecord: [x]
     * Output
     *      Partition
     *          round_robin: [x]
     *          by_key:
     *      Compress
     *          none: [x]
     *          gzip:
     *          snappy:
     *
     */
    @Test
    public void testCase1() {
        inputDatasetProperties.setFieldDelimiter(FieldDelimiterType.SEMICOLON);
        outputDatasetProperties.setFieldDelimiter(FieldDelimiterType.SEMICOLON);

        String fieldDelim = fieldDelimiter;

        String testID = uid + new Random().nextInt();

        expectedPersons = Person.genRandomList(testID, maxRecords);

        // ----------------- Send data to TOPIC_IN start --------------------
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Void, String> producer = new KafkaProducer<>(props);
        for (Person person : expectedPersons) {
            ProducerRecord<Void, String> message = new ProducerRecord<>(topicIn + uid, person.toCSV(fieldDelim));
            producer.send(message);
        }
        producer.close();
        // ----------------- Send data to TOPIC_IN done --------------------

        KafkaInputConfiguration inputProperties = new KafkaInputConfiguration();
        inputProperties.setDataset(inputDatasetProperties);
        inputProperties.setAutoOffsetReset(OffsetResetStrategy.EARLIEST);
        inputProperties.setUseMaxNumRecords(true);
        inputProperties.setMaxNumRecords(Long.valueOf(maxRecords));

        KafkaOutputConfiguration outputProperties = new KafkaOutputConfiguration();
        outputProperties.setDataset(outputDatasetProperties);

        inputDatasetProperties.setTopic(topicIn + uid);
        outputDatasetProperties.setTopic(topicOut + uid);

        KafkaInput inputRuntime = new KafkaInput(inputProperties);
        KafkaOutput outputRuntime = new KafkaOutput(outputProperties);

        // ----------------- pipeline start --------------------
        pipeline.apply(inputRuntime).apply(outputRuntime);

        // TODO: Remove this when tacokit coders are immutable.
        DirectOptions options = pipeline.getOptions().as(DirectOptions.class);
        options.setEnforceImmutability(false);

        PipelineResult result = pipeline.run(options);
        result.waitUntilFinish();
        // ----------------- pipeline done --------------------

        // ----------------- Read data from TOPIC_OUT start --------------------
        props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("group.id", testID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        KafkaConsumer<Void, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicOut + uid));
        List<Person> results = new ArrayList<>();
        while (true) {
            ConsumerRecords<Void, String> records = consumer.poll(100);
            for (ConsumerRecord<Void, String> record : records) {
                Person person = Person.fromCSV(record.value(), fieldDelim);
                if (testID.equals(person.group)) {
                    results.add(person);
                }
            }
            if (results.size() >= maxRecords) {
                break;
            }
        }
        // ----------------- Read data from TOPIC_OUT end --------------------

        assertEquals(expectedPersons, results);
    }

    /*-
     * Connection
     *      Basic: [x]
     *      Ssl:
     * Dataset
     *      Csv
     *          SEMICOLON:
     *          Others: [x]
     *      Avro:
     * Input
     *      GroupId:
     *      OffsetReset
     *          LATEST:
     *          EARLIEST: [x]
     *          NONE:
     *      SourceType
     *          unbounded:
     *          maxReadTime: [x]
     *          maxRecord:
     * Output
     *      Partition
     *          round_robin:
     *          by_key: [x]
     *      Compress
     *          none:
     *          gzip: [x]
     *          snappy:
     *
     */
    @Test
    public void testCase2() {
        inputDatasetProperties.setFieldDelimiter(FieldDelimiterType.OTHER);
        inputDatasetProperties.setSpecificFieldDelimiter(otherFieldDelimiter);
        outputDatasetProperties.setFieldDelimiter(FieldDelimiterType.OTHER);
        outputDatasetProperties.setSpecificFieldDelimiter(otherFieldDelimiter);

        String fieldDelim = otherFieldDelimiter;

        String testID = uid + new Random().nextInt();

        expectedPersons = Person.genRandomList(testID, maxRecords);

        // ----------------- Send data to TOPIC_IN start --------------------
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Void, String> producer = new KafkaProducer<>(props);
        for (Person person : expectedPersons) {
            ProducerRecord<Void, String> message = new ProducerRecord<>(topicIn + uid, person.toCSV(fieldDelim));
            producer.send(message);
        }
        producer.close();
        // ----------------- Send data to TOPIC_IN done --------------------

        KafkaInputConfiguration inputProperties = new KafkaInputConfiguration();
        inputProperties.setDataset(inputDatasetProperties);
        inputProperties.setAutoOffsetReset(OffsetResetStrategy.EARLIEST);
        inputProperties.setUseMaxReadTime(true);
        inputProperties.setMaxReadTime(5000l);

        KafkaOutputConfiguration outputProperties = new KafkaOutputConfiguration();
        outputProperties.setDataset(outputDatasetProperties);
        outputProperties.setPartitionType(KafkaOutputConfiguration.PartitionType.COLUMN);
        outputProperties.setKeyColumn("field1");
        outputProperties.setUseCompress(true);
        outputProperties.setCompressType(KafkaOutputConfiguration.CompressType.GZIP);

        inputDatasetProperties.setTopic(topicIn + uid);
        outputDatasetProperties.setTopic(topicOut + uid);

        KafkaInput inputRuntime = new KafkaInput(inputProperties);
        KafkaOutput outputRuntime = new KafkaOutput(outputProperties);

        // ----------------- pipeline start --------------------
        pipeline.apply(inputRuntime).apply(outputRuntime);

        // TODO: Remove this when tacokit coders are immutable.
        DirectOptions options = pipeline.getOptions().as(DirectOptions.class);
        options.setEnforceImmutability(false);

        PipelineResult result = pipeline.run(options);
        result.waitUntilFinish();
        // ----------------- pipeline done --------------------

        // ----------------- Read data from TOPIC_OUT start --------------------
        props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("group.id", testID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicOut + uid));
        List<Person> results = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                Person person = Person.fromCSV(record.value(), fieldDelim);
                if (testID.equals(person.group)) {
                    keys.add(record.key());
                    results.add(person);
                }
            }
            if (results.size() >= maxRecords) {
                break;
            }
        }
        // ----------------- Read data from TOPIC_OUT end --------------------

        assertEquals(expectedPersons, results);
        List<String> expectedKeys = new ArrayList<>();
        for (Person person : results) {
            expectedKeys.add(person.name);
        }
        assertEquals(expectedKeys, keys);
    }

    /*-
     * Connection
     *      Basic: [x]
     *      Ssl:
     * Dataset
     *      Csv
     *          SEMICOLON:
     *          Others:
     *      Avro: [x]
     * Input
     *      GroupId:
     *      OffsetReset
     *          LATEST:
     *          EARLIEST: [x]
     *          NONE:
     *      SourceType
     *          unbounded:
     *          maxReadTime:
     *          maxRecord: [x]
     * Output
     *      Partition
     *          round_robin: [x]
     *          by_key:
     *      Compress
     *          none:
     *          gzip:
     *          snappy: [x]
     *
     */
    @Test
    public void testCase3() throws IOException {
        inputDatasetProperties.setValueFormat(ValueFormat.AVRO);
        inputDatasetProperties.setAvroSchema(Person.schema.toString());
        outputDatasetProperties.setValueFormat(ValueFormat.AVRO);
        outputDatasetProperties.setAvroSchema(Person.schema.toString());

        String testID = uid + new Random().nextInt();

        expectedPersons = Person.genRandomList(testID, maxRecords);

        // ----------------- Send data to TOPIC_IN start --------------------
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Producer<Void, byte[]> producer = new KafkaProducer<>(props);
        for (Person person : expectedPersons) {
            ProducerRecord<Void, byte[]> message = new ProducerRecord<>(topicIn + uid, person.serToAvroBytes());
            producer.send(message);
        }
        producer.close();
        // ----------------- Send data to TOPIC_IN done --------------------

        KafkaInputConfiguration inputProperties = new KafkaInputConfiguration();
        inputProperties.setDataset(inputDatasetProperties);
        inputProperties.setAutoOffsetReset(OffsetResetStrategy.EARLIEST);
        inputProperties.setUseMaxNumRecords(true);
        inputProperties.setMaxNumRecords(Long.valueOf(maxRecords));

        KafkaOutputConfiguration outputProperties = new KafkaOutputConfiguration();
        outputProperties.setDataset(outputDatasetProperties);
        outputProperties.setUseCompress(true);
        outputProperties.setCompressType(KafkaOutputConfiguration.CompressType.SNAPPY);

        inputDatasetProperties.setTopic(topicIn + uid);
        outputDatasetProperties.setTopic(topicOut + uid);

        KafkaInput inputRuntime = new KafkaInput(inputProperties);
        KafkaOutput outputRuntime = new KafkaOutput(outputProperties);

        // ----------------- pipeline start --------------------
        pipeline.apply(inputRuntime).apply(outputRuntime);

        // TODO: Remove this when tacokit coders are immutable.
        DirectOptions options = pipeline.getOptions().as(DirectOptions.class);
        options.setEnforceImmutability(false);

        PipelineResult result = pipeline.run(options);
        result.waitUntilFinish();
        // ----------------- pipeline done --------------------

        // ----------------- Read data from TOPIC_OUT start --------------------
        props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("group.id", testID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicOut + uid));
        List<Person> results = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(100);
            for (ConsumerRecord<String, byte[]> record : records) {
                Person person = Person.desFromAvroBytes(record.value());
                if (testID.equals(person.group)) {
                    results.add(person);
                }
            }
            if (results.size() >= maxRecords) {
                break;
            }
        }
        // ----------------- Read data from TOPIC_OUT end --------------------

        assertEquals(expectedPersons, results);
    }

    /*-
     * Connection
     *      Basic: [x]
     *      Ssl:
     * Dataset
     *      Csv
     *          SEMICOLON: [x]
     *          Others:
     *      Avro:
     * Input
     *      GroupId: [x]
     *      OffsetReset
     *          LATEST:
     *          EARLIEST: [x]
     *          NONE:
     *      SourceType
     *          unbounded:
     *          maxReadTime:
     *          maxRecord: [x]
     * Output
     *      Partition
     *          round_robin: [x]
     *          by_key:
     *      Compress
     *          none: [x]
     *          gzip:
     *          snappy:
     *
     * two input test same groupId
     */
    @Test
    public void testCase4() {
        inputDatasetProperties.setFieldDelimiter(FieldDelimiterType.SEMICOLON);
        outputDatasetProperties.setFieldDelimiter(FieldDelimiterType.SEMICOLON);

        String fieldDelim = fieldDelimiter;

        String testID = uid + new Random().nextInt();

        expectedPersons = Person.genRandomList(testID, maxRecords);

        // ----------------- Send data to TOPIC_IN start --------------------
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Void, String> producer = new KafkaProducer<>(props);
        for (Person person : expectedPersons) {
            ProducerRecord<Void, String> message = new ProducerRecord<>(topicIn + uid, person.toCSV(fieldDelim));
            producer.send(message);
        }
        producer.close();
        // ----------------- Send data to TOPIC_IN done --------------------

        KafkaInputConfiguration inputProperties = new KafkaInputConfiguration();
        inputProperties.setDataset(inputDatasetProperties);
        inputProperties.setGroupId(uid);
        inputProperties.setAutoOffsetReset(OffsetResetStrategy.EARLIEST);
        inputProperties.setUseMaxNumRecords(true);
        inputProperties.setMaxNumRecords(Long.valueOf(maxRecords / 2));

        KafkaOutputConfiguration outputProperties = new KafkaOutputConfiguration();
        outputProperties.setDataset(outputDatasetProperties);

        inputDatasetProperties.setTopic(topicIn + uid);
        outputDatasetProperties.setTopic(topicOut + uid);

        KafkaInput inputRuntime = new KafkaInput(inputProperties);
        KafkaOutput outputRuntime = new KafkaOutput(outputProperties);

        // ----------------- pipeline start --------------------
        pipeline.apply(inputRuntime).apply(outputRuntime);

        // TODO: Remove this when tacokit coders are immutable.
        DirectOptions options = pipeline.getOptions().as(DirectOptions.class);
        options.setEnforceImmutability(false);

        pipeline.run(options).waitUntilFinish(); // first pipeline read half records from topic in
        pipeline.run(options).waitUntilFinish(); // second pipeline read last half records from topic in
        // ----------------- pipeline done --------------------

        // ----------------- Read data from TOPIC_OUT start --------------------
        props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("group.id", testID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        KafkaConsumer<Void, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicOut + uid));
        List<Person> results = new ArrayList<>();
        long timeout = System.currentTimeMillis() + 5000; // timeout is 2s
        while (true) {
            ConsumerRecords<Void, String> records = consumer.poll(100);
            for (ConsumerRecord<Void, String> record : records) {
                Person person = Person.fromCSV(record.value(), fieldDelim);
                if (testID.equals(person.group)) {
                    results.add(person);
                }
            }

            if (System.currentTimeMillis() > timeout || results.size() >= maxRecords) {
                break;
            }
        }
        // ----------------- Read data from TOPIC_OUT end --------------------

        assertEquals(expectedPersons, results);
        assertEquals(10, results.size());
    }

}
