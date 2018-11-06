package org.talend.components.kafka;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
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
import org.talend.components.kafka.KafkaDatasetConfiguration.FieldDelimiterType;
import org.talend.components.kafka.KafkaDatasetConfiguration.ValueFormat;
import org.talend.components.kafka.KafkaInputConfiguration.OffsetResetStrategy;
import org.talend.sdk.component.api.record.Record;

import net.manub.embeddedkafka.EmbeddedK;
import net.manub.embeddedkafka.EmbeddedKafka$;
import net.manub.embeddedkafka.EmbeddedKafkaConfig$;
import scala.collection.Map$;

public class KafkaCsvTest {

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

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Before
    public void setup() {
        EmbeddedK brk = EmbeddedKafka$.MODULE$.start(EmbeddedKafkaConfig$.MODULE$.apply(0, 0, //
                Map$.MODULE$.<String, String> empty(), //
                Map$.MODULE$.<String, String> empty(), //
                Map$.MODULE$.<String, String> empty()));
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
    }

    @After
    public void teardown() {
        EmbeddedKafka$.MODULE$.stop();
    }

    /**
     * Read csv format value and write csv format value without schema and do not write key
     * using standard field delimiter
     */
    @Test
    public void csvBasicTest1() {
        inputDatasetProperties.setFieldDelimiter(FieldDelimiterType.SEMICOLON);
        outputDatasetProperties.setFieldDelimiter(FieldDelimiterType.SEMICOLON);

        testCase1("csvBasicTest1", "1", fieldDelimiter);
    }

    /**
     * Read csv format value and write csv format value without schema and do not write key
     * using standard field delimiter
     */
    @Test
    public void csvBasicTest2() {
        inputDatasetProperties.setFieldDelimiter(FieldDelimiterType.SEMICOLON);
        outputDatasetProperties.setFieldDelimiter(FieldDelimiterType.SEMICOLON);

        testCase2("csvBasicTest2", "2", fieldDelimiter);
    }

    /**
     * Read csv format value and write csv format value without schema and do not write key
     * using custom field delimiter
     */
    @Test
    public void csvBasicTest1WithOtherFieldDelimiter() {
        inputDatasetProperties.setFieldDelimiter(FieldDelimiterType.OTHER);
        inputDatasetProperties.setSpecificFieldDelimiter(otherFieldDelimiter);
        outputDatasetProperties.setFieldDelimiter(FieldDelimiterType.OTHER);
        outputDatasetProperties.setSpecificFieldDelimiter(otherFieldDelimiter);

        testCase1("csvBasicTest1WithOtherFieldDelimiter", "3", otherFieldDelimiter);
    }

    /**
     * Read csv format value and write csv format value without schema and do not write key
     * using standard field delimiter
     */
    @Test
    public void csvBasicTest2WithOtherFieldDelimiter() {
        inputDatasetProperties.setFieldDelimiter(FieldDelimiterType.OTHER);
        inputDatasetProperties.setSpecificFieldDelimiter(otherFieldDelimiter);
        outputDatasetProperties.setFieldDelimiter(FieldDelimiterType.OTHER);
        outputDatasetProperties.setSpecificFieldDelimiter(otherFieldDelimiter);

        testCase2("csvBasicTest2WithOtherFieldDelimiter", "4", otherFieldDelimiter);
    }

    /**
     * Input with earliest & maxRecords
     * Output with round_robin partition & Non Compress
     */
    public void testCase1(String title, String topicSuffix, String fieldDelim) {
        String testID = title + new Random().nextInt();

        expectedPersons = Person.genRandomList(testID, maxRecords);

        // ----------------- Send data to TOPIC_IN start --------------------
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Void, String> producer = new KafkaProducer<>(props);
        for (Person person : expectedPersons) {
            ProducerRecord<Void, String> message = new ProducerRecord<>(topicIn + topicSuffix, person.toCSV(fieldDelim));
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

        inputDatasetProperties.setTopic(topicIn + topicSuffix);
        outputDatasetProperties.setTopic(topicOut + topicSuffix);

        KafkaInput inputRuntime = new KafkaInput(inputProperties);
        KafkaOutput outputRuntime = new KafkaOutput(outputProperties);

        // ----------------- pipeline start --------------------
        pipeline.apply(inputRuntime).apply(Filter.by(new FilterByGroup(testID))).apply(outputRuntime);

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
        consumer.subscribe(Arrays.asList(topicOut + topicSuffix));
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

    /**
     * Input with earliest & maxTime 5s
     * Output with by_key partition & Non Compress
     */
    public void testCase2(String title, String topicSuffix, String fieldDelim) {
        String testID = title + new Random().nextInt();

        expectedPersons = Person.genRandomList(testID, maxRecords);

        // ----------------- Send data to TOPIC_IN start --------------------
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Void, String> producer = new KafkaProducer<>(props);
        for (Person person : expectedPersons) {
            ProducerRecord<Void, String> message = new ProducerRecord<>(topicIn + topicSuffix, person.toCSV(fieldDelim));
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

        inputDatasetProperties.setTopic(topicIn + topicSuffix);
        outputDatasetProperties.setTopic(topicOut + topicSuffix);

        KafkaInput inputRuntime = new KafkaInput(inputProperties);
        KafkaOutput outputRuntime = new KafkaOutput(outputProperties);

        // ----------------- pipeline start --------------------
        pipeline.apply(inputRuntime).apply(Filter.by(new FilterByGroup(testID))).apply(outputRuntime);

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
        consumer.subscribe(Arrays.asList(topicOut + topicSuffix));
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

    public static class FilterByGroup implements SerializableFunction<Record, Boolean> {

        private final String groupID;

        public FilterByGroup(String groupID) {
            this.groupID = groupID;
        }

        @Override
        public Boolean apply(Record input) {
            // schema of input is not same as Person.schema
            return groupID.equals(input.getString("field0"));
        }
    }

}
