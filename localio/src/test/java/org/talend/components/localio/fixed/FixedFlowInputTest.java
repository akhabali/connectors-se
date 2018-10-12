package org.talend.components.localio.fixed;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.talend.daikon.avro.SampleSchemas;
import org.talend.sdk.component.junit.ComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;
import static org.talend.sdk.component.runtime.manager.ComponentManager.ComponentType.MAPPER;

@WithComponents("org.talend.components.localio")
class FixedFlowInputTest {

    private static GenericRecord r1;

    private static GenericRecord r2;

    @Injected
    private ComponentsHandler handler;

    @BeforeAll
    static void init() {
        // The two records to use as values.
        r1 = new GenericData.Record(SampleSchemas.recordSimple());
        r1.put("id", 1);
        r1.put("name", "one");
        r2 = new GenericData.Record(SampleSchemas.recordSimple());
        r2.put("id", 2);
        r2.put("name", "two");
    }

    @Test
    void testAvroInput() {
        final FixedFlowInputConfiguration configuration = new FixedFlowInputConfiguration();
        configuration.setRepeat(2);
        configuration.getDataset().setFormat(FixedDataSetConfiguration.RecordFormat.AVRO);
        configuration.getDataset().setSchema(SampleSchemas.recordSimple().toString());
        configuration.getDataset().setValues(r1.toString() + r2.toString());
        final Map<String, String> asConfig = configurationByExample().forInstance(configuration).configured().toMap();
        final Pipeline pipeline = Pipeline.create();
        final PTransform<PBegin, PCollection<IndexedRecord>> input = handler.asManager()
                .createComponent("LocalIO", "FixedFlowInputRuntime", MAPPER, 1, asConfig)
                .map(e -> (PTransform<PBegin, PCollection<IndexedRecord>>) e)
                .orElseThrow(() -> new IllegalArgumentException("No component for fixed flow input"));
        PAssert.that(pipeline.apply(input)).satisfies(it -> {
            final List<IndexedRecord> records = StreamSupport.stream(it.spliterator(), false).collect(toList());
            assertEquals(4, records.size());
            Map<Boolean, List<IndexedRecord>> groups = records.stream().collect(Collectors.groupingBy(r -> (int) r.get(0) == 1));
            groups.get(true).forEach(record -> assertEquals(r1.toString(), record.toString()));
            assertEquals(2, groups.get(true).size());
            groups.get(false).forEach(record -> assertEquals(r2.toString(), record.toString()));
            assertEquals(2, groups.get(false).size());
            return null;
        });
        pipeline.run().waitUntilFinish();
    }

    @Test
    void testCsvInput() {
        final FixedFlowInputConfiguration configuration = new FixedFlowInputConfiguration();
        configuration.setRepeat(3);
        configuration.getDataset().setFormat(FixedDataSetConfiguration.RecordFormat.CSV);
        configuration.getDataset().setCsvSchema("id;name");
        configuration.getDataset().setValues("1;one\n2;two");
        final Map<String, String> asConfig = configurationByExample().forInstance(configuration).configured().toMap();
        final Pipeline pipeline = Pipeline.create();
        final PTransform<PBegin, PCollection<IndexedRecord>> input = handler.asManager()
                .createComponent("LocalIO", "FixedFlowInputRuntime", MAPPER, 1, asConfig)
                .map(e -> (PTransform<PBegin, PCollection<IndexedRecord>>) e)
                .orElseThrow(() -> new IllegalArgumentException("No component for fixed flow input"));
        PAssert.that(pipeline.apply(input)).satisfies(it -> {
            final List<IndexedRecord> records = StreamSupport.stream(it.spliterator(), false).collect(toList());
            assertEquals(6, records.size());
            Map<Boolean, List<IndexedRecord>> groups = records.stream()
                    .collect(Collectors.groupingBy(r -> ((org.apache.avro.util.Utf8) r.get(0)).toString().equals("1")));
            groups.get(true).forEach(record -> assertEquals("{\"id\": \"1\", \"name\": \"one\"}", record.toString()));
            assertEquals(3, groups.get(true).size());
            groups.get(false).forEach(record -> assertEquals("{\"id\": \"2\", \"name\": \"two\"}", record.toString()));
            assertEquals(3, groups.get(false).size());
            return null;
        });
        pipeline.run().waitUntilFinish();
    }

    @Test
    void testJsonInput() {
        final FixedFlowInputConfiguration configuration = new FixedFlowInputConfiguration();
        configuration.setRepeat(2);
        configuration.getDataset().setFormat(FixedDataSetConfiguration.RecordFormat.JSON);
        configuration.getDataset().setValues("{'id':1, 'name':'one'} {'id':2, 'name':'two'}".replace('\'', '"'));
        final Map<String, String> asConfig = configurationByExample().forInstance(configuration).configured().toMap();
        final Pipeline pipeline = Pipeline.create();
        final PTransform<PBegin, PCollection<IndexedRecord>> input = handler.asManager()
                .createComponent("LocalIO", "FixedFlowInputRuntime", MAPPER, 1, asConfig)
                .map(e -> (PTransform<PBegin, PCollection<IndexedRecord>>) e)
                .orElseThrow(() -> new IllegalArgumentException("No component for fixed flow input"));
        PAssert.that(pipeline.apply(input)).satisfies(it -> {
            final List<IndexedRecord> records = StreamSupport.stream(it.spliterator(), false).collect(toList());
            assertEquals(4, records.size());
            Map<Boolean, List<IndexedRecord>> groups = records.stream().collect(Collectors.groupingBy(r -> (int) r.get(0) == 1));
            groups.get(true).forEach(record -> assertEquals("{\"id\": 1, \"name\": \"one\"}", record.toString()));
            assertEquals(2, groups.get(true).size());
            groups.get(false).forEach(record -> assertEquals("{\"id\": 2, \"name\": \"two\"}", record.toString()));
            assertEquals(2, groups.get(false).size());
            return null;
        });
        pipeline.run().waitUntilFinish();
    }
}
