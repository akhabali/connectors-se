package org.talend.components.salesforce.input;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.talend.components.salesforce.service.SalesforceService.URL;

import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;
import org.talend.components.salesforce.SalesforceBaseTest;
import org.talend.components.salesforce.dataset.QueryDataSet;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.runtime.beam.TalendIO;
import org.talend.sdk.component.runtime.input.Mapper;

@Ignore
@DisplayName("Suite of test for the Salesforce Input with beam")
public class SalesforceInputEmitterBeamTest extends SalesforceBaseTest {

    @Test
    public void inputWithModuleName() {
        final QueryDataSet queryDataSet = new QueryDataSet();
        dataStore.setEndpoint(URL);
        dataStore.setUserId(USER_ID);
        dataStore.setPassword(PASSWORD);
        dataStore.setSecurityKey(SECURITY_KEY);
        queryDataSet.setModuleName("Account");
        queryDataSet.setSourceType(QueryDataSet.SourceType.MODULE_SELECTION);
        queryDataSet.setSelectColumnIds(Arrays.asList("Id", "Name", "CreatedDate"));
        queryDataSet.setDataStore(dataStore);

        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = COMPONENT_FACTORY.createMapper(InputEmitter.class, queryDataSet);

        // create a pipeline starting with the mapper
        final PCollection<Record> out = pipeline.apply(TalendIO.read(mapper));

        // then append some assertions to the output of the mapper,
        // PAssert is a beam utility to validate part of the pipeline
        PAssert.that(out).satisfies(it -> {
            final List<Record> records = StreamSupport.stream(it.spliterator(), false).collect(toList());
            assertEquals(10, records.size());
            Record record = records.get(0);
            Schema schema = record.getSchema();
            List<Schema.Entry> entries = schema.getEntries();
            assertEquals(3, entries.size());
            print(record);
            return null;
        });

        // finally run the pipeline and ensure it was successful - i.e. data were validated
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }

    @Test
    public void inputWithSOQL() {
        final QueryDataSet queryDataSet = new QueryDataSet();
        dataStore.setEndpoint(URL);
        dataStore.setUserId(USER_ID);
        dataStore.setPassword(PASSWORD);
        dataStore.setSecurityKey(SECURITY_KEY);
        queryDataSet.setSourceType(QueryDataSet.SourceType.SOQL_QUERY);
        queryDataSet.setQuery("Select Id,Name,CreatedDate from Account");
        queryDataSet.setDataStore(dataStore);

        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = COMPONENT_FACTORY.createMapper(InputEmitter.class, queryDataSet);

        // create a pipeline starting with the mapper
        final PCollection<Record> out = pipeline.apply(TalendIO.read(mapper));

        // then append some assertions to the output of the mapper,
        // PAssert is a beam utility to validate part of the pipeline
        PAssert.that(out).satisfies(it -> {
            final List<Record> records = StreamSupport.stream(it.spliterator(), false).collect(toList());
            assertEquals(10, records.size());
            Record record = records.get(0);
            Schema schema = record.getSchema();
            List<Schema.Entry> entries = schema.getEntries();
            assertEquals(3, entries.size());
            print(record);
            return null;
        });

        // finally run the pipeline and ensure it was successful - i.e. data were validated
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }

    public void print(final Record record) {
        final Schema schema = record.getSchema();
        // log in the natural type
        schema.getEntries().forEach(entry -> System.out.println(record.get(Object.class, entry.getName())));
        // log only strings
        schema
                .getEntries()
                .stream()
                .filter(e -> e.getType() == Schema.Type.STRING)
                .forEach(entry -> System.out.println(record.getString(entry.getName())));
    }
}