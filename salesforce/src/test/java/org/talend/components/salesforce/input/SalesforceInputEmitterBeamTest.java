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
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.runtime.beam.TalendIO;
import org.talend.sdk.component.runtime.input.Mapper;

@Ignore
@DisplayName("Suite of test for the Salesforce Input with beam")
public class SalesforceInputEmitterBeamTest extends SalesforceBaseTest {

    @Test
    public void inputWithModuleName() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setEndpoint(URL);
        datasore.setUserId(USER_ID);
        datasore.setPassword(PASSWORD);
        datasore.setSecurityKey(SECURITY_KEY);
        final QueryDataSet queryDataSet = new QueryDataSet();
        queryDataSet.setModuleName("Account");
        queryDataSet.setSourceType(QueryDataSet.SourceType.MODULE_SELECTION);
        queryDataSet.setSelectColumnIds(Arrays.asList("Id", "Name"));
        queryDataSet.setDataStore(datasore);

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
            assertEquals(2, entries.size());
            return null;
        });

        // finally run the pipeline and ensure it was successful - i.e. data were validated
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }
}