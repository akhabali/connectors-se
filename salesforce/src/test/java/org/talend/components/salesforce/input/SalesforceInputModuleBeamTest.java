package org.talend.components.salesforce.input;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.components.salesforce.service.SalesforceService.URL;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.talend.components.salesforce.SalesforceBaseTest;
import org.talend.components.salesforce.dataset.ModuleQueryDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.runtime.beam.TalendIO;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.manager.chain.Job;

@DisplayName("Suite of test for the Salesforce Input with beam")
public class SalesforceInputModuleBeamTest extends SalesforceBaseTest {

    @Test
    public void inputWithModuleName() {
        final ModuleQueryDataSet queryDataSet = new ModuleQueryDataSet();
        queryDataSet.setModuleName("Account");
        queryDataSet.setSelectColumnIds(Arrays.asList("Id", "Name"));
        queryDataSet.setDataStore(dataStore);

        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = COMPONENT_FACTORY.createMapper(ModuleQueryEmitter.class, queryDataSet);

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

    @Test
    @DisplayName("Module selection case [valid]")
    public void inputWithModuleNameValid() {
        final ModuleQueryDataSet moduleQueryDataSet = new ModuleQueryDataSet();
        moduleQueryDataSet.setModuleName("Account");
        moduleQueryDataSet.setSelectColumnIds(singletonList("Name"));
        moduleQueryDataSet.setDataStore(dataStore);
        moduleQueryDataSet.setCondition("Name Like '%TEST_Name%'");

        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = COMPONENT_FACTORY.createMapper(ModuleQueryEmitter.class, moduleQueryDataSet);

        // create a pipeline starting with the mapper
        final PCollection<Record> out = pipeline.apply(TalendIO.read(mapper));

        // then append some assertions to the output of the mapper,
        // PAssert is a beam utility to validate part of the pipeline
        PAssert.that(out).satisfies(it -> {
            final List<Record> records = StreamSupport.stream(it.spliterator(), false).collect(toList());
            Assert.assertEquals(10, records.size());
            assertTrue(records.iterator().next().getString("Name").contains("TEST_Name"));
            Assertions.assertEquals(1, records.iterator().next().getSchema().getEntries().size());
            return null;
        });

        // finally run the pipeline and ensure it was successful - i.e. data were validated
        Assert.assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }

    @Test
    @DisplayName("Bad credentials case")
    public void inputWithBadCredential() {
        final BasicDataStore datstore = new BasicDataStore();
        datstore.setEndpoint(URL);
        datstore.setUserId("badUser");
        datstore.setPassword("badPasswd");
        datstore.setSecurityKey("badSecurityKey");
        final ModuleQueryDataSet moduleQueryDataSet = new ModuleQueryDataSet();
        moduleQueryDataSet.setModuleName("account");
        moduleQueryDataSet.setDataStore(datstore);
        final String config = configurationByExample().forInstance(moduleQueryDataSet).configured().toQueryString();
        final IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job
                        .components()
                        .component("salesforce-input", "Salesforce://Input?" + config)
                        .component("collector", "test://collector")
                        .connections()
                        .from("salesforce-input")
                        .to("collector")
                        .build()
                        .run());
    }

    @Test
    @DisplayName("Module selection case [invalid]")
    public void inputWithModuleNameInvalid() {
        final ModuleQueryDataSet moduleQueryDataSet = new ModuleQueryDataSet();
        moduleQueryDataSet.setModuleName("invalid0");
        moduleQueryDataSet.setDataStore(dataStore);
        final String config = configurationByExample().forInstance(moduleQueryDataSet).configured().toQueryString();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job
                        .components()
                        .component("salesforce-input", "Salesforce://ModuleQueryInput?" + config)
                        .component("collector", "test://collector")
                        .connections()
                        .from("salesforce-input")
                        .to("collector")
                        .build()
                        .run());
    }

    @Test
    @DisplayName("Module selection with fields case [invalid]")
    public void inputWithModuleNameValidAndInvalidField() {
        final ModuleQueryDataSet moduleQueryDataSet = new ModuleQueryDataSet();
        moduleQueryDataSet.setModuleName("account");
        moduleQueryDataSet.setSelectColumnIds(singletonList("InvalidField10x"));
        moduleQueryDataSet.setDataStore(dataStore);
        final String config = configurationByExample().forInstance(moduleQueryDataSet).configured().toQueryString();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job
                        .components()
                        .component("salesforce-input", "Salesforce://ModuleQueryInput?" + config)
                        .component("collector", "test://collector")
                        .connections()
                        .from("salesforce-input")
                        .to("collector")
                        .build()
                        .run());
    }

}