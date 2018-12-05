package org.talend.components.salesforce.input;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.ziplock.JarLocation.jarLocation;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;
import static org.talend.components.salesforce.service.SalesforceService.URL;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.talend.components.salesforce.SalesforceTestBase;
import org.talend.components.salesforce.dataset.ModuleDataSet;
import org.talend.components.salesforce.dataset.SOQLQueryDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.components.salesforce.output.OutputConfiguration;
import org.talend.components.salesforce.output.SalesforceOutput;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.JoinInputFactory;
import org.talend.sdk.component.junit.beam.Data;
import org.talend.sdk.component.runtime.beam.TalendFn;
import org.talend.sdk.component.runtime.beam.TalendIO;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.manager.chain.Job;
import org.talend.sdk.component.runtime.output.Processor;

@FixMethodOrder(NAME_ASCENDING)
public class SalesforceInputBeamTest extends SalesforceTestBase {

    private static final String PLUGIN = jarLocation(SalesforceInputBeamTest.class).getAbsolutePath();

    private static final String UNIQUE_ID = Integer.toString(ThreadLocalRandom.current().nextInt(1, 100000));

    @Rule
    public transient final TestPipeline pipePre = TestPipeline.create();

    private RecordBuilderFactory factory = COMPONENT_FACTORY.findService(RecordBuilderFactory.class);

    @Test
    public void test00_prepareData() {
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        moduleDataSet.setDataStore(dataStore);
        configuration.setOutputAction(OutputConfiguration.OutputAction.INSERT);
        configuration.setModuleDataSet(moduleDataSet);

        // We create the component processor instance using the configuration filled above
        final Processor processor = COMPONENT_FACTORY.createProcessor(SalesforceOutput.class, configuration);

        List<Record> records = new ArrayList<>(10);
        for (int i = 0; i < 300; i++) {
            Record.Builder recordBuilder = factory.newRecordBuilder();
            recordBuilder.withString("Name", "TestName_" + i + "_" + UNIQUE_ID);
            if (i % 40 == 0) {
                recordBuilder.withInt("NumberOfEmployees", 8000);
            }
            if (i % 80 == 0) {
                factory.newRecordBuilder().withDouble("AnnualRevenue", 5000.0);
            }
            records.add(recordBuilder.build());
        }

        // The join input factory construct inputs test data for every input branch you have defined for this component
        // Make sure to fil in some test data for the branches you want to test
        // You can also remove the branches that you don't need from the factory below
        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__", records);

        // Convert it to a beam "source"
        final PCollection<Record> inputs = pipePre.apply(Data.of(processor.plugin(), joinInputFactory.asInputRecords()));

        // add our processor right after to see each data as configured previously
        inputs.apply(TalendFn.asFn(processor)).apply(Data.map(processor.plugin(), Record.class));

        // run the pipeline and ensure the execution was successful
        assertEquals(PipelineResult.State.DONE, pipePre.run().waitUntilFinish());

    }

    // Module Query part
    @Test
    public void test01_ModuleQuery() {
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        ModuleDataSet.ColumnSelectionConfig selectionConfig = new ModuleDataSet.ColumnSelectionConfig();
        selectionConfig.setSelectColumnNames(Arrays.asList("Id", "Name"));
        moduleDataSet.setColumnSelectionConfig(selectionConfig);
        moduleDataSet.setDataStore(dataStore);
        moduleDataSet.setCondition("Name Like 'TestName_%" + UNIQUE_ID + "%'");

        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = COMPONENT_FACTORY.createMapper(ModuleQueryEmitter.class, moduleDataSet);

        // create a pipeline starting with the mapper
        final PCollection<Record> out = pipeline.apply(TalendIO.read(mapper));

        // then append some assertions to the output of the mapper,
        // PAssert is a beam utility to validate part of the pipeline
        PAssert.that(out).satisfies(it -> {
            final List<Record> records = StreamSupport.stream(it.spliterator(), false).collect(toList());
            assertEquals(300, records.size());
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
    public void test02_ModuleQueryWithLimit() {
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        ModuleDataSet.ColumnSelectionConfig selectionConfig = new ModuleDataSet.ColumnSelectionConfig();
        selectionConfig.setSelectColumnNames(Arrays.asList("Name"));
        moduleDataSet.setColumnSelectionConfig(selectionConfig);
        moduleDataSet.setDataStore(dataStore);
        moduleDataSet.setCondition("Name Like 'TestName_%" + UNIQUE_ID + "%' limit 10");

        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = COMPONENT_FACTORY.createMapper(ModuleQueryEmitter.class, moduleDataSet);

        // create a pipeline starting with the mapper
        final PCollection<Record> out = pipeline.apply(TalendIO.read(mapper));

        // then append some assertions to the output of the mapper,
        // PAssert is a beam utility to validate part of the pipeline
        PAssert.that(out).satisfies(it -> {
            final List<Record> records = StreamSupport.stream(it.spliterator(), false).collect(toList());
            Assert.assertEquals(10, records.size());

            records.stream().forEach(r -> assertTrue(records.iterator().next().getString("Name").contains("TestName")));
            return null;
        });

        // finally run the pipeline and ensure it was successful - i.e. data were validated
        Assert.assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }

    @Test
    public void test03_AllType() {
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        ModuleDataSet.ColumnSelectionConfig selectionConfig = new ModuleDataSet.ColumnSelectionConfig();
        selectionConfig.setSelectColumnNames(Arrays.asList("Id", "IsDeleted", "MasterRecordId", "Name", "Type", "ParentId",
                "BillingStreet", "BillingCity", "BillingState", "BillingPostalCode", "BillingCountry", "BillingLatitude",
                "BillingLongitude", "ShippingStreet", "ShippingCity", "ShippingState", "ShippingPostalCode", "ShippingCountry",
                "ShippingLatitude", "ShippingLongitude", "Phone", "Fax", "AccountNumber", "Website", "PhotoUrl", "Sic",
                "Industry", "AnnualRevenue", "NumberOfEmployees", "Ownership", "TickerSymbol", "Description", "Rating", "Site",
                "OwnerId", "CreatedDate", "CreatedById", "LastModifiedDate", "LastModifiedById", "SystemModstamp",
                "LastActivityDate", "LastViewedDate", "LastReferencedDate", "Jigsaw", "JigsawCompanyId", "AccountSource",
                "SicDesc"));
        moduleDataSet.setColumnSelectionConfig(selectionConfig);
        moduleDataSet.setDataStore(dataStore);
        moduleDataSet.setCondition("Name Like 'TestName_%" + UNIQUE_ID + "%' and NumberOfEmployees != null");

        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = COMPONENT_FACTORY.createMapper(ModuleQueryEmitter.class, moduleDataSet);

        // create a pipeline starting with the mapper
        final PCollection<Record> out = pipeline.apply(TalendIO.read(mapper));

        // then append some assertions to the output of the mapper,
        // PAssert is a beam utility to validate part of the pipeline
        PAssert.that(out).satisfies(it -> {
            final List<Record> records = StreamSupport.stream(it.spliterator(), false).collect(toList());
            Assert.assertEquals(8, records.size());
            Record record = records.get(0);
            assertEquals(8000, record.getInt("NumberOfEmployees"));
            return null;
        });

        // finally run the pipeline and ensure it was successful - i.e. data were validated
        Assert.assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }

    @Test
    public void test03_inputWithBadCredential() {
        final BasicDataStore datstore = new BasicDataStore();
        datstore.setEndpoint(URL);
        datstore.setUserId("badUser");
        datstore.setPassword("badPasswd");
        datstore.setSecurityKey("badSecurityKey");
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("account");
        moduleDataSet.setDataStore(datstore);
        final String config = configurationByExample().forInstance(moduleDataSet).configured().toQueryString();
        final IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job.components().component("salesforce-input", "Salesforce://Input?" + config)
                        .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build()
                        .run());
    }

    @Test
    public void test04_ModuleQueryWithInvalidName() {
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("invalid0");
        moduleDataSet.setDataStore(dataStore);
        final String config = configurationByExample().forInstance(moduleDataSet).configured().toQueryString();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job.components().component("salesforce-input", "Salesforce://ModuleQueryInput?" + config)
                        .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build()
                        .run());
    }

    @Test
    public void test05_ModuleQueryWithInvalidField() {
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("account");
        ModuleDataSet.ColumnSelectionConfig selectionConfig = new ModuleDataSet.ColumnSelectionConfig();
        selectionConfig.setSelectColumnNames(singletonList("InvalidField10x"));
        moduleDataSet.setColumnSelectionConfig(selectionConfig);
        moduleDataSet.setDataStore(dataStore);
        final String config = configurationByExample().forInstance(moduleDataSet).configured().toQueryString();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job.components().component("salesforce-input", "Salesforce://ModuleQueryInput?" + config)
                        .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build()
                        .run());
    }

    // SOQL Query part
    @Test
    public void test10_SOQLQueryBasicCase() {
        final SOQLQueryDataSet soqlQueryDataSet = new SOQLQueryDataSet();
        soqlQueryDataSet.setQuery("select Id,Name,IsDeleted from account where Name Like 'TestName_%" + UNIQUE_ID + "%'");
        soqlQueryDataSet.setDataStore(dataStore);

        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = COMPONENT_FACTORY.createMapper(SOQLQueryEmitter.class, soqlQueryDataSet);

        // create a pipeline starting with the mapper
        final PCollection<Record> out = pipeline.apply(TalendIO.read(mapper));

        // then append some assertions to the output of the mapper,
        // PAssert is a beam utility to validate part of the pipeline
        PAssert.that(out).satisfies(it -> {
            final List<Record> records = StreamSupport.stream(it.spliterator(), false).collect(toList());
            Assert.assertEquals(300, records.size());
            Record record = records.get(0);
            Schema schema = record.getSchema();
            assertNotNull(schema);
            assertEquals(3, schema.getEntries().size());
            assertTrue(record.getString("Name").contains("TestName"));
            return null;
        });

        // finally run the pipeline and ensure it was successful - i.e. data were validated
        Assert.assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }

    @Test
    public void test11_SOQLQueryWithEmptyResult() {
        final SOQLQueryDataSet soqlQueryDataSet = new SOQLQueryDataSet();
        soqlQueryDataSet.setQuery("select  name from account where name = 'this name will never exist $'");
        soqlQueryDataSet.setDataStore(dataStore);

        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = COMPONENT_FACTORY.createMapper(SOQLQueryEmitter.class, soqlQueryDataSet);

        // create a pipeline starting with the mapper
        final PCollection<Record> out = pipeline.apply(TalendIO.read(mapper));

        // then append some assertions to the output of the mapper,
        // PAssert is a beam utility to validate part of the pipeline
        PAssert.that(out).satisfies(it -> {
            final List<Record> records = StreamSupport.stream(it.spliterator(), false).collect(toList());
            Assert.assertEquals(0, records.size());
            return null;
        });

        // finally run the pipeline and ensure it was successful - i.e. data were validated
        Assert.assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }

    @Test
    public void test12_SOQLQueryWithInvalidQuery() {
        final SOQLQueryDataSet soqlQueryDataSet = new SOQLQueryDataSet();
        soqlQueryDataSet.setQuery("from account");
        soqlQueryDataSet.setDataStore(dataStore);
        final String config = configurationByExample().forInstance(soqlQueryDataSet).configured().toQueryString();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job.components().component("salesforce-input", "Salesforce://SOQLQueryInput?" + config)
                        .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build()
                        .run());
    }

    @Test
    public void test99_cleanupModuleAccount() {
        cleanTestRecords("Account", "Name Like '%" + UNIQUE_ID + "%'", PLUGIN);
        checkModuleData("Account", "Name Like '%" + UNIQUE_ID + "%'", 0);
    }

}