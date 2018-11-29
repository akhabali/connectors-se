/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package org.talend.components.salesforce.output;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;
import org.talend.components.salesforce.SalesforceBaseTest;
import org.talend.components.salesforce.dataset.ModuleDataSet;
import org.talend.components.salesforce.dataset.SOQLQueryDataSet;
import org.talend.components.salesforce.input.SOQLQueryEmitter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.JoinInputFactory;
import org.talend.sdk.component.junit.beam.Data;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.beam.TalendFn;
import org.talend.sdk.component.runtime.beam.TalendIO;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.output.Processor;

@Ignore("TODO improve")
@WithComponents("org.talend.components.salesforce")
public class SalesforceOutputBeamTest extends SalesforceBaseTest {

    private static final String UNIQUE_ID = Integer.toString(ThreadLocalRandom.current().nextInt(1, 100000));

    @Rule
    public transient final TestPipeline writePip = TestPipeline.create();

    @Rule
    public transient final TestPipeline readPip = TestPipeline.create();

    private RecordBuilderFactory factory = COMPONENT_FACTORY.findService(RecordBuilderFactory.class);

    @Test
    @DisplayName("test insert")
    public void testInsert() {
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        moduleDataSet.setDataStore(dataStore);
        configuration.setOutputAction(OutputConfiguration.OutputAction.INSERT);
        configuration.setModuleDataSet(moduleDataSet);

        // We create the component processor instance using the configuration filled above
        final Processor processor = COMPONENT_FACTORY.createProcessor(SalesforceOutput.class, configuration);

        Record record = factory.newRecordBuilder().withString("Name", "test_" + UNIQUE_ID).build();

        // The join input factory construct inputs test data for every input branch you have defined for this component
        // Make sure to fil in some test data for the branches you want to test
        // You can also remove the branches that you don't need from the factory below
        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__", asList(record));

        // Convert it to a beam "source"
        final PCollection<Record> inputs = writePip.apply(Data.of(processor.plugin(), joinInputFactory.asInputRecords()));

        // add our processor right after to see each data as configured previously
        inputs.apply(TalendFn.asFn(processor)).apply(Data.map(processor.plugin(), Record.class));

        // run the writePip and ensure the execution was successful
        assertEquals(PipelineResult.State.DONE, writePip.run().waitUntilFinish());
    }

    @Test
    @DisplayName("test update")
    public void testUpdate() {
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        moduleDataSet.setDataStore(dataStore);
        configuration.setOutputAction(OutputConfiguration.OutputAction.UPDATE);
        configuration.setModuleDataSet(moduleDataSet);

        // We create the component processor instance using the configuration filled above
        final Processor processor = COMPONENT_FACTORY.createProcessor(SalesforceOutput.class, configuration);

        Record record = factory.newRecordBuilder().withString("Id", "0019000002AhPGSAA3")
                .withString("Name", "test_update_" + UNIQUE_ID).build();

        // The join input factory construct inputs test data for every input branch you have defined for this component
        // Make sure to fil in some test data for the branches you want to test
        // You can also remove the branches that you don't need from the factory below
        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__", asList(record));

        // Convert it to a beam "source"
        final PCollection<Record> inputs = writePip.apply(Data.of(processor.plugin(), joinInputFactory.asInputRecords()));

        // add our processor right after to see each data as configured previously
        inputs.apply(TalendFn.asFn(processor)).apply(Data.map(processor.plugin(), Record.class));

        // run the writePip and ensure the execution was successful
        assertEquals(PipelineResult.State.DONE, writePip.run().waitUntilFinish());
    }

    @Test
    @DisplayName("test upsert")
    public void testUpsert() {
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Contact");
        moduleDataSet.setDataStore(dataStore);
        configuration.setOutputAction(OutputConfiguration.OutputAction.UPSERT);
        configuration.setUpsertKeyColumn("Email");
        configuration.setModuleDataSet(moduleDataSet);

        // We create the component processor instance using the configuration filled above
        final Processor processor = COMPONENT_FACTORY.createProcessor(SalesforceOutput.class, configuration);

        Record record_1 = factory.newRecordBuilder().withString("Email", "aaa_" + UNIQUE_ID + "@test.com")
                .withString("FirstName", "F1_" + UNIQUE_ID).withString("LastName", "L1_" + UNIQUE_ID).build();
        Record record_2 = factory.newRecordBuilder().withString("Email", "bbb_" + UNIQUE_ID + "@test.com")
                .withString("FirstName", "F2_" + UNIQUE_ID).withString("LastName", "L2_" + UNIQUE_ID).build();
        Record record_3 = factory.newRecordBuilder().withString("Email", "aaa_" + UNIQUE_ID + "@test.com")
                .withString("FirstName", "F1_UPDATE_" + UNIQUE_ID).withString("LastName", "L1_UPDATE_" + UNIQUE_ID).build();

        // The join input factory construct inputs test data for every input branch you have defined for this component
        // Make sure to fil in some test data for the branches you want to test
        // You can also remove the branches that you don't need from the factory below
        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__",
                asList(record_1, record_2, record_3));

        // Convert it to a beam "source"
        final PCollection<Record> inputs = writePip.apply(Data.of(processor.plugin(), joinInputFactory.asInputRecords()));

        // add our processor right after to see each data as configured previously
        inputs.apply(TalendFn.asFn(processor)).apply(Data.map(processor.plugin(), Record.class));

        // run the writePip and ensure the execution was successful
        assertEquals(PipelineResult.State.DONE, writePip.run().waitUntilFinish());
    }

    @Test
    public void clean() {
        final SOQLQueryDataSet queryDataSet = new SOQLQueryDataSet();
        // queryDataSet.setQuery("Select Id,Name,CreatedDate from Account where Name like '%test_" + UNIQUE_ID + "%'");
        queryDataSet.setQuery("Select Id,Name,CreatedDate from Account");
        queryDataSet.setDataStore(dataStore);

        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = COMPONENT_FACTORY.createMapper(SOQLQueryEmitter.class, queryDataSet);

        // create a writePip starting with the mapper
        final PCollection<Record> out = readPip.apply(TalendIO.read(mapper));
        PAssert.that(out).satisfies(it -> {
            final List<Record> records = StreamSupport.stream(it.spliterator(), false).collect(toList());
            cleanupAllRecords(records);
            return null;
        });

        // finally run the writePip and ensure it was successful - i.e. data were validated
        Assert.assertEquals(PipelineResult.State.DONE, readPip.run().waitUntilFinish());

    }

    public void cleanupAllRecords(List<Record> deletedRecords) {
        if (deletedRecords.size() == 0) {
            return;
        }
        final OutputConfiguration outputConfig = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setDataStore(dataStore);
        moduleDataSet.setModuleName("Account");
        outputConfig.setOutputAction(OutputConfiguration.OutputAction.DELETE);
        outputConfig.setModuleDataSet(moduleDataSet);
        final Processor processor = COMPONENT_FACTORY.createProcessor(SalesforceOutput.class, outputConfig);

        // The join input factory construct inputs test data for every input branch you have defined for this component
        // Make sure to fil in some test data for the branches you want to test
        // You can also remove the branches that you don't need from the factory below
        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__", deletedRecords);

        // Convert it to a beam "source"
        final PCollection<Record> inputs = writePip.apply(Data.of(processor.plugin(), joinInputFactory.asInputRecords()));

        // add our processor right after to see each data as configured previously
        inputs.apply(TalendFn.asFn(processor)).apply(Data.map(processor.plugin(), Record.class));

        // finally run the writePip and ensure it was successful - i.e. data were validated
        Assert.assertEquals(PipelineResult.State.DONE, writePip.run().waitUntilFinish());

        deletedRecords.clear();
    }

}