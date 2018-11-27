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

package org.talend.components.salesforce.input;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

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
import org.talend.components.salesforce.dataset.SOQLQueryDataSet;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.runtime.beam.TalendIO;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.manager.chain.Job;

@DisplayName("Suite of test for the Salesforce Input with beam")
public class SalesforceInputSOQLBeamTest extends SalesforceBaseTest {

    @Test
    @DisplayName("Soql query selection [valid]")
    public void inputWithSoqlQueryValid() {
        final SOQLQueryDataSet soqlQueryDataSet = new SOQLQueryDataSet();
        soqlQueryDataSet.setQuery("select Name from account where Name Like  '%TEST_Name%'");
        soqlQueryDataSet.setDataStore(dataStore);

        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = COMPONENT_FACTORY.createMapper(SOQLQueryEmitter.class, soqlQueryDataSet);

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
    @DisplayName("Soql query selection [empty result]")
    public void inputWithSoqlQueryEmptyResult() {
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
    @DisplayName("Soql query selection [invalid]")
    public void inputWithSoqlQueryInvalid() {
        final SOQLQueryDataSet soqlQueryDataSet = new SOQLQueryDataSet();
        soqlQueryDataSet.setQuery("from account");
        soqlQueryDataSet.setDataStore(dataStore);
        final String config = configurationByExample().forInstance(soqlQueryDataSet).configured().toQueryString();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job
                        .components()
                        .component("salesforce-input", "Salesforce://SOQLQueryInput?" + config)
                        .component("collector", "test://collector")
                        .connections()
                        .from("salesforce-input")
                        .to("collector")
                        .build()
                        .run());
    }

}