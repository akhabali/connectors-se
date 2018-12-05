package org.talend.components.salesforce;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.talend.components.salesforce.service.SalesforceService.URL;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.talend.components.salesforce.dataset.ModuleDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.components.salesforce.input.ModuleQueryEmitter;
import org.talend.components.salesforce.output.OutputConfiguration;
import org.talend.components.salesforce.output.SalesforceOutput;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;
import org.talend.sdk.component.runtime.beam.TalendIO;
import org.talend.sdk.component.runtime.beam.transform.ViewsMappingTransform;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.output.Processor;

public class SalesforceTestBase implements Serializable {

    @ClassRule
    public static final SimpleComponentRule COMPONENT_FACTORY = new SimpleComponentRule("org.talend.components.salesforce");

    @Rule
    public transient final TestPipeline pipeCheck = TestPipeline.create();

    @Rule
    public transient final TestPipeline pipeline = TestPipeline.create();

    public static String USER_ID;

    public static String PASSWORD;

    public static String SECURITY_KEY;

    protected BasicDataStore dataStore;

    static {
        final MavenDecrypter decrypter = new MavenDecrypter();
        final Server serverWithPassword = decrypter.find("salesforce-password");
        final Server serverWithSecuritykey = decrypter.find("salesforce-securitykey");
        USER_ID = serverWithPassword.getUsername();
        PASSWORD = serverWithPassword.getPassword();
        SECURITY_KEY = serverWithSecuritykey.getPassword();
    }

    @Before
    public void setUp() {
        dataStore = new BasicDataStore();
        dataStore.setEndpoint(URL);
        dataStore.setUserId(USER_ID);
        dataStore.setPassword(PASSWORD);
        dataStore.setSecurityKey(SECURITY_KEY);
    }

    protected void cleanTestRecords(String module, String condition, String plugin) {

        final ModuleDataSet inputDataSet = new ModuleDataSet();
        inputDataSet.setModuleName(module);
        ModuleDataSet.ColumnSelectionConfig selectionConfig = new ModuleDataSet.ColumnSelectionConfig();
        selectionConfig.setSelectColumnNames(Arrays.asList("Id"));
        inputDataSet.setColumnSelectionConfig(selectionConfig);
        inputDataSet.setDataStore(dataStore);
        inputDataSet.setCondition(condition);

        final Mapper mapper = COMPONENT_FACTORY.createMapper(ModuleQueryEmitter.class, inputDataSet);

        // output
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet outDataSet = new ModuleDataSet();
        outDataSet.setModuleName(module);
        outDataSet.setDataStore(dataStore);
        configuration.setOutputAction(OutputConfiguration.OutputAction.DELETE);
        configuration.setModuleDataSet(outDataSet);

        // We create the component processor instance using the configuration filled above
        final Processor processor = COMPONENT_FACTORY.createProcessor(SalesforceOutput.class, configuration);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // Create beam input from mapper and apply input to pipeline
        pipeline.apply(TalendIO.read(mapper)).apply(new ViewsMappingTransform(emptyMap(), plugin))
                .apply(TalendIO.write(processor));

        // run the pipeline and ensure the execution was successful
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }

    protected void checkModuleData(String module, String condition, int expected) {
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName(module);
        ModuleDataSet.ColumnSelectionConfig selectionConfig = new ModuleDataSet.ColumnSelectionConfig();
        selectionConfig.setSelectColumnNames(Arrays.asList("Id"));
        moduleDataSet.setColumnSelectionConfig(selectionConfig);
        moduleDataSet.setDataStore(dataStore);
        moduleDataSet.setCondition(condition);

        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = COMPONENT_FACTORY.createMapper(ModuleQueryEmitter.class, moduleDataSet);

        // create a pipeline starting with the mapper
        final PCollection<Record> out = pipeCheck.apply(TalendIO.read(mapper));

        // then append some assertions to the output of the mapper,
        // PAssert is a beam utility to validate part of the pipeline
        PAssert.that(out).satisfies(it -> {
            final List<Record> records = StreamSupport.stream(it.spliterator(), false).collect(toList());
            Assert.assertEquals(expected, records.size());
            return null;
        });

        // finally run the pipeline and ensure it was successful - i.e. data were validated
        Assert.assertEquals(PipelineResult.State.DONE, pipeCheck.run().waitUntilFinish());
    }

}