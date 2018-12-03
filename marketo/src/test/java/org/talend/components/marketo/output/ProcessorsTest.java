package org.talend.components.marketo.output;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_DEDUPE_FIELDS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_EMAIL;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_EXTERNAL_OPPORTUNITY_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.talend.components.marketo.MarketoBaseTest;
import org.talend.components.marketo.component.DataCollector;
import org.talend.components.marketo.dataset.CompoundKey;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoInputConfiguration.LeadAction;
import org.talend.components.marketo.dataset.MarketoInputConfiguration.OtherEntityAction;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration.DeleteBy;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration.OutputAction;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration.SyncMethod;
import org.talend.components.marketo.input.LeadSource;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true, responseLocator = org.talend.sdk.component.junit.http.internal.impl.MarketoResponseLocator.class)
@WithComponents("org.talend.components.marketo")
class ProcessorsTest extends MarketoBaseTest {

    String generator;

    String expectedErrorMessage;

    String action;

    String keyName;

    String keyValue;

    void ensureLeadExist() {
        outputConfiguration.setAction(OutputAction.sync);
        outputConfiguration.setSyncMethod(SyncMethod.createOrUpdate);
        outputConfiguration.setLookupField(ATTR_EMAIL);
        processor = new MarketoProcessor(outputConfiguration, service);
        processor.init();
        data = service.getRecordBuilder().newRecordBuilder().withString(ATTR_EMAIL, "egallois@talend.com")
                .withString("firstName", "Emmanuel").build();
        processor.map(data);

        inputConfiguration.setLeadAction(LeadAction.getMultipleLeads);
        inputConfiguration.setLeadKeyName(ATTR_EMAIL);
        inputConfiguration.setLeadKeyValues("egallois@talend.com");
        LeadSource source = new LeadSource(inputConfiguration, service);
        source.init();
        Record result;
        keyName = ATTR_ID;
        while ((result = source.next()) != null) {
            keyValue = String.valueOf(result.getInt(ATTR_ID));
            assertNotNull(result);
        }

        if ("sync".equals(action)) {
            keyName = ATTR_EMAIL;
            keyValue = "egallois@talend.com";
        }

    }

    void initTest(String entity) {
        generator = entity + "Generator";
        expectedErrorMessage = "[1013] Record not found";
        outputConfiguration.getDataSet().setEntity(MarketoEntity.valueOf(entity));
        inputConfiguration.getDataSet().setEntity(MarketoEntity.valueOf(entity));
        switch (MarketoEntity.valueOf(entity)) {
        case Lead:
            expectedErrorMessage = "[1004] Lead not found";
            ensureLeadExist();
            if (action.equals("sync")) {
                inputConfiguration.setLeadAction(LeadAction.getMultipleLeads);
                inputConfiguration.setLeadKeyName(keyName);
                inputConfiguration.setLeadKeyValues(keyValue);
            } else {
                inputConfiguration.setLeadAction(LeadAction.getLead);
                inputConfiguration.setLeadId(Integer.parseInt(keyValue));
            }
            break;
        case List:
            break;
        case CustomObject:

            outputConfiguration.setCustomObjectName("car_c");
            outputConfiguration.setDedupeBy("dedupeFields");

            inputConfiguration.setOtherAction(OtherEntityAction.get);
            inputConfiguration.setCustomObjectName("car_c");
            inputConfiguration.setFilterType("dedupeFields");
            inputConfiguration.setUseCompoundKey(true);
            List<CompoundKey> compoundKey = new ArrayList<>();
            compoundKey.add(new CompoundKey("customerId", "3"));
            compoundKey.add(new CompoundKey("VIN", "ABC-DEF-12345-GIN"));
            inputConfiguration.setCompoundKey(compoundKey);
            inputConfiguration.setFields(asList("createdAt,marketoGUID,updatedAt,VIN,customerId,model,year".split(",")));

            break;
        // company
        case Company:
            outputConfiguration.setDedupeBy("dedupeFields");

            inputConfiguration.setOtherAction(OtherEntityAction.get);
            inputConfiguration.setFilterType("externalCompanyId");
            inputConfiguration.setFilterValues("google666");
            inputConfiguration.setFields(asList("mainPhone", "company", "website"));

            data = service.getRecordBuilder().newRecordBuilder().withString("externalCompanyId", "google666").build();
            // we create a record
            outputConfiguration.setAction(OutputAction.sync);
            outputConfiguration.setSyncMethod(SyncMethod.createOrUpdate);
            processor = new MarketoProcessor(outputConfiguration, service);
            processor.init();
            processor.map(data);

            break;
        case Opportunity:
        case OpportunityRole:
            outputConfiguration.setDedupeBy(ATTR_DEDUPE_FIELDS);
            outputConfiguration.setDeleteBy(DeleteBy.dedupeFields);

            inputConfiguration.setOtherAction(OtherEntityAction.get);
            inputConfiguration.setFilterType(ATTR_EXTERNAL_OPPORTUNITY_ID);
            inputConfiguration.setFilterValues("opportunity102");

            break;
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "Company", "Lead", "CustomObject", "Opportunity" })
    // "Opportunity", "OpportunityRole" })
    void deleteFail(String entity) {
        action = "delete";
        initTest(entity);
        final Exception error = assertThrows(Exception.class, () -> {
            outputConfiguration.setAction(OutputAction.delete);
            outputConfiguration.setDeleteBy(DeleteBy.dedupeFields);
            final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
            final String inputConfig = "config.isInvalid=true";
            runOutputPipeline(generator, inputConfig, config);
        });
        assertTrue(error.getMessage().contains(expectedErrorMessage));
    }

    @ParameterizedTest
    @ValueSource(strings = { "Company", "Lead", "CustomObject", "Opportunity" })
    void delete(String entity) {
        action = "delete";
        initTest(entity);
        outputConfiguration.setAction(OutputAction.delete);
        outputConfiguration.setDeleteBy(DeleteBy.dedupeFields);
        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        final String inputConfig = "config.isInvalid=false&config.keyName=" + keyName + "&config.keyValue=" + keyValue;
        runOutputPipeline(generator, inputConfig, config);
        //
        runInputPipeline(configurationByExample().forInstance(inputConfiguration).configured().toQueryString());
        assertEquals(0, DataCollector.getData().size());
    }

    @ParameterizedTest
    @ValueSource(strings = { "Company", "Lead", "CustomObject", "Opportunity" })
    void syncFail(String entity) {
        action = "sync";
        initTest(entity);
        final Exception error = assertThrows(Exception.class, () -> {
            outputConfiguration.setAction(OutputAction.sync);
            outputConfiguration.setSyncMethod(SyncMethod.updateOnly);
            final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
            final String inputConfig = "config.isInvalid=true&config.action=sync";
            runOutputPipeline(generator, inputConfig, config);
        });
        assertTrue(error.getMessage().contains(expectedErrorMessage));
    }

    @ParameterizedTest
    @ValueSource(strings = { "Company", "Lead", "CustomObject", "Opportunity" })
    void sync(String entity) {
        action = "sync";
        initTest(entity);
        outputConfiguration.setAction(OutputAction.sync);
        outputConfiguration.setSyncMethod(SyncMethod.createOrUpdate);
        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        final String inputConfig = "config.isInvalid=false&config.action=sync&config.keyName=" + keyName + "&config.keyValue="
                + keyValue;
        runOutputPipeline(generator, inputConfig, config);
        //
        runInputPipeline(configurationByExample().forInstance(inputConfiguration).configured().toQueryString());
        assertEquals(1, DataCollector.getData().size());
    }

}
