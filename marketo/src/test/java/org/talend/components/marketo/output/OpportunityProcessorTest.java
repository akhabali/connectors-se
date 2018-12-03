// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo.output;

import static org.talend.components.marketo.MarketoApiConstants.ATTR_DEDUPE_FIELDS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_EXTERNAL_OPPORTUNITY_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LEAD_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ROLE;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.marketo.MarketoBaseTest;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration.DeleteBy;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration.OutputAction;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration.SyncMethod;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true, responseLocator = org.talend.sdk.component.junit.http.internal.impl.MarketoResponseLocator.class)
@WithComponents("org.talend.components.marketo")
class OpportunityProcessorTest extends MarketoBaseTest {

    public static final String OPPORTUNITY_101 = "opportunity102";

    private Record dataOR;

    private Record dataNotExistOR;

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();
        outputConfiguration.getDataSet().setEntity(MarketoEntity.Opportunity);
        outputConfiguration.setSyncMethod(SyncMethod.createOrUpdate);
        outputConfiguration.setDedupeBy(ATTR_DEDUPE_FIELDS);
        outputConfiguration.setDeleteBy(DeleteBy.dedupeFields);
        // create our opportunity
        data = service.getRecordBuilder().newRecordBuilder().withString(ATTR_EXTERNAL_OPPORTUNITY_ID, OPPORTUNITY_101).build();
        // create our opportunityRole
        dataOR = service.getRecordBuilder().newRecordBuilder().withString(ATTR_EXTERNAL_OPPORTUNITY_ID, OPPORTUNITY_101) //
                .withString(ATTR_ROLE, "newCust") //
                .withInt(ATTR_LEAD_ID, 4) //
                .build();
        // not existing opportunity
        dataNotExist = service.getRecordBuilder().newRecordBuilder()
                .withString(ATTR_EXTERNAL_OPPORTUNITY_ID, "XxXOppportunityXxX").build();
        // not existing opportunityRole
        dataNotExistOR = service.getRecordBuilder().newRecordBuilder()
                .withString(ATTR_EXTERNAL_OPPORTUNITY_ID, "XxXOppportunityXxX") //
                .withString(ATTR_ROLE, "newCust") //
                .withInt(ATTR_LEAD_ID, 4) //
                .build();
        //
        // outputConfiguration.setAction(OutputAction.sync);
        // initProcessor();
        // processor.map(data, main -> assertEquals(0, main.getInt(ATTR_SEQ)), reject -> fail(FAIL_REJECT));
        // outputConfiguration.getDataSet().setEntity(MarketoEntity.OpportunityRole);
        // outputConfiguration.setAction(OutputAction.sync);
        // initProcessor();
        // processor.map(dataOR, main -> assertEquals(0, main.getInt(ATTR_SEQ)), reject -> fail(FAIL_REJECT));
        // // be sure that those one do not exist
        // outputConfiguration.getDataSet().setEntity(MarketoEntity.OpportunityRole);
        // outputConfiguration.setAction(OutputAction.delete);
        // initProcessor();
        // processor.map(dataNotExistOR, main -> {
        // }, reject -> {
        // });
        // outputConfiguration.getDataSet().setEntity(MarketoEntity.Opportunity);
        // initProcessor();
        // processor.map(dataNotExist, main -> {
        // }, reject -> {
        // });
        //
        //
        outputConfiguration.getDataSet().setEntity(MarketoEntity.Opportunity);
    }

    private void initProcessor() {
        processor = new MarketoProcessor(outputConfiguration, service);
        processor.init();
    }

    @Test
    void testSyncOpportunityRole() {
        outputConfiguration.getDataSet().setEntity(MarketoEntity.OpportunityRole);
        outputConfiguration.setAction(OutputAction.sync);
        initProcessor();
        processor.map(dataOR);
    }

    @Test
    void testSyncOpportunityRoleFail() {
        Assertions.assertThrows(RuntimeException.class, () -> {
            outputConfiguration.getDataSet().setEntity(MarketoEntity.OpportunityRole);
            outputConfiguration.setAction(OutputAction.sync);
            outputConfiguration.setSyncMethod(SyncMethod.updateOnly);
            initProcessor();
            processor.map(dataNotExistOR);

        });
    }

    @Test
    void testDeleteOpportunityRole() {
        outputConfiguration.getDataSet().setEntity(MarketoEntity.OpportunityRole);
        outputConfiguration.setAction(OutputAction.delete);
        initProcessor();
        processor.map(dataOR);
    }

    @Test
    void testDeleteOpportunityRoleFail() {
        Assertions.assertThrows(RuntimeException.class, () -> {
            outputConfiguration.getDataSet().setEntity(MarketoEntity.OpportunityRole);
            outputConfiguration.setAction(OutputAction.delete);
            initProcessor();
            processor.map(dataNotExistOR);
        });
    }

}
