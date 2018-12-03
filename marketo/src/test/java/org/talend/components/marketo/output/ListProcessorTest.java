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

import static org.junit.Assert.*;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LEAD_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LIST_ID;
import static org.talend.components.marketo.component.ListGeneratorSource.LEAD_ID_ADDREMOVE;
import static org.talend.components.marketo.component.ListGeneratorSource.LIST_ID;
import static org.talend.components.marketo.dataset.MarketoInputConfiguration.ListAction.isMemberOf;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.marketo.MarketoBaseTest;
import org.talend.components.marketo.component.DataCollector;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration.ListAction;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true, responseLocator = org.talend.sdk.component.junit.http.internal.impl.MarketoResponseLocator.class)
@WithComponents("org.talend.components.marketo")
public class ListProcessorTest extends MarketoBaseTest {

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();
        outputConfiguration.getDataSet().setEntity(MarketoEntity.List);
        data = service.getRecordBuilder().newRecordBuilder().withInt(ATTR_LIST_ID, LIST_ID)
                .withInt(ATTR_LEAD_ID, LEAD_ID_ADDREMOVE).build();
        outputConfiguration.setListAction(ListAction.addTo);
        initProcessor();
        processor.map(data);

        inputConfiguration.getDataSet().setEntity(MarketoEntity.List);
    }

    private void initProcessor() {
        processor = new MarketoProcessor(outputConfiguration, service);
        processor.init();
    }

    @Test
    void testAddToList() {
        outputConfiguration.setListAction(ListAction.removeFrom);
        initProcessor();
        processor.map(data);
        //
        outputConfiguration.setListAction(ListAction.addTo);
        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        final String inputConfig = "config.isInvalid=false";
        runOutputPipeline("ListGenerator", inputConfig, config);
        //
        inputConfiguration.setListAction(isMemberOf);
        inputConfiguration.setListName(String.valueOf(LIST_ID));
        inputConfiguration.setLeadIds(String.valueOf(LEAD_ID_ADDREMOVE));
        runInputPipeline(configurationByExample().forInstance(inputConfiguration).configured().toQueryString());
        assertEquals(1, DataCollector.getData().size());
        assertEquals("memberof", DataCollector.getData().poll().getString("status"));
    }

    @Test
    void testRemoveFromList() {
        outputConfiguration.setListAction(ListAction.addTo);
        initProcessor();
        processor.map(data);
        //
        outputConfiguration.setListAction(ListAction.removeFrom);
        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        final String inputConfig = "config.isInvalid=false";
        runOutputPipeline("ListGenerator", inputConfig, config);
        inputConfiguration.setListAction(isMemberOf);
        inputConfiguration.setListName(String.valueOf(LIST_ID));
        inputConfiguration.setLeadIds(String.valueOf(LEAD_ID_ADDREMOVE));
        runInputPipeline(configurationByExample().forInstance(inputConfiguration).configured().toQueryString());
        assertEquals(1, DataCollector.getData().size());
        assertEquals("notmemberof", DataCollector.getData().poll().getString("status"));
    }

}
