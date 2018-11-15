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
package org.talend.components.marketo.input;

import org.junit.jupiter.api.BeforeEach;
import org.talend.components.marketo.MarketoBaseTest;
import org.talend.components.marketo.dataset.MarketoInputDataSet;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@WithComponents("org.talend.components.marketo")
public class SourceBaseTest extends MarketoBaseTest {

    MarketoInputDataSet dataset;

    protected Record result;

    protected static final String MARKETO_TEST_DATA_COLLECTOR = "MarketoTest://DataCollector";

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();
        dataset = new MarketoInputDataSet();
        dataset.setDataStore(dataStore);
    }

    protected void runInputPipeline(String config) {
        Job.components() //
                .component("MktoInput", "Marketo://Input?" + config) //
                .component("collector", MARKETO_TEST_DATA_COLLECTOR) //
                .connections() //
                .from("MktoInput") //
                .to("collector") //
                .build().run();
    }

}