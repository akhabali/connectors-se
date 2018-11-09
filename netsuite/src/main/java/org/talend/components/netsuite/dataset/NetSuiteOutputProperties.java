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
 */
package org.talend.components.netsuite.dataset;

import java.io.Serializable;
import java.util.List;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayouts({ @GridLayout({ @GridLayout.Row({ "dataSet" }), @GridLayout.Row({ "action" }) }), @GridLayout(names = {
        GridLayout.FormType.ADVANCED }, value = { @GridLayout.Row({ "dataSet" }), @GridLayout.Row({ "useNativeUpsert" }) }) })
@Documentation("Properties for Output component")
public class NetSuiteOutputProperties implements Serializable {

    /**
     * Basic operation with NetSuite records.
     *
     */
    public enum DataAction {
        ADD,
        UPDATE,
        UPSERT,
        DELETE
    }

    private List<String> schemaIn;

    private List<String> schemaRejected;

    @Option
    @Documentation("Common dataset properties - datastore + module")
    private NetSuiteDataSet dataSet;

    @Option
    @Documentation("Operation to be performed with records. Default - ADD")
    private DataAction action = DataAction.ADD;

    @Option
    @ActiveIf(target = "action", value = "UPSERT")
    @Documentation("Changes UPSERT strategy. Default - true, uses NetSuite upsert; otherwise - custom")
    private boolean useNativeUpsert = true;
}
