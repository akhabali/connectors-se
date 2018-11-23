
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

package org.talend.components.salesforce.dataset;

import java.util.List;

import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Uniques;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@DataSet("ModuleQuery")
@GridLayout(value = { @GridLayout.Row("dataStore"), @GridLayout.Row("moduleName"), @GridLayout.Row("condition"),
        @GridLayout.Row("selectColumnIds"), @GridLayout.Row({ "addAllColumns", "selectedColumn" }), })
@Documentation("")
public class ModuleQueryDataSet implements QueryDataSet {

    @Option
    @Documentation("")
    private BasicDataStore dataStore;

    @Option
    @Suggestable(value = "loadSalesforceModules", parameters = { "dataStore" })
    @Documentation("")
    private String moduleName;

    @Option
    @Documentation("")
    private String condition;

    @Option
    @Documentation("")
    @Structure(type = Structure.Type.OUT, discoverSchema = "addColumns")
    @Uniques
    private List<String> selectColumnIds;

    @Option
    @Documentation("")
    private boolean addAllColumns = true;

    @Option
    @Documentation("")
    @ActiveIf(target = "addAllColumns", value = { "false" })
    @Suggestable(value = "retrieveColumns", parameters = { "dataStore", "moduleName", "selectColumnIds" })
    private String selectedColumn;

}
