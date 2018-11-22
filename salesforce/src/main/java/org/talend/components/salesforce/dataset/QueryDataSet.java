
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

import static org.talend.components.salesforce.dataset.QueryDataSet.SourceType.MODULE_SELECTION;

import java.io.Serializable;
import java.util.List;

import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.constraint.Uniques;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@DataSet("query")
@GridLayout(value = { @GridLayout.Row("dataStore"), @GridLayout.Row("sourceType"), @GridLayout.Row("query"),
        @GridLayout.Row("moduleName"), @GridLayout.Row("condition"), @GridLayout.Row("selectColumnIds"),
        @GridLayout.Row({ "addAllColumns", "selectedColumn" }), })
@Documentation("")
public class QueryDataSet implements Serializable {

    @Option
    @Documentation("")
    private BasicDataStore dataStore;

    @Option
    @Required
    @Documentation("")
    private SourceType sourceType = MODULE_SELECTION;

    @Option
    @ActiveIf(target = "sourceType", value = { "MODULE_SELECTION" })
    @Suggestable(value = "loadSalesforceModules", parameters = { "dataStore" })
    @Documentation("")
    private String moduleName;

    @Option
    @ActiveIf(target = "sourceType", value = { "MODULE_SELECTION" })
    @Documentation("")
    private String condition;

    @Option
    @ActiveIf(target = "sourceType", value = { "SOQL_QUERY" })
    @Code("sql")
    @Documentation("")
    private String query;

    @Option
    @Documentation("")
    @ActiveIf(target = "sourceType", value = { "MODULE_SELECTION" })
    @Structure(type = Structure.Type.OUT, discoverSchema = "addColumns")
    @Uniques
    private List<String> selectColumnIds;

    @Option
    @ActiveIf(target = "sourceType", value = { "MODULE_SELECTION" })
    @Documentation("")
    private boolean addAllColumns = true;

    @Option
    @Documentation("")
    @ActiveIfs({ @ActiveIf(target = "sourceType", value = { "MODULE_SELECTION" }),
            @ActiveIf(target = "addAllColumns", value = { "false" }) })
    @Suggestable(value = "retrieveColumns", parameters = { "dataStore", "moduleName", "selectColumnIds" })
    private String selectedColumn;

    public enum SourceType {
        MODULE_SELECTION,
        SOQL_QUERY
    }

}
