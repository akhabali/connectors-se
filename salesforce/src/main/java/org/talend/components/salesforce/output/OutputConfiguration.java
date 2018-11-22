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

import java.io.Serializable;
import java.util.List;

import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@DataSet("salesforceOutput")
@GridLayout({
        // the generated layout put one configuration entry per line,
        // customize it as much as needed
        @GridLayout.Row({ "dataStore" }), @GridLayout.Row({ "moduleName" }), @GridLayout.Row({ "schema" }),
        @GridLayout.Row({ "outputAction" }), @GridLayout.Row({ "batchMode" }), @GridLayout.Row("commitLevel"),
        @GridLayout.Row("exceptionForErrors") })
@Documentation("TODO fill the documentation for this configuration")
public class OutputConfiguration implements Serializable {

    @Option
    @Documentation("")
    private BasicDataStore dataStore;

    @Option
    @Suggestable(value = "loadSalesforceModules", parameters = { "dataStore" })
    @Documentation("module name")
    private String moduleName;

    @Option
    @Structure(type = Structure.Type.IN, discoverSchema = "addColumns")
    @Documentation("schem of the module")
    private List<String> schema;

    @Option
    @Documentation("write operation")
    private OutputAction outputAction = OutputAction.INSERT;

    @Option
    @Documentation("key column for upsert")
    private String upsertKeyColumn;

    @Option
    @Documentation("whether use batch operation")
    private boolean batchMode;

    @Option
    @ActiveIf(target = "batchMode", value = "true")
    @DefaultValue("200")
    @Documentation("size of every batch")
    private int commitLevel;

    @Option
    @Documentation("exception for errors")
    private boolean exceptionForErrors;

    public enum OutputAction {
        INSERT,
        UPDATE,
        UPSERT,
        DELETE
    }

}