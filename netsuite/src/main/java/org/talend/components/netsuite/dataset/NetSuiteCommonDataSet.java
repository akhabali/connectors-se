package org.talend.components.netsuite.dataset;

import java.util.List;

import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.configuration.ui.widget.Structure.Type;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@DataSet
@AllArgsConstructor
@NoArgsConstructor
@GridLayout({ @GridLayout.Row({ "dataStore" }), @GridLayout.Row({ "recordType" }), @GridLayout.Row({ "schema" }) })
@Documentation("Common properties that are present in Input & Output components")
public class NetSuiteCommonDataSet {

    @Option
    @Documentation("Datastore")
    private NetsuiteDataStore dataStore;

    @Option
    @Suggestable(value = "loadRecordTypes", parameters = { "dataStore" })
    @Documentation("Record Type to be used")
    private String recordType;

    @Option
    @Structure(discoverSchema = "guessSchema", type = Type.OUT)
    @Documentation("Design Schema")
    private List<String> schema;
}
