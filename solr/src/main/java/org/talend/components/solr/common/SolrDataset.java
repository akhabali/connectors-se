package org.talend.components.solr.common;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import java.util.List;

@Data
@DataSet("Solr")
@GridLayout({ @GridLayout.Row({ "dataStore" }), @GridLayout.Row({ "core" }) })

@GridLayout(value = { @GridLayout.Row({ "schema" }) }, names = { GridLayout.FormType.ADVANCED })

public class SolrDataset {

    @Option
    @Documentation("Solr server URL DataStore")
    private SolrDataStore dataStore;

    @Option
    @Required
    @Documentation("the name of Solr Core")
    @Suggestable(value = "coreList", parameters = { "dataStore/url", "dataStore/login", "dataStore/password" })
    private String core;

    public String getFullUrl() {
        String solr = dataStore.getUrl();
        boolean addSlash = !solr.endsWith("/") && !solr.endsWith("\\");
        return (addSlash ? solr + "/" : solr) + core;
    }

    @Option
    @Structure(type = Structure.Type.OUT, discoverSchema = "discoverSchema")
    @Documentation("Document schema")
    private List<String> schema;
}
