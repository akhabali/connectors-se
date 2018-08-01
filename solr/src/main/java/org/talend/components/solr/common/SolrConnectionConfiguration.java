package org.talend.components.solr.common;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@DataStore("SolrConnectionConfiguration")
@GridLayout({ @GridLayout.Row({ "solrUrl" }), @GridLayout.Row({ "core" }) })
public class SolrConnectionConfiguration {

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private SolrDataStore solrUrl;

    @Option
    @Required
    @Documentation("TODO fill the documentation for this parameter")
    @Suggestable(value = "coreList", parameters = { "solrUrl/url" })
    private String core;

    public String getFullUrl() {
        String solr = solrUrl.getUrl();
        boolean addSlash = !solr.endsWith("/") && !solr.endsWith("\\");
        return (addSlash ? solr + "/" : solr) + core;
    }
}
