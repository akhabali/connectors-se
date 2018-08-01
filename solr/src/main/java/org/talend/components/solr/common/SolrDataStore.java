package org.talend.components.solr.common;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@DataStore("SolrDataStore")
@Checkable("checkSolrConnection")
@GridLayout({ @GridLayout.Row({ "url" }) })
public class SolrDataStore {

    @Option
    @Required
    @Documentation("TODO fill the documentation for this parameter")
    private String url;
}
