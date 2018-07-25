package org.talend.components.output;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@GridLayout({
        // the generated layout put one configuration entry per line,
        // customize it as much as needed
        @GridLayout.Row({ "solrUrl" }), @GridLayout.Row({ "core" }), @GridLayout.Row({ "action" }) })
@Documentation("TODO fill the documentation for this configuration")
public class TSolrDeleteDocumentOutputConfiguration implements Serializable {

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String solrUrl;

    @Option
    @Required
    @Documentation("TODO fill the documentation for this parameter")
    @Suggestable(value = "coreList", parameters = { "solrUrl" })
    private String core;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private ActionEnum action = ActionEnum.UPDATE;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String id;

}