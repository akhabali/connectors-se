package org.talend.components.solr.output;

import lombok.Data;
import org.talend.components.solr.common.SolrConnectionConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@DataSet
@GridLayout({
        // the generated layout put one configuration entry per line,
        // customize it as much as needed
        @GridLayout.Row({ "solrConnection" }), @GridLayout.Row({ "action" }) })
@Documentation("TODO fill the documentation for this configuration")
public class TSolrProcessorOutputConfiguration implements Serializable {

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private SolrConnectionConfiguration solrConnection;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private ActionEnum action = ActionEnum.UPDATE;

}