package org.talend.components.solr.output;

import lombok.Data;
import org.talend.components.solr.common.SolrDataset;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@GridLayout({ @GridLayout.Row({ "dataset" }), @GridLayout.Row({ "action" }) })

@Documentation("Solr Processor output")
public class SolrProcessorOutputConfiguration implements Serializable {

    @Option
    @Documentation("Solr URL. Including core")
    private SolrDataset dataset;

    @Option
    @Documentation("Combobox field. Update and Delete values are available")
    private Action action = Action.UPSERT;

}