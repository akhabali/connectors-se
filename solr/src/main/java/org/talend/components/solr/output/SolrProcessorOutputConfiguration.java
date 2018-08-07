package org.talend.components.solr.output;

import lombok.Data;
import org.talend.components.solr.common.SolrConnectionConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

@Data
@DataSet("Solr Output")
@GridLayout({ @GridLayout.Row({ "schema" }), @GridLayout.Row({ "solrConnection" }), @GridLayout.Row({ "action" }) })
@GridLayout(value = { @GridLayout.Row({ "schema" }) }, names = { GridLayout.FormType.ADVANCED })
@Documentation("TODO fill the documentation for this configuration")
public class SolrProcessorOutputConfiguration implements Serializable {

    @Option
    @Documentation("Solr URL. Including core")
    private SolrConnectionConfiguration solrConnection;

    @Option
    @Documentation("Combobox field. Update and Delete values are available")
    private ActionEnum action = ActionEnum.UPDATE;

    @Option
    @Structure(type = Structure.Type.IN, discoverSchema = "discoverOutPutSchema")
    @Documentation("Document Schema")
    private List<String> schema;

}