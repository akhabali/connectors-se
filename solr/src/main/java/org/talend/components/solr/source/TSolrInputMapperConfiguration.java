package org.talend.components.solr.source;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.talend.components.solr.common.Row;
import org.talend.components.solr.common.SolrConnectionConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;
import lombok.Data;

@Data
@DataSet("dataset")
@GridLayout({ @GridLayout.Row({ "solrConnection" }), @GridLayout.Row({ "filterQuery" }), @GridLayout.Row({ "start" }),
        @GridLayout.Row({ "rows" }) })

@GridLayout(value = { @GridLayout.Row({ "schema" }) }, names = { GridLayout.FormType.ADVANCED })
@Documentation("TODO fill the documentation for this configuration")
public class TSolrInputMapperConfiguration implements Serializable {

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private SolrConnectionConfiguration solrConnection;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String start = "0";

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String rows = "10";

    @Option
    @Documentation("The name of the row to be read")
    private List<Row> filterQuery = new ArrayList<>();

    @Option
    @Structure(type = Structure.Type.OUT, discoverSchema = "discoverSchema")
    @Documentation("")
    private List<String> schema;

}