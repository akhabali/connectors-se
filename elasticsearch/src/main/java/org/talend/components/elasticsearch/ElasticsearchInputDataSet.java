package org.talend.components.elasticsearch;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
@DataSet("ElasticsearchInputDataSet")
@Documentation("Elasticsearch input dataset.")
@OptionsOrder({ "datastore", "index", "type", "query" })
public class ElasticsearchInputDataSet extends ElasticsearchDataSet {

    @Option
    @TextArea
    @Documentation("The query.")
    private String query;
}
