package org.talend.components.elasticsearch;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@DataSet("ElasticsearchDataSet")
@Documentation("Elasticsearch dataset.")
@OptionsOrder({ "datastore", "index", "type" })
public class ElasticsearchDataSet implements Serializable {

    @Option
    @Documentation("The connection.")
    private ElasticsearchDataStore datastore;

    @Option
    @Required
    @Documentation("The index to query.")
    private String index;

    @Option
    @Required
    @Documentation("The type to query.")
    private String type;
}
