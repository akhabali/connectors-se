package org.talend.components.source;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;
import lombok.Data;

@Data
@DataSet("dataset")
@GridLayout({
        @GridLayout.Row({"solrUrl"}),
        @GridLayout.Row({"core"}),
        @GridLayout.Row({"filterQuery" }),
        @GridLayout.Row({"start"}),
        @GridLayout.Row({"rows"}),
        @GridLayout.Row({"sort"}),
        @GridLayout.Row({"sortOrder"})
})
@Documentation("TODO fill the documentation for this configuration")
public class TSolrInputMapperConfiguration implements Serializable {

    @Option
    @Required
    @Documentation("TODO fill the documentation for this parameter")
    private String solrUrl;

    @Option
    @Required
    @Documentation("TODO fill the documentation for this parameter")
    @Suggestable(value = "coreList", parameters = {"solrUrl"})
    private String core;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String sort;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private OrderEnum sortOrder = OrderEnum.ASC;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String start = "0";

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String rows = "10";

    @Option
    @Documentation("The name of the row to be read")
    private List<Row> filterQuery = new ArrayList<>();

    enum OrderEnum {
        DESC,
        ASC
    }
}