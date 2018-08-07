package org.talend.components.solr.common;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;

@Data
public class FilterCriteria {

    @Option
    @Documentation("Field name of criteria")
    private String field;

    @Option
    @Documentation("Value of criteria")
    private String value;
}
