package org.talend.components.source;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;

@Data
public class Row {

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String field;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String value;
}
