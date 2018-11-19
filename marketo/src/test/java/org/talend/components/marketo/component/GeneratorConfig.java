package org.talend.components.marketo.component;

import lombok.Data;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;

@Data
public class GeneratorConfig implements Serializable {

    @Option
    private int startIndex;

    @Option
    private int rowCount;

    @Option
    private boolean isInvalid = false;

    @Option
    private String keyName;

    @Option
    private String keyValue;

    @Option
    private String action;
}
