package org.talend.components.kafka;

import java.io.Serializable;

import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Version(1)
@Data
@GridLayout({ @GridLayout.Row("dataset") })
@Documentation("TODO fill the documentation for this configuration")
public class KafkaOutputConfiguration implements Serializable {

    @Option
    @Required
    @Documentation("TODO fill the documentation for this parameter")
    private KafkaDatasetConfiguration dataset;

}