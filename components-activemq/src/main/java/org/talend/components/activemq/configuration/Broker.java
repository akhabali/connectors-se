package org.talend.components.activemq.configuration;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
public class Broker implements Serializable {

    @Option
    @Documentation("JMS broker Host name")
    private String host;

    @Option
    @Documentation("JMS broker port")
    private String port;

}