package org.talend.components.kafka;

import java.io.Serializable;

import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Version(1)
@Data
@DataStore("KafkaConnection")
@GridLayout({ @GridLayout.Row("brokers"), //
        @GridLayout.Row("useSsl"), //
        @GridLayout.Row("trustStoreType"), //
        @GridLayout.Row("trustStorePath"), //
        @GridLayout.Row("trustStorePassword"), //
        @GridLayout.Row("needClientAuth"), //
        @GridLayout.Row("keyStoreType"), //
        @GridLayout.Row("keyStorePath"), //
        @GridLayout.Row("keyStorePassword"), //
        @GridLayout.Row("verifyHost") })
@Checkable("healthCheck")
@Documentation("Information necessary to communicate with Kafka.")
public class KafkaConnectionConfiguration implements Serializable {

    @Option
    @Required
    @Documentation("Comma-delimited list of Kafka broker hosts with ports.")
    @Pattern("^(\\w+)(\\.\\w+)*:\\d+(,(\\w+)(\\.\\w+)*:\\d+)*$")
    private String brokers;

    @Option
    @Required
    @Documentation("Whether the connection to Kafka is protected by SSL.")
    private boolean useSsl = false;

    @Option
    @ActiveIf(target = "useSsl", value = "true")
    @Documentation("TODO fill the documentation for this parameter")
    private StoreType trustStoreType;

    @Option
    @ActiveIf(target = "useSsl", value = "true")
    @Documentation("TODO fill the documentation for this parameter")
    private String trustStorePath;

    @Option
    @Credential
    @ActiveIf(target = "useSsl", value = "true")
    @Documentation("TODO fill the documentation for this parameter")
    private String trustStorePassword;

    @Option
    @ActiveIf(target = "useSsl", value = "true")
    @Documentation("TODO fill the documentation for this parameter")
    private boolean needClientAuth = false;

    @Option
    @DefaultValue("JKS")
    @ActiveIfs({ //
            @ActiveIf(target = "useSsl", value = "true"), //
            @ActiveIf(target = "needClientAuth", value = "true") })
    @Documentation("TODO fill the documentation for this parameter")
    private StoreType keyStoreType;

    @Option
    @ActiveIfs({ //
            @ActiveIf(target = "useSsl", value = "true"), //
            @ActiveIf(target = "needClientAuth", value = "true") })
    @Documentation("TODO fill the documentation for this parameter")
    private String keyStorePath;

    @Option
    @Credential
    @ActiveIfs({ //
            @ActiveIf(target = "useSsl", value = "true"), //
            @ActiveIf(target = "needClientAuth", value = "true") })
    @Documentation("TODO fill the documentation for this parameter")
    private String keyStorePassword;

    // For debugging, this is usually set to false.
    @Option
    @ActiveIf(target = "useSsl", value = "true")
    @Documentation("Verify the client hostname with the hostname in the certificate.")
    private boolean verifyHost = true;

    public enum StoreType {
        JKS,
        PKCS12
    }
}
