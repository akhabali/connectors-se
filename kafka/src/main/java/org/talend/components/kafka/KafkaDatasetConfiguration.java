package org.talend.components.kafka;

import java.io.Serializable;

import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.constraint.Max;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Version(1)
@Data
@DataSet("KafkaDataset")
@GridLayout({ @GridLayout.Row("connection"), //
        @GridLayout.Row("topic"), //
        @GridLayout.Row("valueFormat"), //
        @GridLayout.Row("fieldDelimiter"), //
        @GridLayout.Row("specificFieldDelimiter"), //
        @GridLayout.Row("isHierarchy"), //
        @GridLayout.Row("avroSchema") })
@Documentation("Kafka records contained in a topic.")
public class KafkaDatasetConfiguration implements Serializable {

    @Option
    @Required
    @Documentation("Kafka connection configuration.")
    private KafkaConnectionConfiguration connection;

    @Option
    @Required
    @Documentation("The Kafka topic to read/write records from.")
    @Max(249) // See https://github.com/apache/kafka/blob/0.10.1/core/src/main/scala/kafka/common/Topic.scala#L29
    @Pattern("^[a-zA-Z0-9\\._\\-]+$")
    private String topic;

    @Option
    @Required
    @Documentation("The format of the records stored in Kafka messages.")
    private ValueFormat valueFormat = ValueFormat.CSV;

    @Option
    @ActiveIf(target = "valueFormat", value = "CSV")
    @Documentation("TODO fill the documentation for this parameter")
    private FieldDelimiterType fieldDelimiter = FieldDelimiterType.SEMICOLON;

    @Option
    @ActiveIfs({ //
            @ActiveIf(target = "valueFormat", value = "CSV"), //
            @ActiveIf(target = "fieldDelimiter", value = "OTHER") })
    @Documentation("TODO fill the documentation for this parameter")
    private String specificFieldDelimiter = ";";

    @Option
    @ActiveIf(target = "valueFormat", value = "AVRO")
    @Documentation("TODO fill the documentation for this parameter")
    // TODO: Rename to useAvroSchema.
    private boolean isHierarchy = true;

    @Option
    @TextArea
    @ActiveIfs({ //
            @ActiveIf(target = "valueFormat", value = "AVRO"), //
            @ActiveIf(target = "isHierarchy", value = "true") })
    @Code("json")
    @Documentation("TODO fill the documentation for this parameter")
    private String avroSchema;

    public String getFieldDelimiter() {
        if (FieldDelimiterType.OTHER.equals(fieldDelimiter)) {
            return specificFieldDelimiter;
        } else {
            return fieldDelimiter.getDelimiter();
        }
    }

    public enum ValueFormat {
        CSV,
        AVRO
    }

    public enum FieldDelimiterType {
        SEMICOLON(";"),
        COMMA(","),
        TAB("\t"),
        SPACE(" "),
        OTHER("Other");

        private final String value;

        FieldDelimiterType(final String value) {
            this.value = value;
        }

        public String getDelimiter() {
            return value;
        }
    }
}