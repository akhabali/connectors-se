package org.talend.components.marketo.component;

import javax.annotation.PostConstruct;

import org.talend.sdk.component.api.record.Record.Builder;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class CustomObjectGeneratorSource extends GeneratorSource {

    public CustomObjectGeneratorSource(GeneratorConfig config, RecordBuilderFactory recordBuilderFactory) {
        super(config, recordBuilderFactory);
    }

    @PostConstruct
    public void init() {
        final Builder builder = recordBuilderFactory.newRecordBuilder();
        if (config.isInvalid()) {
            builder.withInt("customerId", 0).withString("VIN", "ABC-DEF-12345-GIN");
        } else {
            builder.withInt("customerid", 3).withString("VIN", "ABC-DEF-12345-GIN");
        }
        data.add(builder.build());
    }

}
