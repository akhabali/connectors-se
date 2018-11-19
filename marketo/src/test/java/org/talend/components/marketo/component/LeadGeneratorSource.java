package org.talend.components.marketo.component;

import static org.talend.components.marketo.MarketoApiConstants.ATTR_EMAIL;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.record.Record.Builder;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class LeadGeneratorSource extends GeneratorSource {

    private static final Logger LOG = LoggerFactory.getLogger(LeadGeneratorSource.class);

    public LeadGeneratorSource(final GeneratorConfig config, final RecordBuilderFactory recordBuilderFactory) {
        super(config, recordBuilderFactory);
    }

    @PostConstruct
    public void init() {
        final Builder builder = recordBuilderFactory.newRecordBuilder();
        if (config.isInvalid()) {
            if ("sync".equals(config.getAction())) {
                builder.withString(ATTR_EMAIL, "0000@0000.com");
            } else {
                builder.withInt(ATTR_ID, 0);
            }
        } else {
            builder.withString(config.getKeyName(), config.getKeyValue());
        }
        data.add(builder.build());
    }
}
