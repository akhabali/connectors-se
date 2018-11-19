package org.talend.components.marketo.component;

import static org.talend.components.marketo.MarketoApiConstants.ATTR_EXTERNAL_OPPORTUNITY_ID;

import javax.annotation.PostConstruct;

import org.talend.sdk.component.api.record.Record.Builder;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class OpportunityGeneratorSource extends GeneratorSource {

    public OpportunityGeneratorSource(GeneratorConfig config, RecordBuilderFactory recordBuilderFactory) {
        super(config, recordBuilderFactory);
    }

    @PostConstruct
    public void init() {
        final Builder builder = recordBuilderFactory.newRecordBuilder();
        if (config.isInvalid()) {
            builder.withString(ATTR_EXTERNAL_OPPORTUNITY_ID, "XxXOppportunityXxX");
        } else {
            builder.withString(ATTR_EXTERNAL_OPPORTUNITY_ID, "opportunity102");
        }
        data.add(builder.build());
    }

}
