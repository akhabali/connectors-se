package org.talend.components.marketo.component;

import javax.annotation.PostConstruct;

import org.talend.sdk.component.api.record.Record.Builder;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class CompanyGeneratorSource extends GeneratorSource {

    public CompanyGeneratorSource(GeneratorConfig config, RecordBuilderFactory recordBuilderFactory) {
        super(config, recordBuilderFactory);
    }

    @PostConstruct
    public void init() {
        final Builder builder = recordBuilderFactory.newRecordBuilder();
        if (config.isInvalid()) {
            builder.withString("externalCompanyId", "UnbelievableGoogleXYZ");
        } else {
            builder.withString("externalCompanyId", "google666");
        }
        data.add(builder.build());
    }

}
