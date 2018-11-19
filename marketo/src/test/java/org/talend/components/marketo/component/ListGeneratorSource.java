package org.talend.components.marketo.component;

import static org.talend.components.marketo.MarketoApiConstants.ATTR_LEAD_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LIST_ID;

import javax.annotation.PostConstruct;

import org.talend.sdk.component.api.record.Record.Builder;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class ListGeneratorSource extends GeneratorSource {

    public ListGeneratorSource(GeneratorConfig config, RecordBuilderFactory recordBuilderFactory) {
        super(config, recordBuilderFactory);
    }

    public static final int LEAD_ID_ADDREMOVE = 4;

    public static final int LIST_ID = 1001;

    public static final int LEAD_ID_INVALID = -100;

    @PostConstruct
    public void init() {
        final Builder builder = recordBuilderFactory.newRecordBuilder();
        if (config.isInvalid()) {
            builder.withInt(ATTR_LIST_ID, LIST_ID).withInt(ATTR_LEAD_ID, LEAD_ID_INVALID);
        } else {
            builder.withInt(ATTR_LIST_ID, LIST_ID).withInt(ATTR_LEAD_ID, LEAD_ID_ADDREMOVE);
        }
        data.add(builder.build());
    }

}
