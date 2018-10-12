package org.talend.components.localio.fixed;

import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.schema.Schema;

@Service
public class FixedFlowInputService {

    @DiscoverSchema
    public Schema getSchema(FixedDataSetConfiguration dataSet) {
        switch (dataSet.getFormat()) {
        case CSV:
        case JSON:
        case AVRO:
        }
        throw new RuntimeException("");
    }
}
