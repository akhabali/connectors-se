package org.talend.components.marketo.component;

import static java.util.Arrays.asList;

import java.io.Serializable;
import java.util.List;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

@Version
@Icon(Icon.IconType.SAMPLE)
@PartitionMapper(name = "CustomObjectGenerator", family = "MarketoTest")
public class CustomObjectGeneratorMapper implements Serializable {

    protected GeneratorConfig config;

    protected RecordBuilderFactory recordBuilderFactory;

    public CustomObjectGeneratorMapper(GeneratorConfig config, RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.config = config;
    }

    @Assessor
    public long estimateSize() {
        return 100;
    }

    @Split
    public List<CustomObjectGeneratorMapper> split(@PartitionSize final long bundles) {
        return asList(new CustomObjectGeneratorMapper(config, recordBuilderFactory));
    }

    @Emitter
    public CustomObjectGeneratorSource createWorker() {
        return new CustomObjectGeneratorSource(config, recordBuilderFactory);
    }

}
