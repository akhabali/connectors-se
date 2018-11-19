package org.talend.components.marketo.component;

import static java.util.Arrays.asList;

import java.io.Serializable;
import java.util.List;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

@Version
@Icon(Icon.IconType.SAMPLE)
@PartitionMapper(name = "LeadGenerator", family = "MarketoTest")
public class LeadGeneratorMapper implements Serializable {

    protected GeneratorConfig config;

    protected RecordBuilderFactory recordBuilderFactory;

    public LeadGeneratorMapper(@Option("config") final GeneratorConfig config, final RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.config = config;
    }

    @Assessor
    public long estimateSize() {
        return 100;
    }

    @Split
    public List<LeadGeneratorMapper> split(@PartitionSize final long bundles) {
        return asList(new LeadGeneratorMapper(config, recordBuilderFactory));
    }

    @Emitter
    public LeadGeneratorSource createWorker() {
        return new LeadGeneratorSource(config, recordBuilderFactory);
    }

}
