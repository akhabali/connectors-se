package org.talend.components.marketo.component;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Objects;
import java.util.stream.LongStream;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

@Version
@Icon(Icon.IconType.SAMPLE)
public class GeneratorMapper {

    protected GeneratorConfig config;

    protected RecordBuilderFactory recordBuilderFactory;

    public GeneratorMapper(@Option("config") final GeneratorConfig config, final RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.config = config;
    }

    @Assessor
    public long estimateSize() {
        return "{id:1000, name:\"somename\"}".getBytes().length * config.getRowCount();
    }

    @Split
    public List<GeneratorMapper> split(@PartitionSize final long bundles) {
        long recordSize = "{id:1000, name:\"somename\"}".getBytes().length;
        long nbBundle = Math.max(1, estimateSize() / bundles);
        final long bundleCount = bundles / recordSize;
        final int totalData = config.getRowCount();
        return LongStream.range(0, nbBundle).mapToObj(i -> {
            final int from = (int) (bundleCount * i);
            final int to = (i == nbBundle - 1) ? totalData : (int) (from + bundleCount);
            if (to == 0) {
                return null;
            }
            final GeneratorConfig dataSetChunk = new GeneratorConfig();
            dataSetChunk.setStartIndex(from);
            dataSetChunk.setRowCount(to);
            return new GeneratorMapper(dataSetChunk, recordBuilderFactory);
        }).filter(Objects::nonNull).collect(toList());
    }

}
