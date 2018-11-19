package org.talend.components.marketo.component;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;
import javax.annotation.PreDestroy;

import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public abstract class GeneratorSource implements Serializable {

    protected final GeneratorConfig config;

    protected RecordBuilderFactory recordBuilderFactory;

    protected Queue<Record> data = new LinkedList<>();

    public GeneratorSource(final GeneratorConfig config, final RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.config = config;
    }

    @Producer
    public Record next() {
        return data.poll();
    }

    @PreDestroy
    public void close() {
        data = new LinkedList<>();
    }
}
