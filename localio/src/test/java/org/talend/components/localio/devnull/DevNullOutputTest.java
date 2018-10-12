package org.talend.components.localio.devnull;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;
import org.talend.daikon.avro.GenericDataRecordHelper;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.ComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

import javax.json.JsonBuilderFactory;

import static java.util.Collections.emptyMap;
import static org.talend.sdk.component.runtime.manager.ComponentManager.ComponentType.PROCESSOR;

@WithComponents("org.talend.components.localio")
class DevNullOutputTest {

    @Injected
    private ComponentsHandler handler;

    @Test
    void ensureItRuns() {
        final Pipeline pipeline = Pipeline.create();
        final PTransform<PCollection<IndexedRecord>, PCollection<IndexedRecord>> input = handler.asManager()
                .createComponent("LocalIO", "DevNullOutputRuntime", PROCESSOR, 1, emptyMap())
                .map(e -> (PTransform<PCollection<IndexedRecord>, PCollection<IndexedRecord>>) e)
                .orElseThrow(() -> new IllegalArgumentException("No component for fixed flow input"));
        pipeline.apply(Create.of(GenericDataRecordHelper.createRecord(new Object[] { "a", 1 }))).apply(input);
        pipeline.run().waitUntilFinish();
    }
}
