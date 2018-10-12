package org.talend.components.localio.devnull;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.talend.components.localio.fixed.FixedDataSetConfiguration;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Processor;

import javax.json.JsonObject;

import static org.talend.sdk.component.api.component.Icon.IconType.FLOW_TARGET_O;

@Version
@Icon(FLOW_TARGET_O)
@Processor(name = "DevNullOutputRuntime")
@Documentation("This component ignores any input.")
public class DevNullOutputRuntime extends PTransform<PCollection<IndexedRecord>, PCollection<IndexedRecord>> {

    public DevNullOutputRuntime(@Option("configuration") final FixedDataSetConfiguration configuration) {
        // no-op
    }

    @ElementListener
    public void onElement(final JsonObject ignored) {
        // no-op
    }

    @Override
    public PCollection<IndexedRecord> expand(PCollection<IndexedRecord> input) {
        return input;
    }
}
