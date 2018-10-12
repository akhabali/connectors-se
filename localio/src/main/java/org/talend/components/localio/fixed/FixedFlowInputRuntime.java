package org.talend.components.localio.fixed;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.talend.components.adapter.beam.io.rowgenerator.RowGeneratorIO;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.meta.Documentation;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.talend.sdk.component.api.component.Icon.IconType.FLOW_SOURCE_O;

@Version
@Icon(FLOW_SOURCE_O)
@PartitionMapper(name = "FixedFlowInputRuntime")
@Documentation("This component duplicates an input a configured number of times.")
public class FixedFlowInputRuntime extends PTransform<PBegin, PCollection<IndexedRecord>> {

    private final FixedFlowInputConfiguration configuration;

    public FixedFlowInputRuntime(@Option("configuration") final FixedFlowInputConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public PCollection<IndexedRecord> expand(final PBegin begin) {
        FixedDataSetRuntime runtime = new FixedDataSetRuntime(configuration.getDataset());
        // The values to include in the PCollection
        List<IndexedRecord> values = new LinkedList<>();

        if (configuration.getOverrideValuesAction() == FixedFlowInputConfiguration.OverrideValuesAction.NONE
                || configuration.getOverrideValuesAction() == FixedFlowInputConfiguration.OverrideValuesAction.APPEND) {
            if (!configuration.getDataset().getValues().trim().isEmpty()) {
                values.addAll(runtime.getValues(Integer.MAX_VALUE));
            }
        }

        if (configuration.getOverrideValuesAction() == FixedFlowInputConfiguration.OverrideValuesAction.APPEND
                || configuration.getOverrideValuesAction() == FixedFlowInputConfiguration.OverrideValuesAction.REPLACE) {
            configuration.getDataset().setValues(configuration.getOverrideValues());
            if (!configuration.getDataset().getValues().trim().isEmpty()) {
                values.addAll(runtime.getValues(Integer.MAX_VALUE));
            }
        }

        if (values.size() != 0) {
            final PCollectionView<List<IndexedRecord>> data = ((PCollection<IndexedRecord>) begin
                    .apply(Create.of(values).withCoder((AvroCoder) AvroCoder.of(runtime.getSchema())))).apply(View.asList());
            return begin.apply(GenerateSequence.from(0).to(configuration.getRepeat()))
                    .apply(ParDo.of(new DoFn<Long, IndexedRecord>() {

                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            List<IndexedRecord> indexedRecords = c.sideInput(data);
                            indexedRecords.forEach(r -> c.output(r));
                        }
                    }).withSideInputs(data));
        } else {
            return begin.apply(RowGeneratorIO.read().withSchema(runtime.getSchema()) //
                    .withSeed(0L) //
                    .withPartitions(1) //
                    .withRows(configuration.getRepeat()));
        }
    }
}
