package org.talend.components.salesforce;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class BulkResultSet {

    private final com.csvreader.CsvReader reader;

    private final List<String> header;

    private RecordBuilderFactory recordBuilderFactory;

    public BulkResultSet(com.csvreader.CsvReader reader, List<String> header,
            final RecordBuilderFactory recordBuilderFactory) {
        this.reader = reader;
        this.header = header;
        this.recordBuilderFactory = recordBuilderFactory;
    }

    public Record next() {
        try {
            boolean hasNext = reader.readRecord();
            String[] row;
            if (hasNext) {
                if ((row = reader.getValues()) != null) {
                    Map<String, String> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                    for (int i = 0; i < this.header.size(); i++) {
                        // We replace the . with _ to add support of relationShip Queries
                        // The relationShip Queries Use . in Salesforce and we use _ in Talend (Studio)
                        // So Account.Name in SF will be Account_Name in Talend
                        result.put(header.get(i).replace('.', '_'), row[i]);

                    }
                    Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder();
                    result
                            .entrySet()
                            .stream()
                            .filter(it -> it.getValue() != null)
                            .forEach(e -> recordBuilder.withString(e.getKey(), e.getValue()));
                    return recordBuilder.build();
                } else {
                    return next();
                }
            } else {
                this.reader.close();
            }
            return null;
        } catch (IOException e) {
            this.reader.close();
            throw new IllegalStateException(e);
        }
    }

}