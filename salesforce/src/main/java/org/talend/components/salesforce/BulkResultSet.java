package org.talend.components.salesforce;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class BulkResultSet {

    private final com.csvreader.CsvReader reader;

    private final List<String> header;

    public BulkResultSet(com.csvreader.CsvReader reader, List<String> header) {
        this.reader = reader;
        this.header = header;
    }

    public Map<String, String> next() {
        try {
            boolean hasNext = reader.readRecord();
            Map<String, String> result = null;
            String[] row;
            if (hasNext) {
                if ((row = reader.getValues()) != null) {
                    result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                    for (int i = 0; i < this.header.size(); i++) {
                        // We replace the . with _ to add support of relationShip Queries
                        // The relationShip Queries Use . in Salesforce and we use _ in Talend (Studio)
                        // So Account.Name in SF will be Account_Name in Talend
                        result.put(header.get(i).replace('.', '_'), row[i]);

                    }
                    return result;
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