package org.talend.components.solr.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.schema.SchemaRepresentation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.solr.common.FilterCriteria;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Type;

import javax.json.Json;
import javax.json.JsonObject;

import java.io.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public class SolrConnectorUtilsTest {

    private SolrConnectorUtils util;

    @BeforeEach
    public void init() {
        util = new SolrConnectorUtils();
    }

    @Test
    public void testTrimQuotesNoQuotes() {
        String result = util.trimQuotes("testString");
        assertEquals("testString", result);
    }

    @Test
    public void testTrimQuotesWithQuotes() {
        String result = util.trimQuotes("\"testString\"");
        assertEquals("testString", result);
    }

    @Test
    public void testTrimQuotesPrefix() {
        String result = util.trimQuotes("\"testString");
        assertEquals("\"testString", result);
    }

    @Test
    public void testTrimQuotesSuffix() {
        String result = util.trimQuotes("testString\"");
        assertEquals("testString\"", result);
    }

    @Test
    public void testTrimSingleQuote() {
        String result = util.trimQuotes("\'testString\'");
        assertEquals("testString", result);
    }

    @Test
    public void testCreateQueryFromPositive() {
        JsonObject record = Json.createObjectBuilder().add("field1", "value1").add("field2", "value2").add("field3", "value3")
                .build();
        String result = util.createQueryFromRecord(record);

        assertEquals("field1:value1 AND field2:value2 AND field3:value3", result);
    }

    @Test
    public void testCreateQueryFromRecordWithWhiteSpaces() {
        JsonObject record = Json.createObjectBuilder().add("field1", "value 1").add("field2", "value 2").add("field3", "value 3")
                .build();
        String result = util.createQueryFromRecord(record);

        assertEquals("field1:\"value 1\" AND field2:\"value 2\" AND field3:\"value 3\"", result);
    }

    @Test
    public void testCreateQueryFromRecordWithEmptyValue() {
        JsonObject record = Json.createObjectBuilder().add("field1", "").add("field2", "value2").add("field3", "value3").build();
        String result = util.createQueryFromRecord(record);

        assertEquals("field2:value2 AND field3:value3", result);
    }

    @Test
    public void testCreateQueryFromRecordShortString() {
        JsonObject record = Json.createObjectBuilder().add("field1", "1").add("field2", "2").add("field3", "3").build();
        String result = util.createQueryFromRecord(record);

        assertEquals("field1:1 AND field2:2 AND field3:3", result);
    }

    @Test
    public void testGetSchemaFromRepresentationNullRepresentation() {
        assertEquals(new Schema(Collections.emptyList()), util.getSchemaFromRepresentation(null));
    }

    @Test
    public void testGetSchemaFromRepresentationEmpty() {
        SchemaRepresentation representation = new SchemaRepresentation();
        representation.setFields(Arrays.asList(new HashMap<>()));
        assertEquals(new Schema(Collections.emptyList()), util.getSchemaFromRepresentation(representation));
    }

    @Test
    public void testGetSchemaFromRepresentation() {
        InputStream fis;
        Reader reader = null;
        try {
            fis = new FileInputStream("src/test/resources/test-get-schema.json");
            reader = new InputStreamReader(fis);
        } catch (FileNotFoundException e) {
            log.error(e.getMessage(), e);
            fail();
        }
        Gson gson = new GsonBuilder().create();
        SchemaRepresentation representation = gson.fromJson(reader, SchemaRepresentation.class);

        List<Schema.Entry> entries = new ArrayList<>();
        entries.add(new Schema.Entry("author", Type.STRING));
        entries.add(new Schema.Entry("cat", Type.STRING));
        entries.add(new Schema.Entry("category", Type.STRING));
        entries.add(new Schema.Entry("comments", Type.STRING));
        entries.add(new Schema.Entry("inStock", Type.BOOLEAN));
        entries.add(new Schema.Entry("popularity", Type.INT));
        entries.add(new Schema.Entry("price", Type.DOUBLE));
        Schema expected = new Schema(entries);

        assertEquals(expected, util.getSchemaFromRepresentation(representation));
    }

    @Test
    public void testGetCoresEmpty() {
        Collection cores = util.getCoreListFromResponse(null);
        assertTrue(cores.isEmpty());
    }

    @Test
    public void testAddFilterQuery() {
        FilterCriteria fc = new FilterCriteria();
        fc.setField("field");
        fc.setValue("value");
        SolrQuery query = new SolrQuery("*:*");
        util.addFilterQuery(fc, query);
        assertEquals("q=*:*&fq=field:value", query.toString());
    }

    @Test
    public void testWrapFqValue() {
        String inputString = "TestString";
        assertEquals("TestString", util.wrapFqValue(inputString));
    }

    @Test
    public void testWrapFqValueQuotes() {
        String inputString = "Test String";
        assertEquals("\"Test String\"", util.wrapFqValue(inputString));
    }

    @Test
    public void testParseInt() {
        assertEquals(new Integer(1234567), util.parseInt("1234567"));
    }

    @Test
    public void testParseIntNegative() {
        assertEquals(new Integer(0), util.parseInt("1234f567"));
    }

}
