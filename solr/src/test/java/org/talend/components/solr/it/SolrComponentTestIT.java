package org.talend.components.solr.it;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrDocument;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.solr.common.FilterCriteria;
import org.talend.components.solr.common.SolrDataset;
import org.talend.components.solr.common.SolrDataStore;
import org.talend.components.solr.output.Action;
import org.talend.components.solr.output.SolrProcessorOutputConfiguration;
import org.talend.components.solr.service.Messages;
import org.talend.components.solr.service.SolrConnectorService;
import org.talend.components.solr.service.SolrConnectorUtils;
import org.talend.components.solr.source.SolrInputMapperConfiguration;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Type;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.solr")
public class SolrComponentTestIT {

    private final static String DOCKER_HOST_ADDRESS = System.getProperty("dockerHostAddress", "localhost");

    private final static String SOLR_PORT = System.getProperty("solr.test.port", "8983");

    private final static String SOLR_URL = "https://" + DOCKER_HOST_ADDRESS + ":" + SOLR_PORT + "/solr/";

    private final static String CORE = "testcore";

    private final static String LOGIN = "solr";

    private final static String PASSWORD = "SolrRocks";

    private final static Messages messages = new Messages() {

        @Override
        public String healthCheckOk() {
            return "OK";
        }

        @Override
        public String healthCheckFailed(String cause) {
            return "FAIL";
        }

        @Override
        public String badCredentials() {
            return "";
        }
    };

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private JsonBuilderFactory factory;

    private SolrInputMapperConfiguration inputMapperConfiguration;

    private SolrProcessorOutputConfiguration solrProcessorOutputConfiguration;

    private SolrDataset solrConnection;

    @BeforeAll
    public static void beforeAll() {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @BeforeEach
    public void init() {
        final SolrDataStore dataStore = new SolrDataStore();
        dataStore.setUrl(SOLR_URL);
        dataStore.setLogin(LOGIN);
        dataStore.setPassword(PASSWORD);
        solrConnection = new SolrDataset();
        solrConnection.setCore(CORE);
        solrConnection.setDataStore(dataStore);
        inputMapperConfiguration = new SolrInputMapperConfiguration();
        inputMapperConfiguration.setDataset(solrConnection);
        solrProcessorOutputConfiguration = new SolrProcessorOutputConfiguration();
        solrProcessorOutputConfiguration.setDataset(solrConnection);
    }

    @Test
    @DisplayName("Solr")
    void inputTest() {
        inputMapperConfiguration.setRows("100");
        final String config = configurationByExample().forInstance(inputMapperConfiguration).configured().toQueryString();
        Job.components().component("SolrInput", "Solr://Input?" + config).component("collector", "test://collector").connections()
                .from("SolrInput").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertTrue(res.stream().map(e -> e.getString("id")).collect(Collectors.toSet()).containsAll(Arrays.asList("apple",
                "corsair", "samsung", "viewsonic", "ati", "belkin", "maxtor", "asus", "adata", "canon", "dell")));
    }

    @Test
    @DisplayName("Solr")
    void inputRawTest() {
        inputMapperConfiguration.setRows("100");
        inputMapperConfiguration.setRawQuery("q=id:adata");
        final String config = configurationByExample().forInstance(inputMapperConfiguration).configured().toQueryString();
        Job.ExecutorBuilder executorBuilder = Job.components().component("SolrInput", "Solr://Input?" + config)
                .component("collector", "test://collector").connections().from("SolrInput").to("collector").build();
        executorBuilder.run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertFalse(res.stream().map(e -> e.getString("id")).collect(Collectors.toSet()).contains("dell"));
        assertTrue(res.stream().map(e -> e.getString("id")).collect(Collectors.toSet()).contains("adata"));
    }

    @Test
    @DisplayName("UpdateTest")
    void outputUpdateTest() throws IOException, SolrServerException {
        solrProcessorOutputConfiguration.setAction(Action.UPSERT);
        final String config = configurationByExample().forInstance(solrProcessorOutputConfiguration).configured().toQueryString();

        componentsHandler.setInputData(asList(factory.createObjectBuilder().add("address_s", "comp1").build(),
                factory.createObjectBuilder().add("address_s", "comp2").build(),
                factory.createObjectBuilder().add("address_s", "comp3").build(),
                factory.createObjectBuilder().add("address_s", "comp4").build()));

        Job.components().component("emitter", "test://emitter").component("SolrOutput", "Solr://Output?" + config).connections()
                .from("emitter").to("SolrOutput").build().run();

        List<SolrDocument> res = getFirs100Documents();
        assertTrue(res.stream().map(e -> e.get("address_s")).collect(Collectors.toSet())
                .containsAll(Arrays.asList("comp1", "comp2", "comp3")));
    }

    @Test
    @DisplayName("Solr")
    void outputDeleteTest() throws IOException, SolrServerException {
        solrProcessorOutputConfiguration.setAction(Action.DELETE);
        final String config = configurationByExample().forInstance(solrProcessorOutputConfiguration).configured().toQueryString();
        componentsHandler.setInputData(asList(factory.createObjectBuilder().add("id", "apple").build(),
                factory.createObjectBuilder().add("id", "corsair").build(),
                factory.createObjectBuilder().add("id", "samsung").build()));

        Job.components().component("emitter", "test://emitter").component("SolrOutput", "Solr://Output?" + config).connections()
                .from("emitter").to("SolrOutput").build().run();

        List<SolrDocument> res = getFirs100Documents();
        assertFalse(res.stream().map(e -> e.get("id")).collect(Collectors.toSet())
                .containsAll(Arrays.asList("apple", "corsair", "samsung")));
        assertTrue(res.stream().map(e -> e.get("id")).collect(Collectors.toSet())
                .containsAll(Arrays.asList("viewsonic", "ati", "belkin")));
    }

    @Test
    @DisplayName("Guess schema")
    void guessTableSchemaTest() {
        SolrConnectorService service = new SolrConnectorService();
        SolrInputMapperConfiguration config = new SolrInputMapperConfiguration();
        SolrConnectorUtils util = new SolrConnectorUtils();
        config.setDataset(solrConnection);
        Schema schema = service.guessTableSchema(config.getDataset(), util);
        Schema expectedSchema = new Schema(Arrays.asList(new Schema.Entry("id", Type.STRING)));
        assertEquals(expectedSchema, schema);
    }

    @Test
    @DisplayName("Guess schema failed test")
    void guessSchemaFailedTest() {
        SolrConnectorService service = new SolrConnectorService();
        SolrInputMapperConfiguration config = new SolrInputMapperConfiguration();
        SolrConnectorUtils util = new SolrConnectorUtils();
        SolrDataset connection = new SolrDataset();
        connection.setCore(CORE);
        SolrDataStore dataStore = new SolrDataStore();
        dataStore.setUrl("https://localhost:8983/badsolrurl");
        dataStore.setLogin(LOGIN);
        dataStore.setPassword(PASSWORD);
        connection.setDataStore(dataStore);
        config.setDataset(connection);
        Schema schema = service.guessTableSchema(config.getDataset(), util);
        assertEquals(new Schema(Collections.emptyList()), schema);
    }

    @Test
    @DisplayName("Check Connection")
    void checkConnectionTest() {
        SolrConnectorService service = new SolrConnectorService();
        SolrConnectorUtils util = new SolrConnectorUtils();
        SolrDataStore dataStore = new SolrDataStore();
        dataStore.setUrl(SOLR_URL);
        dataStore.setLogin(LOGIN);
        dataStore.setPassword(PASSWORD);
        HealthCheckStatus status = service.checkConnection(dataStore, messages, util);
        assertEquals("OK", status.getStatus().name());
    }

    @Test
    @DisplayName("Check Failed Connection")
    void checkConnectionNegativeTest() {
        SolrConnectorService service = new SolrConnectorService();
        SolrConnectorUtils util = new SolrConnectorUtils();
        SolrDataStore dataStore = new SolrDataStore();
        dataStore.setUrl("http://localhost:8982/badsolrurl");
        dataStore.setLogin(LOGIN);
        dataStore.setPassword(PASSWORD);
        HealthCheckStatus status = service.checkConnection(dataStore, messages, util);
        assertEquals("KO", status.getStatus().name());
    }

    @Test
    @DisplayName("Check suggestCore")
    void suggestTest() {
        SolrConnectorService service = new SolrConnectorService();
        SolrInputMapperConfiguration config = new SolrInputMapperConfiguration();
        SolrConnectorUtils util = new SolrConnectorUtils();
        config.setDataset(solrConnection);
        SolrDataStore dataStore = new SolrDataStore();
        dataStore.setUrl(SOLR_URL);
        dataStore.setPassword(PASSWORD);
        dataStore.setLogin(LOGIN);
        SuggestionValues values = service.suggestCore(dataStore, util);
        assertEquals(Arrays.asList(new SuggestionValues.Item(CORE, CORE)), values.getItems());
    }

    @Test
    @DisplayName("Check suggestCore")
    void testSuggestRawQuery() {
        SolrConnectorService service = new SolrConnectorService();
        SolrInputMapperConfiguration config = new SolrInputMapperConfiguration();
        config.setDataset(solrConnection);
        FilterCriteria criteriaId = new FilterCriteria();
        criteriaId.setField("id");
        criteriaId.setValue("apple");
        FilterCriteria criteriaComp = new FilterCriteria();
        criteriaComp.setField("compName_s");
        criteriaComp.setValue("Apple");
        config.setFilterQuery(Arrays.asList(criteriaId, criteriaComp));
        config.setRows("1");
        config.setStart("1");
        SuggestionValues values = service.suggestRawQuery(config, new SolrConnectorUtils());
        SuggestionValues expected = new SuggestionValues();
        SuggestionValues.Item item = new SuggestionValues.Item();
        item.setId("q=*:*&fq=id:apple&fq=compName_s:Apple&rows=1&start=1");
        item.setLabel("q=*:*&fq=id:apple&fq=compName_s:Apple&rows=1&start=1");
        expected.setItems(Arrays.asList(item));
        assertEquals(expected, values);
    }

    private List<SolrDocument> getFirs100Documents() throws IOException, SolrServerException {
        HttpSolrClient solrClient = new HttpSolrClient.Builder(SOLR_URL + CORE).build();
        SolrQuery query = new SolrQuery("*:*");
        query.setRows(100);
        QueryRequest req = new QueryRequest(query);
        req.setBasicAuthCredentials(LOGIN, PASSWORD);
        return req.process(solrClient).getResults();
    }

}
