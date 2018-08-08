package org.talend.components.solr.source;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.talend.components.solr.common.SolrConnectionConfiguration;
import org.talend.components.solr.common.SolrDataStore;
import org.talend.components.solr.service.SolrConnectorService;
import org.talend.components.solr.service.SolrConnectorUtils;

import javax.json.JsonBuilderFactory;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SolrInputSourceTest {

    @Mock
    private SolrInputMapperConfiguration conf;

    @Mock
    private SolrConnectorService service;

    @Mock
    private JsonBuilderFactory jsonBuilderFactory;

    @Mock
    private SolrConnectorUtils utils;

    @Mock
    private HttpSolrClient.Builder clientBuilder;

    @Mock
    private SolrClient solr;

    @Mock
    private QueryResponse response;

    private SolrInputSource source;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @BeforeEach
    void setUp() {
        conf = mock(SolrInputMapperConfiguration.class);
        service = mock(SolrConnectorService.class);
        jsonBuilderFactory = mock(JsonBuilderFactory.class);
        utils = mock(SolrConnectorUtils.class);
        clientBuilder = mock(HttpSolrClient.Builder.class);
        solr = mock(SolrClient.class);
        response = mock(QueryResponse.class);
        source = new SolrInputSource(conf, service, jsonBuilderFactory, utils);
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void testInit() throws IOException, SolrServerException {
        SolrConnectionConfiguration connection = new SolrConnectionConfiguration();
        connection.setCore("core");
        SolrDataStore dataStore = new SolrDataStore();
        dataStore.setUrl("http://localhost:8983/solr");
        connection.setSolrUrl(dataStore);
        when(conf.getSolrConnection()).thenReturn(connection);
        when(clientBuilder.build()).thenReturn(null);
        when(conf.getFilterQuery()).thenReturn(new ArrayList<>());
        when(conf.getRows()).thenReturn("10");
        when(conf.getStart()).thenReturn("0");
        when(solr.query(new SolrQuery("*:*"))).thenReturn(new QueryResponse(solr));
        when(response.getResults()).thenReturn(new SolrDocumentList());
        source.init();
        assertEquals(new SolrDocumentList(), source.getResultList());
    }
}