package org.talend.components.solr.source;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.talend.components.solr.service.SolrConnectorUtils;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import org.talend.components.solr.service.SolrConnectorService;

@Slf4j
@Documentation("Solr input source")
public class SolrInputSource implements Serializable {

    private final SolrInputMapperConfiguration configuration;

    private final SolrConnectorService service;

    private final SolrConnectorUtils util;

    private final JsonBuilderFactory jsonBuilderFactory;

    private SolrClient solr;

    private List<SolrDocument> resultList;

    private Iterator<SolrDocument> iter;

    public SolrInputSource(@Option("configuration") final SolrInputMapperConfiguration configuration,
            final SolrConnectorService service, final JsonBuilderFactory jsonBuilderFactory, final SolrConnectorUtils util) {
        this.configuration = configuration;
        this.service = service;
        this.util = util;
        this.jsonBuilderFactory = jsonBuilderFactory;
    }

    @PostConstruct
    public void init() {
        solr = new HttpSolrClient.Builder(configuration.getSolrDataset().getFullUrl()).build();
        SolrQuery query = new SolrQuery("*:*");
        configuration.getFilterQuery().forEach(e -> util.addFilterQuery(e, query));
        query.setRows(util.parseInt(configuration.getRows()));
        query.setStart(util.parseInt(configuration.getStart()));
        QueryRequest req = new QueryRequest(query);
        req.setBasicAuthCredentials(configuration.getSolrDataset().getSolrUrl().getLogin(),
                configuration.getSolrDataset().getSolrUrl().getPassword());
        resultList = executeSolrQuery(solr, req);
        iter = resultList.iterator();
    }

    private List<SolrDocument> executeSolrQuery(SolrClient solr, QueryRequest req) {
        List<SolrDocument> resultList;
        try {
            resultList = req.process(solr).getResults();
        } catch (SolrServerException | IOException | SolrException e) {
            log.error(util.getMessages(e));
            resultList = new ArrayList<>();
        }
        return resultList;
    }

    @Producer
    public JsonObject next() {
        if (iter.hasNext()) {
            JsonObjectBuilder jsonBuilder = jsonBuilderFactory.createObjectBuilder();
            iter.next().forEach((key, value) -> jsonBuilder.add(key, value.toString()));
            return jsonBuilder.build();
        }
        return null;
    }

    @PreDestroy
    public void release() {
        try {
            solr.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }
}