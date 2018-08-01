package org.talend.components.solr.source;

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
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import org.talend.components.solr.service.Solr_connectorService;

@Slf4j
@Documentation("TODO fill the documentation for this source")
public class TSolrInputSource implements Serializable {

    private final TSolrInputMapperConfiguration configuration;

    private final Solr_connectorService service;

    private final JsonBuilderFactory jsonBuilderFactory;

    private List<SolrDocument> resultList;

    private Iterator<SolrDocument> iter;

    public TSolrInputSource(@Option("configuration") final TSolrInputMapperConfiguration configuration,
            final Solr_connectorService service, final JsonBuilderFactory jsonBuilderFactory) {
        this.configuration = configuration;
        this.service = service;
        this.jsonBuilderFactory = jsonBuilderFactory;
    }

    @PostConstruct
    public void init() {
        SolrClient solr = new HttpSolrClient.Builder(configuration.getSolrConnection().getFullUrl()).build();
        SolrQuery query = new SolrQuery("*:*");
        configuration.getFilterQuery().forEach(e -> query.addFilterQuery(e.getField() + ":" + e.getValue()));
        query.setRows(parseInt(configuration.getRows()));
        query.setStart(parseInt(configuration.getStart()));
        resultList = executeSolrQuery(solr, query);
        iter = resultList.iterator();
    }

    private String wrapFqValue(String fqValue) {
        if (fqValue.contains(" ")) {
            return "\"" + fqValue + "\"";
        }
        return fqValue;
    }

    private Integer parseInt(String value) {
        Integer result = 0;
        try {
            result = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.warn(e.getMessage());
        }
        return result;
    }

    private List<SolrDocument> executeSolrQuery(SolrClient solr, SolrQuery query) {
        List<SolrDocument> resultList;
        try {
            QueryResponse response = solr.query(query);
            resultList = response.getResults();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            resultList = new ArrayList<>();
        }
        return resultList;
    }

    @Producer
    public JsonObject next() {
        if (iter.hasNext()) {
            JsonObjectBuilder jsonBuilder = jsonBuilderFactory.createObjectBuilder();
            iter.next().forEach((key, value) -> jsonBuilder.add(key, value.toString())); // todo need to add number support
            return jsonBuilder.build();
        }
        return null;
    }

    @PreDestroy
    public void release() {
        // this is the symmetric method of the init() one,
        // release potential connections you created or data you cached
    }
}