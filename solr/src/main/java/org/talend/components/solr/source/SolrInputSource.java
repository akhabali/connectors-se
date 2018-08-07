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
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.talend.components.solr.common.FilterCriteria;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import org.talend.components.solr.service.SolrConnectorService;

@Slf4j
@Documentation("Solr input source")
public class SolrInputSource implements Serializable {

    private final SolrInputMapperConfiguration configuration;

    private final SolrConnectorService service;

    private final JsonBuilderFactory jsonBuilderFactory;

    private SolrClient solr;

    private List<SolrDocument> resultList;

    private Iterator<SolrDocument> iter;

    public SolrInputSource(@Option("configuration") final SolrInputMapperConfiguration configuration,
            final SolrConnectorService service, final JsonBuilderFactory jsonBuilderFactory) {
        this.configuration = configuration;
        this.service = service;
        this.jsonBuilderFactory = jsonBuilderFactory;
    }

    @PostConstruct
    public void init() {
        solr = new HttpSolrClient.Builder(configuration.getSolrConnection().getFullUrl()).build();
        SolrQuery query = new SolrQuery("*:*");
        configuration.getFilterQuery().forEach(e -> addFilterQuery(e, query));
        query.setRows(parseInt(configuration.getRows()));
        query.setStart(parseInt(configuration.getStart()));
        resultList = executeSolrQuery(solr, query);
        iter = resultList.iterator();
    }

    private void addFilterQuery(FilterCriteria row, SolrQuery query) {
        String field = row.getField();
        String value = row.getValue();
        if (StringUtils.isNotBlank(field) && StringUtils.isNotBlank(value)) {
            query.addFilterQuery(field + ":" + value);
        }
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
            resultList = solr.query(query).getResults();
        } catch (SolrServerException | IOException | SolrException e) {
            log.error(e.getMessage());
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
            log.error(e.getMessage());
        }
    }
}