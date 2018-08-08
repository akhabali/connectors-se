package org.talend.components.solr.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.schema.SchemaRepresentation;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.params.CoreAdminParams;
import org.talend.components.solr.common.SolrDataStore;
import org.talend.components.solr.source.SolrInputMapperConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.schema.Schema;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class SolrConnectorService {

    @DiscoverSchema("discoverSchema")
    public Schema guessTableSchema(SolrInputMapperConfiguration config, SolrConnectorUtils util) {
        HttpSolrClient solrClient = new HttpSolrClient.Builder(config.getSolrConnection().getFullUrl()).build();
        SchemaRepresentation representation = getSchemaRepresentation(solrClient);
        return util.getSchemaFromRepresentation(representation);
    }

    private SchemaRepresentation getSchemaRepresentation(SolrClient solrClient) {
        SchemaRequest schemaRequest = new SchemaRequest();
        SchemaRepresentation representation = null;
        try {
            SchemaResponse schemaResponse = schemaRequest.process(solrClient, null);
            representation = schemaResponse.getSchemaRepresentation();
        } catch (SolrServerException | IOException e) {
            log.error(e.getMessage());
        }
        return representation;
    }

    @Suggestions("coreList")
    public SuggestionValues suggest(@Option("solrUrl") final String solrUrl) {
        return new SuggestionValues(false,
                getCores(solrUrl).stream().map(e -> new SuggestionValues.Item(e, e)).collect(Collectors.toList()));
    }

    private Collection<String> getCores(String solrUrl) {
        HttpSolrClient solrClient = new HttpSolrClient.Builder(solrUrl).build();
        CoreAdminRequest request = new CoreAdminRequest();
        request.setAction(CoreAdminParams.CoreAdminAction.STATUS);
        CoreAdminResponse cores = getCoresFromRequest(request, solrClient);
        return new SolrConnectorUtils().getCoreListFromResponse(cores);
    }

    private CoreAdminResponse getCoresFromRequest(CoreAdminRequest request, HttpSolrClient solrClient) {
        try {
            return request.process(solrClient);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return null;
    }

    @HealthCheck("checkSolrConnection")
    public HealthCheckStatus checkConnection(@Option final SolrDataStore dataStore, final Messages i18n) {
        HttpSolrClient solrClient = new HttpSolrClient.Builder(dataStore.getUrl()).build();
        CoreAdminRequest request = new CoreAdminRequest();
        request.setAction(CoreAdminParams.CoreAdminAction.STATUS);
        HealthCheckStatus status = new HealthCheckStatus();
        try {
            request.process(solrClient);
            status.setStatus(HealthCheckStatus.Status.OK);
            status.setComment(i18n.healthCheckOk());
        } catch (Exception e) {
            status.setStatus(HealthCheckStatus.Status.KO);
            status.setComment(i18n.healthCheckFailed(e.getMessage()));
        }
        return status;
    }
}