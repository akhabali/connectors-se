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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.talend.components.solr.common.SolrDataset;
import org.talend.components.solr.common.SolrDataStore;
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
    public Schema guessTableSchema(SolrDataset dataset, SolrConnectorUtils util) {
        HttpSolrClient solrClient = new HttpSolrClient.Builder(dataset.getFullUrl()).build();
        SchemaRepresentation representation = getSchemaRepresentation(solrClient, dataset.getSolrUrl().getLogin(),
                dataset.getSolrUrl().getPassword(), util);
        return util.getSchemaFromRepresentation(representation);
    }

    private SchemaRepresentation getSchemaRepresentation(SolrClient solrClient, String login, String pass,
            SolrConnectorUtils util) {
        SchemaRequest schemaRequest = new SchemaRequest();
        schemaRequest.setBasicAuthCredentials(login, pass);
        SchemaRepresentation representation = null;
        try {
            SchemaResponse schemaResponse = schemaRequest.process(solrClient, null);
            representation = schemaResponse.getSchemaRepresentation();
        } catch (SolrServerException | IOException | SolrException e) {
            log.error(util.getMessages(e));
        }
        return representation;
    }

    @Suggestions("coreList")
    public SuggestionValues suggest(@Option("a") final String solrUrl, @Option("b") final String login,
            @Option("c") final String password, SolrConnectorUtils util) {
        return new SuggestionValues(false, getCores(solrUrl, login, password, util).stream()
                .map(e -> new SuggestionValues.Item(e, e)).collect(Collectors.toList()));
    }

    private Collection<String> getCores(String solrUrl, String login, String password, SolrConnectorUtils util) {
        HttpSolrClient solrClient = new HttpSolrClient.Builder(solrUrl).build();
        CoreAdminRequest request = new CoreAdminRequest();
        request.setBasicAuthCredentials(login, password);
        request.setAction(CoreAdminParams.CoreAdminAction.STATUS);
        CoreAdminResponse cores = getCoresFromRequest(request, solrClient, util);
        return util.getCoreListFromResponse(cores);
    }

    private CoreAdminResponse getCoresFromRequest(CoreAdminRequest request, HttpSolrClient solrClient, SolrConnectorUtils util) {
        try {
            return request.process(solrClient);
        } catch (Exception e) {
            log.error(util.getMessages(e));
        }
        return null;
    }

    @HealthCheck("checkSolrConnection")
    public HealthCheckStatus checkConnection(@Option final SolrDataStore dataStore, final Messages i18n,
            SolrConnectorUtils util) {
        HttpSolrClient solrClient = new HttpSolrClient.Builder(dataStore.getUrl()).build();
        CoreAdminRequest request = new CoreAdminRequest();
        request.setAction(CoreAdminParams.CoreAdminAction.STATUS);
        request.setBasicAuthCredentials(dataStore.getLogin(), dataStore.getPassword());
        HealthCheckStatus status = new HealthCheckStatus();
        try {
            request.process(solrClient);
            status.setStatus(HealthCheckStatus.Status.OK);
            status.setComment(i18n.healthCheckOk());
        } catch (Exception e) {
            status.setStatus(HealthCheckStatus.Status.KO);
            String errorMessage = util.getCustomLocalizedMessage(util.getMessages(e), i18n);
            status.setComment(i18n.healthCheckFailed(errorMessage));
        }
        return status;
    }
}