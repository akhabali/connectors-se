package org.talend.components.solr.output;

import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.talend.components.solr.service.SolrConnectorService;
import org.talend.components.solr.service.SolrConnectorUtils;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonObject;
import java.io.IOException;
import java.io.Serializable;

@Slf4j
@Version(1)
@Icon(Icon.IconType.STAR)
@Processor(name = "Output")
@Documentation("Solr processor. Delete and Update methods are available")
public class SolrProcessorOutput implements Serializable {

    private final SolrProcessorOutputConfiguration configuration;

    private final SolrConnectorService service;

    private final SolrConnectorUtils utils;

    private SolrClient solr;

    private UpdateRequest request;

    public SolrProcessorOutput(@Option("configuration") final SolrProcessorOutputConfiguration configuration,
            final SolrConnectorService service, final SolrConnectorUtils utils) {
        this.configuration = configuration;
        this.service = service;
        this.utils = utils;
    }

    @PostConstruct
    public void init() {
        solr = new HttpSolrClient.Builder(configuration.getSolrConnection().getFullUrl()).build();
        request = new UpdateRequest();
        request.setBasicAuthCredentials(configuration.getSolrConnection().getSolrUrl().getLogin(),
                configuration.getSolrConnection().getSolrUrl().getPassword());
    }

    @BeforeGroup
    public void beforeGroup() {
        request.clear();
    }

    @ElementListener
    public void onNext(@Input final JsonObject record) {
        ActionEnum action = configuration.getAction();
        if (ActionEnum.UPDATE == action) {
            update(record);
        } else if (ActionEnum.DELETE == action) {
            deleteDocument(record);
        }
    }

    private void update(JsonObject record) {
        SolrInputDocument doc = new SolrInputDocument();
        record.keySet().forEach(e -> doc.addField(e, utils.trimQuotes(getStringValue(record, e))));
        request.add(doc);
    }

    private String getStringValue(JsonObject record, String key) {
        if (record != null && record.get(key) != null) {
            return record.get(key).toString();
        }
        return "";
    }

    private void deleteDocument(JsonObject record) {
        String query = utils.createQueryFromRecord(record);
        request.deleteByQuery(query);
    }

    @AfterGroup
    public void afterGroup() {
        try {
            request.commit(solr, null);
        } catch (SolrServerException | IOException | SolrException e) {
            log.error(utils.getMessages(e));
        }
    }

    @PreDestroy
    public void release() {
        try {
            solr.close();
        } catch (IOException | SolrException e) {
            log.error(utils.getMessages(e));
        }
    }
}