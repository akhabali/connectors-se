package org.talend.components.output;

import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.talend.components.service.Solr_connectorService;
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

import static org.talend.components.output.ActionEnum.DELETE;
import static org.talend.components.output.ActionEnum.UPDATE;

@Slf4j
@Version(1) // default version is 1, if some configuration changes happen between 2 versions you can add a migrationHandler
@Icon(Icon.IconType.STAR) // you can use a custom one using @Icon(value=CUSTOM, custom="filename") and adding
                          // icons/filename_icon32.png in resources
@Processor(name = "tSolrDeleteDocument")
@Documentation("TODO fill the documentation for this processor")
public class TSolrDeleteDocumentOutput implements Serializable {

    private final TSolrDeleteDocumentOutputConfiguration configuration;

    private final Solr_connectorService service;

    private SolrClient solr;

    public TSolrDeleteDocumentOutput(@Option("configuration") final TSolrDeleteDocumentOutputConfiguration configuration,
            final Solr_connectorService service) {
        this.configuration = configuration;
        this.service = service;
    }

    @PostConstruct
    public void init() {
        solr = new HttpSolrClient.Builder(configuration.getSolrUrl() + configuration.getCore()).build();
    }

    @BeforeGroup
    public void beforeGroup() {
        // if the environment supports chunking this method is called at the beginning if a chunk
        // it can be used to start a local transaction specific to the backend you use
        // Note: if you don't need it you can delete it
    }

    @ElementListener
    public void onNext(@Input final JsonObject record) {
        // this is the method allowing you to handle the input(s) and emit the output(s)
        // after some custom logic you put here, to send a value to next element you can use an
        // output parameter and call emit(value).
        ActionEnum action = configuration.getAction();
        if (UPDATE == action) {
            log.info("update");
            update(record);
        } else if (DELETE == action) {
            log.info("delete");
            deleteDocument(record.getString("id"), true);
        }
    }

    private void update(JsonObject record) {
        SolrInputDocument doc = new SolrInputDocument();
        record.keySet().forEach(e -> doc.addField(e, trimQuotes(record.getString(e))));
        try {
            solr.add(doc);
            solr.commit();
        } catch (SolrServerException | IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    private String trimQuotes(String value) {
        int length = value.length();
        if (length >= 2 && value.charAt(0) == '"' && value.charAt(length - 1) == '"') {
            return value.substring(1, length - 1);
        }
        return value;
    }

    private void deleteDocument(String id) {
        deleteDocument(id, false);
    }

    private void deleteDocument(String id, boolean commit) {
        try {
            solr.deleteByQuery("id:" + id); // TODO solr.deleteById(id) must be used
            if (commit)
                solr.commit();
        } catch (SolrServerException | IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    @AfterGroup
    public void afterGroup() {
        // symmetric method of the beforeGroup() executed after the chunk processing
        // Note: if you don't need it you can delete it
    }

    @PreDestroy
    public void release() {
        // this is the symmetric method of the init() one,
        // release potential connections you created or data you cached
        // Note: if you don't need it you can delete it
    }
}