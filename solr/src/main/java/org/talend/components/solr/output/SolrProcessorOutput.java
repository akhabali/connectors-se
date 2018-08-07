package org.talend.components.solr.output;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.talend.components.solr.service.SolrConnectorService;
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
import java.util.Set;

@Slf4j
@Version(1)
@Icon(Icon.IconType.STAR)
@Processor(name = "Output")
@Documentation("Solr processor. Delete and Update methods are available")
public class SolrProcessorOutput implements Serializable {

    private final SolrProcessorOutputConfiguration configuration;

    private final SolrConnectorService service;

    private SolrClient solr;

    public SolrProcessorOutput(@Option("configuration") final SolrProcessorOutputConfiguration configuration,
            final SolrConnectorService service) {
        this.configuration = configuration;
        this.service = service;
    }

    @PostConstruct
    public void init() {
        solr = new HttpSolrClient.Builder(configuration.getSolrConnection().getFullUrl()).build();
    }

    @BeforeGroup
    public void beforeGroup() {
    }

    @ElementListener
    public void onNext(@Input final JsonObject record) {
        ActionEnum action = configuration.getAction();
        if (ActionEnum.UPDATE == action) {
            update(record);
        } else if (ActionEnum.DELETE == action) {
            deleteDocument(record, true);
        }
    }

    private void update(JsonObject record) {
        SolrInputDocument doc = new SolrInputDocument();
        record.keySet().forEach(e -> doc.addField(e, trimQuotes(record.getString(e))));
        try {
            solr.add(doc);
            solr.commit();
        } catch (SolrServerException | IOException e) {
            log.error(e.getMessage());
        }
    }

    private String trimQuotes(String value) {
        int length = value.length();
        if (length >= 2 && value.charAt(0) == '"' && value.charAt(length - 1) == '"') {
            return value.substring(1, length - 1);
        }
        return value;
    }

    private void deleteDocument(JsonObject record) {
        deleteDocument(record, false);
    }

    private void deleteDocument(JsonObject record, boolean commit) {
        String query = createQueryFromRecord(record);
        try {
            solr.deleteByQuery(query);
            if (commit)
                solr.commit();
        } catch (SolrServerException | IOException | SolrException e) {
            log.error(e.getMessage());
        }
    }

    private String createQueryFromRecord(JsonObject record) {
        StringBuilder query = new StringBuilder();
        Set<String> keySet = record.keySet();
        boolean isFirst = true;
        for (String key : keySet) {
            String value = getStringValue(key, record);
            if (StringUtils.isNotBlank(value)) {
                String subQuery = (isFirst ? "" : " AND ") + key + ":" + addQuotes(value);
                query.append(subQuery);
                isFirst = false;
            }
        }

        return query.toString();
    }

    private String getStringValue(String key, JsonObject record) {
        return record.get(key) != null ? record.get(key).toString() : null;
    }

    private String addQuotes(String value) {
        int length = value.length();
        if (length >= 2 && !(value.charAt(0) == '"' && value.charAt(length - 1) == '"')
                && StringUtils.containsWhitespace(value)) {
            return "\"" + value + "\"";
        }
        return value;
    }

    @AfterGroup
    public void afterGroup() {
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