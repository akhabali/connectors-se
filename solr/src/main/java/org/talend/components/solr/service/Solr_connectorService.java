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
import org.talend.components.solr.output.TSolrProcessorOutputConfiguration;
import org.talend.components.solr.source.TSolrInputMapperConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Type;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
@Service
public class Solr_connectorService {

    private static final String SOLR_FIELD_PROPERTY_INDEXED = "indexed";

    private static final String SOLR_FIELD_PROPERTY_STORED = "stored";

    private static final String SOLR_FIELD_PROPERTY_NAME = "name";

    private static final String SOLR_FIELD_PROPERTY_TYPE = "type";

    private static final Set SOLR_FIELD_PROPERTY_TYPES_DOUBLE = Stream.of("pdouble", "pfloat").collect(Collectors.toSet());

    private static final Set SOLR_FIELD_PROPERTY_TYPES_INT = Stream.of("plong", "pint").collect(Collectors.toSet());

    private static final Set SOLR_FIELD_PROPERTY_TYPES_BOOL = Stream.of("boolean").collect(Collectors.toSet());

    @DiscoverSchema("discoverSchema")
    public Schema guessTableSchema(TSolrInputMapperConfiguration config) {
        HttpSolrClient solrClient = new HttpSolrClient.Builder(config.getSolrConnection().getFullUrl()).build();
        SchemaRepresentation representation = getSchemaRepresentation(solrClient);
        return getSchemaFromRepresentation(representation);
    }

    @DiscoverSchema("discoverOutPutSchema")
    public Schema guessOutPutSchema(TSolrProcessorOutputConfiguration config) {
        HttpSolrClient solrClient = new HttpSolrClient.Builder(config.getSolrConnection().getFullUrl()).build();
        SchemaRepresentation representation = getSchemaRepresentation(solrClient);
        return getSchemaFromRepresentation(representation);
    }

    private SchemaRepresentation getSchemaRepresentation(SolrClient solrClient) {
        SchemaRequest schemaRequest = new SchemaRequest();
        SchemaRepresentation representation = null;
        try {
            SchemaResponse schemaResponse = schemaRequest.process(solrClient, null);
            representation = schemaResponse.getSchemaRepresentation();
        } catch (SolrServerException | IOException e) {
            log.error(e.getMessage(), e);
        }
        return representation;
    }

    private Schema getSchemaFromRepresentation(SchemaRepresentation representation) {
        if (representation == null) {
            return new Schema(Collections.emptyList());
        }
        List<Map<String, Object>> fields = representation.getFields();
        List<Schema.Entry> entries = new ArrayList<>();
        for (Map<String, Object> field : fields) {
            String fieldName = getFieldName(field);
            if (fieldName != null && checkIndexed(field) && checkStored(field)) {
                entries.add(new Schema.Entry(fieldName, getFieldType(field)));
            }
        }
        return new Schema(entries);
    }

    private boolean checkIndexed(Map<String, Object> field) {
        Object indexed = field.get(SOLR_FIELD_PROPERTY_INDEXED);
        return (indexed == null || indexed.equals(true));
    }

    private boolean checkStored(Map<String, Object> field) {
        Object stored = field.get(SOLR_FIELD_PROPERTY_STORED);
        return (stored == null || stored.equals(true));
    }

    private String getFieldName(Map<String, Object> field) {
        Object name = field.get(SOLR_FIELD_PROPERTY_NAME);
        if (name == null) {
            return null;
        }
        return name.toString();
    }

    private Type getFieldType(Map<String, Object> field) {
        Object type = field.get(SOLR_FIELD_PROPERTY_TYPE);
        if (SOLR_FIELD_PROPERTY_TYPES_INT.contains(type)) {
            return Type.INT;
        } else if (SOLR_FIELD_PROPERTY_TYPES_BOOL.contains(type)) {
            return Type.BOOLEAN;
        } else if (SOLR_FIELD_PROPERTY_TYPES_DOUBLE.contains(type)) {
            return Type.DOUBLE;
        } else {
            return Type.STRING;
        }
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
        return getCoreListFromResponse(cores);
    }

    private CoreAdminResponse getCoresFromRequest(CoreAdminRequest request, HttpSolrClient solrClient) {
        try {
            return request.process(solrClient);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    private Collection<String> getCoreListFromResponse(CoreAdminResponse cores) {
        if (cores != null) {
            return IntStream.range(0, cores.getCoreStatus().size()).mapToObj(i -> cores.getCoreStatus().getName(i))
                    .collect(Collectors.toList());
        }
        return new ArrayList<>();
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