package org.talend.components.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.params.CoreAdminParams;
import org.talend.components.source.TSolrInputMapperConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Service
public class Solr_connectorService {

    @DiscoverSchema("discoverSchema")
    public Schema guessTableSchema(TSolrInputMapperConfiguration config) {

        Collection<Schema.Entry> entries = Arrays.asList(new Schema.Entry("id", Type.STRING),
                new Schema.Entry("compName_s", Type.STRING), new Schema.Entry("address_s", Type.STRING),
                new Schema.Entry("test_field", Type.STRING), new Schema.Entry("_version_", Type.STRING));

        return new Schema(entries);
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
}