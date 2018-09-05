package org.talend.components.salesforce.service;

import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.KO;
import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.OK;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.talend.components.salesforce.dataset.QueryDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Type;

import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.ws.ConnectionException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class UiActionService {

    public static final String GET_ENDPOINT = "GET_ENDPOINT";

    @Service
    private SalesforceService service;

    @Service
    private LocalConfiguration configuration;

    @HealthCheck("basic.healthcheck")
    public HealthCheckStatus validateBasicConnection(@Option final BasicDataStore datastore, final Messages i18n,
            LocalConfiguration configuration) {
        try {
            this.service.connect(datastore, configuration);
        } catch (ConnectionException ex) {
            String error;
            if (ApiFault.class.isInstance(ex)) {
                final ApiFault fault = ApiFault.class.cast(ex);
                error = fault.getExceptionCode() + " " + fault.getExceptionMessage();
            } else {
                error = ex.getMessage();
            }
            return new HealthCheckStatus(KO, i18n.healthCheckFailed(error));
        }
        return new HealthCheckStatus(OK, i18n.healthCheckOk());
    }

    @Suggestions("loadSalesforceModules")
    public SuggestionValues loadSalesforceModules(@Option final BasicDataStore dataStore) {
        try {
            List<SuggestionValues.Item> items = new ArrayList<>();
            final PartnerConnection connection = this.service.connect(dataStore, configuration);
            DescribeGlobalSObjectResult[] modules = connection.describeGlobal().getSobjects();
            for (DescribeGlobalSObjectResult module : modules) {
                items.add(new SuggestionValues.Item(module.getName(), module.getLabel()));
            }
            return new SuggestionValues(true, items);
        } catch (ConnectionException e) {
            throw service.handleConnectionException(e);
        }
    }

    @DiscoverSchema("retrieveColumns")
    public Schema retrieveColumns(@Option final QueryDataSet dataSet) {
        final BasicDataStore dataStore = dataSet.getDataStore();
        try {
            final PartnerConnection connection = this.service.connect(dataStore, configuration);
            DescribeSObjectResult modules = connection.describeSObject(dataSet.getModuleName());
            List<Schema.Entry> entries = Arrays.stream(modules.getFields())
                    // TODO change the default type
                    .map(field -> new Schema.Entry(field.getName(), Type.STRING)).collect(Collectors.toList());
            return new Schema(entries);
        } catch (ConnectionException e) {
            throw service.handleConnectionException(e);
        }
    }

    @Suggestions(GET_ENDPOINT)
    public SuggestionValues getEndpoint() {
        final String endpoint = this.service.getEndpoint(configuration);
        return new SuggestionValues(false, Arrays.asList(new SuggestionValues.Item(endpoint, endpoint)));
    }

}
