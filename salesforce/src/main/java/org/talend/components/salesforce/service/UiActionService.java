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
import org.talend.sdk.component.api.service.update.Update;

import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
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
    public SuggestionValues loadSalesforceModules(@Option("dataStore") final BasicDataStore dataStore) {
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

    @Update("guessSchema")
    public QueryDataSet.SelectedColumnsConfig guessSchema(@Option("dataStore") final BasicDataStore dataStore,
            @Option("moduleName") final String moduleName, @Option("columns") final String columns,
            @Option("selectedColumnsConfig") QueryDataSet.SelectedColumnsConfig selectedColumnsConfig) {
        final QueryDataSet.SelectedColumnsConfig newConfig = new QueryDataSet.SelectedColumnsConfig();
        try {
            if ((columns == null || columns.isEmpty())) {
                final PartnerConnection connection = this.service.connect(dataStore, configuration);
                if (moduleName == null || moduleName.isEmpty()) {
                    return newConfig;
                }
                log.debug("retrieve columns from module: " + moduleName);
                DescribeSObjectResult modules = connection.describeSObject(moduleName);
                final List<String> columnNames = Arrays.stream(modules.getFields()).map(field -> field.getName())
                        .collect(Collectors.toList());
                newConfig.setSelectColumnIds(columnNames);
                return newConfig;
            } else {
                List<String> selectedColumns = selectedColumnsConfig.getSelectColumnIds();
                List<String> newSelectedColumns = new ArrayList<>();
                if (selectedColumns != null && !selectedColumns.isEmpty()) {
                    newSelectedColumns.addAll(selectedColumns);
                    newSelectedColumns.add(columns);
                } else {
                    newSelectedColumns.add(columns);
                }
                newConfig.setSelectColumnIds(newSelectedColumns);
                return newConfig;
            }
        } catch (ConnectionException e) {
            throw service.handleConnectionException(e);
        }
    }

    @Suggestions("retrieveColumns")
    public SuggestionValues retrieveColumns(@Option("dataStore") final BasicDataStore dataStore,
            @Option("moduleName") final String moduleName) {
        try {
            List<SuggestionValues.Item> items = new ArrayList<>();
            final PartnerConnection connection = this.service.connect(dataStore, configuration);
            DescribeSObjectResult module = connection.describeSObject(moduleName);
            for (Field field : module.getFields()) {
                items.add(new SuggestionValues.Item(field.getName(), field.getName()));
            }
            return new SuggestionValues(true, items);
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
