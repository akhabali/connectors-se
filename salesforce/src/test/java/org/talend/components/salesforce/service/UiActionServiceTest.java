package org.talend.components.salesforce.service;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.talend.components.salesforce.service.SalesforceService.URL;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.salesforce.SfHeaderFilter;
import org.talend.components.salesforce.dataset.QueryDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.DecryptedServer;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.http.api.HttpApiHandler;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit.http.junit5.HttpApiInject;
import org.talend.sdk.component.junit.http.junit5.HttpApiName;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.WithMavenServers;
import org.talend.sdk.component.maven.Server;

@WithComponents("org.talend.components.salesforce")
@HttpApi(useSsl = true, headerFilter = SfHeaderFilter.class)
@WithMavenServers //
class UiActionServiceTest {

    static {
        // System.setProperty("talend.junit.http.capture", "true");
    }

    @Service
    RecordBuilderFactory factory;

    @Service
    private UiActionService service;

    @Service
    private Messages i18n;

    @Service
    private LocalConfiguration configuration;

    @Injected
    private BaseComponentsHandler componentsHandler;

    @HttpApiInject
    private HttpApiHandler<?> httpApiHandler;

    @DecryptedServer(value = "salesforce-password", alwaysTryLookup = false)
    private Server serverWithPassword;

    @DecryptedServer(value = "salesforce-securitykey", alwaysTryLookup = false)
    private Server serverWithSecuritykey;

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Validate connection")
    void validateBasicConnectionOK() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setEndpoint(URL);
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final HealthCheckStatus status = service.validateBasicConnection(datasore, i18n, configuration);
        assertEquals(i18n.healthCheckOk(), status.getComment());
        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());

    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Validate connection with bad credentials")
    void validateBasicConnectionFailed() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setEndpoint(URL);
        final HealthCheckStatus status = service.validateBasicConnection(datasore, i18n, configuration);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
        assertFalse(status.getComment().isEmpty());
    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Load modules")
    void loadModules() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setEndpoint(URL);
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final SuggestionValues modules = service.loadSalesforceModules(datasore);
        assertNotNull(modules);
        assertTrue(modules.isCacheable());
        assertEquals(376, modules.getItems().size());
    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Load modules with bad basic credentials")
    void loadModulesWithBadCredentials() {
        assertThrows(IllegalStateException.class, () -> {
            final BasicDataStore datasore = new BasicDataStore();
            datasore.setEndpoint(URL);
            datasore.setUserId("basUserName");
            datasore.setPassword("NoPass");
            service.loadSalesforceModules(datasore);
        });
    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Retrive module column names")
    void retriveColumnsName() {
        final String moduleName = "Account";
        final QueryDataSet dataSet = new QueryDataSet();
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setEndpoint(URL);
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        dataSet.setDataStore(datasore);
        dataSet.setModuleName(moduleName);
        Schema schema = service.addColumns(dataSet, factory);
        assertNotNull(schema);
        List<Schema.Entry> columns = schema.getEntries();
        assertNotNull(columns);
        assertEquals(58, columns.size());

        List<String> selectedColumns = new ArrayList<>();
        selectedColumns.add("Id");
        selectedColumns.add("Name");
        SuggestionValues values = service.retrieveColumns(datasore, moduleName, selectedColumns);
        assertEquals(56, values.getItems().size());
    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("add selected column")
    void addColumnsName() {
        final QueryDataSet dataSet = new QueryDataSet();
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setEndpoint(URL);
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        dataSet.setDataStore(datasore);
        dataSet.setModuleName("Account");
        // 1. add selected column "Id" to "selectColumnIds"
        dataSet.setSelectedColumn("Id");
        dataSet.setAddAllColumns(false);
        Schema schema = service.addColumns(dataSet, factory);
        assertEquals(1, schema.getEntries().size());
        // 2. can't add duplicate column to "selectColumnIds"
        List<String> selectedColumns = new ArrayList<>();
        selectedColumns.add("Id");
        selectedColumns.add("Name");
        dataSet.setSelectColumnIds(selectedColumns);
        dataSet.setSelectedColumn("Name");
        schema = service.addColumns(dataSet, factory);
        assertEquals(2, schema.getEntries().size());
        // 3. add new columns to "selectColumnIds"
        dataSet.setSelectedColumn("IsDeleted");
        schema = service.addColumns(dataSet, factory);
        assertEquals(3, schema.getEntries().size());
    }

    @Test
    @DisplayName("Get default endpoint")
    void getDefaultEndpoint() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId("basUserName");
        datasore.setPassword("NoPass");
        final SuggestionValues endpoint = service.getEndpoint();

        assertEquals(1, endpoint.getItems().size());
        for (SuggestionValues.Item item : endpoint.getItems()) {
            assertEquals(URL, item.getLabel());
        }
    }

}
