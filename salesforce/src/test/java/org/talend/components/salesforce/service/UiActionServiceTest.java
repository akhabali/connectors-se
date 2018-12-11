/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package org.talend.components.salesforce.service;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.talend.components.salesforce.service.SalesforceService.URL;

import org.junit.Assert;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.salesforce.SfHeaderFilter;
import org.talend.components.salesforce.dataset.ModuleDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.DecryptedServer;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
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
        final HealthCheckStatus status = service.validateBasicConnection(datasore, i18n);
        assertEquals(i18n.healthCheckOk(), status.getComment());
        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());

    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Validate connection with bad credentials")
    void validateBasicConnectionFailed() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setEndpoint(URL);
        final HealthCheckStatus status = service.validateBasicConnection(datasore, i18n);
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
        assertEquals(354, modules.getItems().size());
        modules.getItems().stream().forEach(c -> Assert.assertNotEquals(c.getLabel(), "AcceptedEventRelation"));
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
    public void retriveColumnsName() {
        final String moduleName = "Account";
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setEndpoint(URL);
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        ModuleDataSet.ColumnSelectionConfig filedNameList = service.defaultColumns(datasore, moduleName);
        assertNotNull(filedNameList);
        assertNotNull(filedNameList.getSelectColumnNames());
        assertEquals(54, filedNameList.getSelectColumnNames().size());

    }

}
