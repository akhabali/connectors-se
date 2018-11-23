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

package org.talend.components.salesforce.input;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.components.salesforce.service.SalesforceService.URL;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.salesforce.SfHeaderFilter;
import org.talend.components.salesforce.dataset.ModuleQueryDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.DecryptedServer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.http.api.HttpApiHandler;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit.http.junit5.HttpApiInject;
import org.talend.sdk.component.junit.http.junit5.HttpApiName;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.WithMavenServers;
import org.talend.sdk.component.maven.Server;
import org.talend.sdk.component.runtime.manager.chain.Job;

@DisplayName("Suite of test for the Salesforce Input component")
@WithComponents("org.talend.components.salesforce")
@HttpApi(useSsl = true, headerFilter = SfHeaderFilter.class)
@WithMavenServers //
class SalesforceModuleQueryEmitterTest {

    static {
        // System.setProperty("talend.junit.http.capture", "true");
    }

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
    @DisplayName("Bad credentials case")
    void inputWithBadCredential() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setEndpoint(URL);
        datasore.setUserId("badUser");
        datasore.setPassword("badPasswd");
        datasore.setSecurityKey("badSecurityKey");
        final ModuleQueryDataSet moduleQueryDataSet = new ModuleQueryDataSet();
        moduleQueryDataSet.setModuleName("account");
        moduleQueryDataSet.setDataStore(datasore);
        final String config = configurationByExample().forInstance(moduleQueryDataSet).configured().toQueryString();
        final IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job.components().component("salesforce-input", "Salesforce://Input?" + config)
                        .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build()
                        .run());
    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Module selection case [valid]")
    void inputWithModuleNameValid() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setEndpoint(URL);
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final ModuleQueryDataSet moduleQueryDataSet = new ModuleQueryDataSet();
        moduleQueryDataSet.setModuleName("account");
        moduleQueryDataSet.setSelectColumnIds(singletonList("Name"));
        moduleQueryDataSet.setDataStore(datasore);
        moduleQueryDataSet.setCondition("Name Like '%Oil%'");
        final String config = configurationByExample().forInstance(moduleQueryDataSet).configured().toQueryString();
        Job.components().component("salesforce-input", "Salesforce://ModuleQueryInput?" + config)
                .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build().run();
        final List<Record> res = componentsHandler.getCollectedData(Record.class);
        assertEquals(4, res.size());
        assertTrue(res.iterator().next().getString("Name").contains("Oil"));
    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Module selection case [invalid]")
    void inputWithModuleNameInvalid() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setEndpoint(URL);
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final ModuleQueryDataSet moduleQueryDataSet = new ModuleQueryDataSet();
        moduleQueryDataSet.setModuleName("invalid0");
        moduleQueryDataSet.setDataStore(datasore);
        final String config = configurationByExample().forInstance(moduleQueryDataSet).configured().toQueryString();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job.components().component("salesforce-input", "Salesforce://ModuleQueryInput?" + config)
                        .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build()
                        .run());
    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Module selection with fields case [invalid]")
    void inputWithModuleNameValidAndInvalidField() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setEndpoint(URL);
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final ModuleQueryDataSet moduleQueryDataSet = new ModuleQueryDataSet();
        moduleQueryDataSet.setModuleName("account");
        moduleQueryDataSet.setSelectColumnIds(singletonList("InvalidField10x"));
        moduleQueryDataSet.setDataStore(datasore);
        final String config = configurationByExample().forInstance(moduleQueryDataSet).configured().toQueryString();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job.components().component("salesforce-input", "Salesforce://ModuleQueryInput?" + config)
                        .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build()
                        .run());
    }

}
