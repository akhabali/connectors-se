// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo.service;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.marketo.MarketoBaseTest;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoInputConfiguration.ListAction;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true, responseLocator = org.talend.sdk.component.junit.http.internal.impl.MarketoResponseLocator.class)
@WithComponents("org.talend.components.marketo")
class UIActionServiceTest extends MarketoBaseTest {

    @BeforeEach
    protected void setUp() {
        super.setUp();
        inputConfiguration.setCustomObjectName("car_c");
        inputConfiguration.setListAction(ListAction.getLeads);
        outputConfiguration.getDataSet().setEntity(MarketoEntity.CustomObject);
    }

    @Test
    void doHealthCheckOk() {
        final HealthCheckStatus status = uiActionService.doHealthCheck(dataStore, i18n);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }

    @Test
    void doHealthCheckInvalidClientId() {
        dataStore.setClientId("invalid");
        final HealthCheckStatus status = uiActionService.doHealthCheck(dataStore, i18n);
        assertNotNull(status);
        assertEquals(Status.KO, status.getStatus());
    }

    @Test
    void doHealthCheckInvalidClientSecret() {
        dataStore.setClientSecret("invalid");
        final HealthCheckStatus status = uiActionService.doHealthCheck(dataStore, i18n);
        assertNotNull(status);
        assertEquals(Status.KO, status.getStatus());
    }

    @Test
    void doHealthCheckInvalidUrl() {
        dataStore.setEndpoint(MARKETO_ENDPOINT + "/bzh");
        final HealthCheckStatus status = uiActionService.doHealthCheck(dataStore, i18n);
        assertNotNull(status);
        assertEquals(Status.KO, status.getStatus());
    }

    @Test
    void validateUrlOk() {
        String url = "https://123-ABC-456.mktorest.com/";
        final ValidationResult res = uiActionService.validateUrl(url);
        assertNotNull(res);
        assertEquals(ValidationResult.Status.OK, res.getStatus());
    }

    @Test
    void validateUrlKo() {
        String url = "htps://123-ABC-456.mktorest.com/";
        final ValidationResult res = uiActionService.validateUrl(url);
        assertNotNull(res);
        assertEquals(ValidationResult.Status.KO, res.getStatus());
    }

    @Test
    void getLeadKeyNames() {
        assertEquals(
                Arrays.asList("id", "cookie", "email", "twitterId", "facebookId", "linkedInId", "sfdcAccountId", "sfdcContactId",
                        "sfdcLeadId", "sfdcLeadOwnerId", "sfdcOpptyId", "Custom"),
                uiActionService.getLeadKeyNames(null).getItems().stream().map(item -> item.getId()).collect(Collectors.toList()));
    }

    @Test
    void testGetActivities() {
        SuggestionValues activities = uiActionService.getActivities(inputConfiguration.getDataSet().getDataStore());
        assertNotNull(activities);
        assertTrue(activities.getItems().size() > 20);
        inputConfiguration.getDataSet().getDataStore().setEndpoint(null);
        activities = uiActionService.getActivities(inputConfiguration.getDataSet().getDataStore());
        assertNotNull(activities);
        assertEquals(60, activities.getItems().size());
    }

    @Test
    void getListNames() {
        inputConfiguration.getDataSet().setEntity(MarketoEntity.List);
        SuggestionValues listNames = uiActionService.getListNames(inputConfiguration.getDataSet().getDataStore());
        assertNotNull(listNames);
        listNames.getItems().stream().forEach(item -> assertNotNull(item.getId()));
    }

    @Test
    void getCustomObjectNames() {
        inputConfiguration.getDataSet().setEntity(MarketoEntity.CustomObject);
        SuggestionValues customObjectNames = uiActionService.getCustomObjectNames(inputConfiguration.getDataSet().getDataStore());
        assertNotNull(customObjectNames);
        customObjectNames.getItems().stream().forEach(item -> assertNotNull(item.getId()));
    }

    @Test
    void getFieldNames() {
        inputConfiguration.getDataSet().setEntity(MarketoEntity.Lead);
        SuggestionValues fieldNames = uiActionService.getFieldNames(inputConfiguration.getDataSet().getDataStore(),
                MarketoEntity.Lead.name(), null);
        assertNotNull(fieldNames);
        assertThat(fieldNames.getItems().size(), is(greaterThan(100)));
        fieldNames.getItems().stream().forEach(item -> assertNotNull(item.getId()));
    }

}
