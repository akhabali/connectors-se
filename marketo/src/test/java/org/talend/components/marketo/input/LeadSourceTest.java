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
package org.talend.components.marketo.input;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_EMAIL;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;
import javax.json.JsonObject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.marketo.component.DataCollector;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoInputDataSet.LeadAction;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true, responseLocator = org.talend.sdk.component.junit.http.internal.impl.MarketoResponseLocator.class)
@WithComponents("org.talend.components.marketo")
public class LeadSourceTest extends SourceBaseTest {

    static final String LEAD_IDS_KEY_VALUES = "1,2,3,4,5";

    private String LEAD_EMAIL = "Marketo@talend.com";

    private Integer LEAD_ID = 4;

    private Integer INVALID_LEAD_ID = -5;

    private LeadSource source;

    private String fields = "company,site,billingStreet,billingCity,billingState,billingCountry,billingPostalCode,website,mainPhone,annualRevenue,numberOfEmployees,industry,sicCode,mktoCompanyNotes,externalCompanyId,id,mktoName,personType,mktoIsPartner,isLead,mktoIsCustomer,isAnonymous,salutation,firstName,middleName,lastName,email,phone,mobilePhone,fax,title,contactCompany,dateOfBirth,address,city,state,country,postalCode,personTimeZone,originalSourceType,originalSourceInfo,registrationSourceType,registrationSourceInfo,originalSearchEngine,originalSearchPhrase,originalReferrer,emailInvalid,emailInvalidCause,unsubscribed,unsubscribedReason,doNotCall,mktoDoNotCallCause,doNotCallReason,mktoPersonNotes,anonymousIP,inferredCompany,inferredCountry,inferredCity,inferredStateRegion,inferredPostalCode,inferredMetropolitanArea,inferredPhoneAreaCode,department,createdAt,updatedAt,cookies,externalSalesPersonId,leadPerson,leadRole,leadSource,leadStatus,leadScore,urgency,priority,relativeScore,relativeUrgency,rating,personPrimaryLeadInterest,leadPartitionId,leadRevenueCycleModelId,leadRevenueStageId,gender,facebookDisplayName,twitterDisplayName,linkedInDisplayName,facebookProfileURL,twitterProfileURL,linkedInProfileURL,facebookPhotoURL,twitterPhotoURL,linkedInPhotoURL,facebookReach,twitterReach,linkedInReach,facebookReferredVisits,twitterReferredVisits,linkedInReferredVisits,totalReferredVisits,facebookReferredEnrollments,twitterReferredEnrollments,linkedInReferredEnrollments,totalReferredEnrollments,lastReferredVisit,lastReferredEnrollment,syndicationId,facebookId,twitterId,linkedInId,acquisitionProgramId,mktoAcquisitionDate";

    private Consumer<Record> leadAsserter = record -> {
        assertThat(record.getInt("id"), is(both(greaterThanOrEqualTo(0)).and(lessThanOrEqualTo(1500))));
        assertEquals("Web service API", record.getString("originalSourceType"));
        assertThat(record.getString("website"), containsString("talend.com"));
        assertNull(record.getString("twitterId"));
        assertEquals(0, record.getInt("twitterReferredVisits"));
        assertTrue(record.getBoolean("isLead"));
        assertFalse(record.getBoolean("isAnonymous"));
        assertFalse(record.getBoolean("mktoIsCustomer"));
        Assertions.assertNotNull(record.getDateTime("createdAt"));
        Assertions.assertNotNull(record.getDateTime("updatedAt"));
    };

    private transient static final Logger LOG = LoggerFactory.getLogger(LeadSourceTest.class);

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();
        inputDataSet.setEntity(MarketoEntity.Lead);
    }

    @Test
    void testDescribeLead() {
        inputDataSet.setLeadAction(LeadAction.describeLead);
        source = new LeadSource(inputDataSet, service, tools);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    @Test
    void testGetActivities() {
        source = new LeadSource(inputDataSet, service, tools);
        source.getAccessToken();
        JsonObject activities = source.getActivities();
        assertTrue(activities.getJsonArray("result").size() > 30);
    }

    @Test
    void testGetLead() {
        inputDataSet.setLeadAction(LeadAction.getLead);
        inputDataSet.setLeadId(LEAD_ID);
        source = new LeadSource(inputDataSet, service, tools);
        source.init();
        result = source.next();
        assertNotNull(result);
        assertEquals("TDI38486_2_SOAP@talend.com", result.getString(ATTR_EMAIL));
        assertNull(source.next());
    }

    @Test
    void testGetLeadNotFound() {
        inputDataSet.setLeadAction(LeadAction.getLead);
        inputDataSet.setLeadId(INVALID_LEAD_ID);
        source = new LeadSource(inputDataSet, service, tools);
        source.init();
        assertNull(source.next());
    }

    @Test
    void testGetMultipleLeads() {
        setMultipleLeadsDefault();
        source = new LeadSource(inputDataSet, service, tools);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
        // will all fields
        inputDataSet.setFields(asList(fields.split(",")));
        source = new LeadSource(inputDataSet, service, tools);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    @Test
    void testGetMultipleLeadsWithAllFields() {
        setMultipleLeadsDefault();
        inputDataSet.setFields(asList(fields.split(",")));
        source = new LeadSource(inputDataSet, service, tools);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    @Test
    void testGetMultipleLeadsWithAllFieldsOver8k() {
        setMultipleLeadsDefault();
        List<String> longFields = new ArrayList<>();
        longFields.addAll(asList(fields.split(",")));
        longFields.add(String.join(" ", Collections.nCopies(5000, " ")));
        inputDataSet.setFields(longFields);
        source = new LeadSource(inputDataSet, service, tools);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    @Test
    void testGetMultipleLeadsWithUnknownField() {
        setMultipleLeadsDefault();
        inputDataSet.setFields(asList("unknownField"));
        source = new LeadSource(inputDataSet, service, tools);
        try {
            source.init();
            fail("[1006] Field 'unknownField' not found -> should have been raised");
        } catch (Exception e) {
        }
    }

    @Test
    void testGetMultipleLeadsWithPager() {
        setMultipleLeadsDefault();
        source = new LeadSource(inputDataSet, service, tools);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    private void setMultipleLeadsDefault() {
        inputDataSet.setLeadAction(LeadAction.getMultipleLeads);
        inputDataSet.setLeadKeyName(ATTR_ID);
        inputDataSet.setLeadKeyValues(LEAD_IDS_KEY_VALUES);
    }

    @Test
    void testGetLeadChanges() {
        inputDataSet.setLeadAction(LeadAction.getLeadChanges);
        inputDataSet.setSinceDateTime("2018-01-01 00:00:01 Z");
        inputDataSet.setFields(asList(fields.split(",")));
        source = new LeadSource(inputDataSet, service, tools);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    @Test
    void testGetLeadChangesWithPaging() {
        inputDataSet.setLeadAction(LeadAction.getLeadChanges);
        inputDataSet.setSinceDateTime("2018-01-01 00:00:01 Z");
        inputDataSet.setFields(asList(fields.split(",")));
        source = new LeadSource(inputDataSet, service, tools);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    @Test
    void testGetLeadActivities() {
        inputDataSet.setLeadAction(LeadAction.getLeadActivity);
        inputDataSet.setSinceDateTime("2018-01-01 00:00:01 Z");
        SuggestionValues acts = uiActionService.getActivities(inputDataSet.getDataStore());
        List<String> activities = activities = acts.getItems().stream().limit(10).map(item -> String.valueOf(item.getId()))
                .collect(toList());
        LOG.debug("[testGetLeadActivities] activities: {}", activities);
        inputDataSet.setActivityTypeIds(activities);
        inputDataSet.setFields(asList(fields.split(",")));
        source = new LeadSource(inputDataSet, service, tools);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    @Test
    void getLead() {
        inputDataSet.setLeadAction(LeadAction.getLead);
        inputDataSet.setLeadId(LEAD_ID);
        inputDataSet.setFields(asList(fields.split(",")));
        final String config = configurationByExample().forInstance(inputDataSet).configured().toQueryString();
        runInputPipeline(config);
        final Queue<Record> records = DataCollector.getData();
        assertNotNull(records);
        assertThat(records.size(), is(1));
        records.stream().forEach(leadAsserter);
    }

    @Test
    void getMultipleLeads() {
        setMultipleLeadsDefault();
        inputDataSet.setFields(asList(fields.split(",")));
        final String config = configurationByExample().forInstance(inputDataSet).configured().toQueryString();
        runInputPipeline(config);
        final Queue<Record> records = DataCollector.getData();
        assertNotNull(records);
        assertThat(records.size(), is(greaterThanOrEqualTo(1)));
        records.stream().forEach(leadAsserter);
    }

    @Test
    void getLeadChanges() {
        inputDataSet.setLeadAction(LeadAction.getLeadChanges);
        inputDataSet.setSinceDateTime("2018-01-01 00:00:01 Z");
        inputDataSet.setFields(asList(fields.split(",")));
        final String config = configurationByExample().forInstance(inputDataSet).configured().toQueryString();
        runInputPipeline(config);
        final Queue<Record> records = DataCollector.getData();
        assertNotNull(records);
        assertThat(records.size(), is(greaterThanOrEqualTo(1)));
        records.stream().forEach(record -> {
            assertNotNull(record.getDateTime("activityDate"));
            assertThat(record.getInt("activityTypeId"), is(greaterThanOrEqualTo(0)));
            assertNotNull(record.getString("fields"));
            assertNotNull(record.getString("attributes"));
        });
    }

    @Test
    void getLeadActivities() {
        inputDataSet.setLeadAction(LeadAction.getLeadActivity);
        inputDataSet.setSinceDateTime("2018-01-01 00:00:01 Z");
        SuggestionValues acts = uiActionService.getActivities(inputDataSet.getDataStore());
        List<String> activities = acts.getItems().stream().limit(10).map(item -> String.valueOf(item.getId())).collect(toList());
        inputDataSet.setActivityTypeIds(activities);
        inputDataSet.setFields(asList(fields.split(",")));
        final String config = configurationByExample().forInstance(inputDataSet).configured().toQueryString();
        runInputPipeline(config);
        final Queue<Record> records = DataCollector.getData();
        assertNotNull(records);
        assertThat(records.size(), is(greaterThanOrEqualTo(1)));
        records.stream().forEach(record -> {
            assertNotNull(record.getDateTime("activityDate"));
            assertNotNull(record.getString("marketoGUID"));
            assertThat(record.getInt("activityTypeId"), is(greaterThanOrEqualTo(0)));
            assertThat(record.getInt("id"), is(greaterThanOrEqualTo(0)));
            assertThat(record.getInt("leadId"), is(greaterThanOrEqualTo(0)));
            assertThat(record.getInt("primaryAttributeValueId"), is(greaterThanOrEqualTo(0)));
            assertNotNull(record.getString("attributes"));
        });
    }
}
