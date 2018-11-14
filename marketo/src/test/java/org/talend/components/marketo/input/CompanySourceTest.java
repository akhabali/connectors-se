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
import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.DataCollector;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoInputDataSet.OtherEntityAction;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.input.Mapper;

@HttpApi(useSsl = true, responseLocator = org.talend.sdk.component.junit.http.internal.impl.MarketoResponseLocator.class)
@WithComponents("org.talend.components.marketo")
class CompanySourceTest extends SourceBaseTest {

    CompanySource source;

    final String fields = "createdAt,externalCompanyId,id,updatedAt,annualRevenue,billingCity,billingCountry,"
            + "billingPostalCode,billingState,billingStreet,company,companyNotes,externalSalesPersonId,industry,"
            + "mainPhone,numberOfEmployees,sicCode,site,website";

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();
        inputDataSet.setEntity(MarketoEntity.Company);
    }

    @Test
    void testDescribeCompanies() {
        inputDataSet.setOtherAction(OtherEntityAction.describe);
        source = new CompanySource(inputDataSet, service, tools);
        source.init();
        result = source.next();
        assertNotNull(result);
        result = source.next();
        assertNull(result);
    }

    @Test
    void testGetCompanies() {
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setFilterType("externalCompanyId");
        inputDataSet.setFilterValues("google01,google02,google03,google04,google05,google06");
        inputDataSet.setFields(asList("mainPhone", "company", "website"));
        inputDataSet.setBatchSize(10);
        source = new CompanySource(inputDataSet, service, tools);
        source.init();
        while ((result = source.next()) != null) {
            assertNotNull(result);
            Assert.assertThat(result.getString("externalCompanyId"), CoreMatchers.containsString("google0"));
        }
    }

    @Test
    void testGetCompaniesFails() {
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setFilterType("billingCountry");
        inputDataSet.setFilterValues("France");
        inputDataSet.setFields(asList("mainPhone", "company", "website"));
        inputDataSet.setBatchSize(10);
        source = new CompanySource(inputDataSet, service, tools);
        try {
            source.init();
        } catch (RuntimeException e) {
            assertEquals("[1003] Invalid filterType 'billingCountry'", e.getMessage());
        }
    }

    @Test
    public void testDescribeCompaniesWithCreateMapper() {
        inputDataSet.setOtherAction(OtherEntityAction.describe);
        final Mapper mapper = component.createMapper(MarketoInputMapper.class, inputDataSet);
        List<Record> res = component.collectAsList(Record.class, mapper);
        assertNotNull(res);
        assertEquals(1, res.size());
    }

    @Test
    void testGetCompaniesWithCreateMapper() {
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setFilterType("externalCompanyId");
        inputDataSet.setFilterValues("google01,google02,google03,google04,google05,google06");
        inputDataSet.setFields(asList(fields.split(",")));
        final Mapper mapper = component.createMapper(MarketoInputMapper.class, inputDataSet);
        List<Record> res = component.collectAsList(Record.class, mapper);
        assertEquals(4, res.size());
        Record record = res.get(0);
        assertThat(record.getString("externalCompanyId"), CoreMatchers.containsString("google0"));
        assertNull(record.getString("industry"));
    }

    @Test
    void testGetErrors() {
        inputDataSet.setOtherAction(OtherEntityAction.list);
        inputDataSet.setFilterType("");
        inputDataSet.setFilterValues("google01,google02,google03,google04,google05,google06");
        inputDataSet.setFields(asList(fields.split(".")));
        source = new CompanySource(inputDataSet, service, tools);
        try {
            source.init();
        } catch (RuntimeException e) {
            assertEquals("[1003] filterType not specified", e.getMessage());
        }
    }

    @Test
    void testInvalidAccessToken() {
        inputDataSet.getDataStore().setEndpoint(MARKETO_ENDPOINT + "/bzh");
        source = new CompanySource(inputDataSet, service, tools);
        try {
            source.init();
            fail("Should have a 403 error. Should not be here");
        } catch (RuntimeException e) {
            System.err.println("[testInvalidAccessToken] {}" + e);
        }
    }

    @Test
    void getCompanies() {
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setFilterType("externalCompanyId");
        inputDataSet.setFilterValues("google01,google02,google03,google04,google05,google06");
        inputDataSet.setFields(asList("mainPhone", "company", "website", "createdAt", "updatedAt"));
        inputDataSet.setBatchSize(10);
        final String config = configurationByExample().forInstance(inputDataSet).configured().toQueryString();
        LOG.debug("[getCompanies] config: {}", config);
        runInputPipeline(config);
        final Queue<Record> records = DataCollector.getData();
        assertNotNull(records);
        assertEquals(4, records.size());
        Consumer<Record> recordConsumer = record -> {
            assertThat(record.getInt("id"), is(both(greaterThanOrEqualTo(52)).and(lessThanOrEqualTo(56))));
            assertThat(record.getString("company"), is("Google"));
            assertThat(record.getString("externalCompanyId"), CoreMatchers.containsString("google0"));
            assertNotNull(record.getDateTime("createdAt"));
            assertNotNull(record.getDateTime("updatedAt"));
        };
        records.stream().forEach(recordConsumer);
    }

    @Ignore
    @Test
    public void testProduce() {
        Record r1 = service.getRecordBuilder().newRecordBuilder().withInt("seq", 0).withInt("id", 53)
                .withString("website", "g.plus").withString("externalCompanyId", "google02")
                .withString("mainPhone", "33688828052").withString("company", "Google").build();
        Record r2 = service.getRecordBuilder().newRecordBuilder().withInt("seq", 1).withInt("id", 54)
                .withString("website", "g.plus").withString("externalCompanyId", "google03")
                .withString("mainPhone", "33688828052").withString("company", "Google").build();
        Record r3 = service.getRecordBuilder().newRecordBuilder().withFloat("seq", 2).withFloat("id", 55)
                .withString("website", "g.plus").withString("externalCompanyId", "google04")
                .withString("mainPhone", "33688828052").withString("company", "Google").build();
        Record r4 = service.getRecordBuilder().newRecordBuilder().withFloat("seq", 3).withFloat("id", 56)
                .withString("website", "g.plus").withString("externalCompanyId", "google05")
                .withString("mainPhone", "33688828052").withString("company", "Google").build();

        // Setup your component configuration for the test here
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setFilterType("externalCompanyId");
        inputDataSet.setFilterValues("google01,google02,google03,google04,google05,google06");
        inputDataSet.setFields(asList("mainPhone", "company", "website"));
        inputDataSet.setBatchSize(10);
        //

    }
}
