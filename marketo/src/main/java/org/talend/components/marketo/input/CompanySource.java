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

import static java.util.stream.Collectors.joining;
import static org.slf4j.LoggerFactory.getLogger;

import javax.json.JsonObject;

import org.slf4j.Logger;
import org.talend.components.marketo.dataset.MarketoInputConfiguration;
import org.talend.components.marketo.service.CompanyClient;
import org.talend.components.marketo.service.MarketoService;

import org.talend.sdk.component.api.configuration.Option;

public class CompanySource extends MarketoSource {

    private final CompanyClient companyClient;

    public CompanySource(@Option("configuration") MarketoInputConfiguration dataSet, //
            final MarketoService service) {
        super(dataSet, service);
        this.companyClient = service.getCompanyClient();
        this.companyClient.base(this.configuration.getDataSet().getDataStore().getEndpoint());
    }

    private transient static final Logger LOG = getLogger(CompanySource.class);

    @Override
    public JsonObject runAction() {
        switch (configuration.getOtherAction()) {
        case describe:
            return describeCompany();
        case list:
        case get:
            return getCompanies();
        }

        throw new RuntimeException(i18n.invalidOperation());
    }

    private JsonObject describeCompany() {
        return handleResponse(companyClient.describeCompanies(accessToken));
    }

    private JsonObject getCompanies() {
        String filterType = configuration.getFilterType();
        String filterValues = configuration.getFilterValues();
        String fields = configuration.getFields() == null ? null : configuration.getFields().stream().collect(joining(","));
        return handleResponse(companyClient.getCompanies(accessToken, filterType, filterValues, fields, nextPageToken));
    }

}
