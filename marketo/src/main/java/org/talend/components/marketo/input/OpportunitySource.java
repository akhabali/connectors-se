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
import static org.talend.components.marketo.MarketoApiConstants.ATTR_DEDUPE_FIELDS;
import static org.talend.components.marketo.MarketoApiConstants.HEADER_CONTENT_TYPE_APPLICATION_JSON;
import static org.talend.components.marketo.MarketoApiConstants.REQUEST_PARAM_QUERY_METHOD_GET;

import javax.json.JsonObject;

import org.slf4j.Logger;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoInputConfiguration;
import org.talend.components.marketo.service.MarketoService;
import org.talend.components.marketo.service.OpportunityClient;

import org.talend.sdk.component.api.configuration.Option;

public class OpportunitySource extends MarketoSource {

    private final OpportunityClient opportunityClient;

    private boolean isOpportunityRole;

    public OpportunitySource(@Option("configuration") final MarketoInputConfiguration configuration, //
            final MarketoService service) {
        super(configuration, service);
        this.opportunityClient = service.getOpportunityClient();
        this.opportunityClient.base(this.configuration.getDataSet().getDataStore().getEndpoint());
        isOpportunityRole = MarketoEntity.OpportunityRole.equals(configuration.getDataSet().getEntity());
    }

    @Override
    public JsonObject runAction() {
        switch (configuration.getOtherAction()) {
        case describe:
            return describeOpportunity();
        case list:
        case get:
            return getOpportunity();
        }

        throw new RuntimeException(i18n.invalidOperation());
    }

    private transient static final Logger LOG = getLogger(OpportunitySource.class);

    private JsonObject getOpportunity() {
        String filterType = configuration.getFilterType();
        String filterValues = configuration.getFilterValues();
        String fields = configuration.getFields() == null ? null : configuration.getFields().stream().collect(joining(","));
        if (isOpportunityRole) {
            if (configuration.getUseCompoundKey()) {
                JsonObject payload = generateCompoundKeyPayload(ATTR_DEDUPE_FIELDS, fields);
                return handleResponse(opportunityClient.getOpportunityRolesWithCompoundKey(HEADER_CONTENT_TYPE_APPLICATION_JSON,
                        REQUEST_PARAM_QUERY_METHOD_GET, accessToken, payload));
            } else {
                return handleResponse(
                        opportunityClient.getOpportunityRoles(accessToken, filterType, filterValues, fields, nextPageToken));
            }
        } else {
            return handleResponse(
                    opportunityClient.getOpportunities(accessToken, filterType, filterValues, fields, nextPageToken));
        }
    }

    private JsonObject describeOpportunity() {
        if (isOpportunityRole) {
            return handleResponse(opportunityClient.describeOpportunityRole(accessToken));
        } else {
            return handleResponse(opportunityClient.describeOpportunity(accessToken));
        }
    }
}
