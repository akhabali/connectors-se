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
package org.talend.components.marketo.output;

import static org.slf4j.LoggerFactory.getLogger;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ACTION;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_DEDUPE_BY;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_DELETE_BY;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_INPUT;
import static org.talend.components.marketo.MarketoApiConstants.HEADER_CONTENT_TYPE_APPLICATION_JSON;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.slf4j.Logger;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoOutputDataSet;
import org.talend.components.marketo.dataset.MarketoOutputDataSet.OutputAction;
import org.talend.components.marketo.service.MarketoService;
import org.talend.components.marketo.service.OpportunityClient;

import org.talend.sdk.component.api.configuration.Option;

public class OpportunityStrategy extends OutputComponentStrategy {

    private OpportunityClient opportunityClient;

    private boolean isOpportunityRole;

    private transient static final Logger LOG = getLogger(OpportunityStrategy.class);

    public OpportunityStrategy(@Option("configuration") final MarketoOutputDataSet dataSet, //
            final MarketoService service) {
        super(dataSet, service);
        this.opportunityClient = service.getOpportunityClient();
        this.opportunityClient.base(dataSet.getDataStore().getEndpoint());
        isOpportunityRole = MarketoEntity.OpportunityRole.equals(dataSet.getEntity());
    }

    @Override
    public JsonObject getPayload(JsonObject incomingData) {
        JsonObject data = incomingData;
        JsonArray input = jsonFactory.createArrayBuilder().add(data).build();
        JsonObjectBuilder builder = jsonFactory.createObjectBuilder();
        if (OutputAction.sync.equals(dataSet.getAction())) {
            builder.add(ATTR_ACTION, dataSet.getSyncMethod().name());
            if (dataSet.getDedupeBy() != null) {
                builder.add(ATTR_DEDUPE_BY, dataSet.getDedupeBy());
            }
        } else {
            builder.add(ATTR_DELETE_BY, dataSet.getDeleteBy().name());
        }
        builder.add(ATTR_INPUT, input);

        return builder.build();
    }

    @Override
    public JsonObject runAction(JsonObject payload) {
        LOG.debug("[runAction] payload: {}.", payload);
        switch (dataSet.getAction()) {
        case sync:
            return syncOpportunity(payload);
        case delete:
            return deleteOpportunity(payload);
        }
        throw new UnsupportedOperationException(i18n.invalidOperation());
    }

    private JsonObject syncOpportunity(JsonObject payload) {
        if (isOpportunityRole) {
            return handleResponse(
                    opportunityClient.syncOpportunityRoles(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload));
        } else {
            return handleResponse(
                    opportunityClient.syncOpportunities(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload));
        }
    }

    private JsonObject deleteOpportunity(JsonObject payload) {
        if (isOpportunityRole) {
            return handleResponse(
                    opportunityClient.deleteOpportunityRoles(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload));
        } else {
            return handleResponse(
                    opportunityClient.deleteOpportunities(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, payload));
        }
    }

}
