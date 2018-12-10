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
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_INPUT;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LEAD_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LIST_ID;
import static org.talend.components.marketo.MarketoApiConstants.HEADER_CONTENT_TYPE_APPLICATION_JSON;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.slf4j.Logger;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration;
import org.talend.components.marketo.service.ListClient;
import org.talend.components.marketo.service.MarketoService;

import org.talend.sdk.component.api.configuration.Option;

/**
 * Actually, this class is useless as we have remnoved the @Ouput parameters to the map function
 */
public class ListStrategy extends OutputComponentStrategy implements ProcessorStrategy {

    private transient static final Logger LOG = getLogger(ListStrategy.class);

    private final ListClient listClient;

    private transient Integer listId;

    private transient Integer leadId;

    public ListStrategy(@Option("configuration") final MarketoOutputConfiguration configuration, //
            final MarketoService service) {
        super(configuration, service);
        this.listClient = service.getListClient();
        this.listClient.base(this.configuration.getDataSet().getDataStore().getEndpoint());
    }

    @Override
    public JsonObject getPayload(JsonObject incomingData) {
        listId = incomingData.getInt(ATTR_LIST_ID);
        leadId = incomingData.getInt(ATTR_LEAD_ID);
        JsonObject leadRecord = jsonFactory.createObjectBuilder().add(ATTR_ID, leadId).build();
        JsonArray input = jsonFactory.createArrayBuilder().add(leadRecord).build();
        LOG.debug("[getPayload] data: {}; input: {}.", incomingData, input);
        return jsonFactory.createObjectBuilder() //
                .add(ATTR_INPUT, input) //
                .build();
    }

    @Override
    public JsonObject runAction(JsonObject payload) {
        switch (configuration.getListAction()) {
        case addTo:
            return addToList(payload);
        case removeFrom:
            return removeFromList(payload);
        }
        throw new UnsupportedOperationException(i18n.invalidOperation());
    }

    private JsonObject addToList(JsonObject payload) {
        return handleResponse(listClient.addToList(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, listId, payload));
    }

    private JsonObject removeFromList(JsonObject payload) {
        return handleResponse(listClient.removeFromList(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken, listId, payload));
    }

    private JsonObject isMemberOfList(JsonObject payload) {
        return handleResponse(listClient.isMemberOfList(accessToken, listId, String.valueOf(leadId)));
    }

}