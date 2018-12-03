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

import static org.talend.components.marketo.MarketoApiConstants.ATTR_ACTION;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_DEDUPE_BY;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_DELETE_BY;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_INPUT;
import static org.talend.components.marketo.MarketoApiConstants.HEADER_CONTENT_TYPE_APPLICATION_JSON;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.talend.components.marketo.dataset.MarketoOutputConfiguration;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration.OutputAction;
import org.talend.components.marketo.service.CustomObjectClient;
import org.talend.components.marketo.service.MarketoService;
import org.talend.sdk.component.api.configuration.Option;

public class CustomObjectStrategy extends OutputComponentStrategy {

    private CustomObjectClient customObjectClient;

    private String customObjectName;

    public CustomObjectStrategy(@Option("configuration") final MarketoOutputConfiguration configuration, //
            final MarketoService service) {
        super(configuration, service);
        this.customObjectClient = service.getCustomObjectClient();
        this.customObjectClient.base(configuration.getDataSet().getDataStore().getEndpoint());
    }

    @Override
    public JsonObject runAction(JsonObject payload) {
        customObjectName = configuration.getCustomObjectName();
        switch (configuration.getAction()) {
        case sync:
            return syncCustomObject(payload);
        case delete:
            return deleteCustomObject(payload);
        }
        throw new UnsupportedOperationException(i18n.invalidOperation());
    }

    @Override
    public JsonObject getPayload(JsonObject incomingData) {
        JsonObject data = incomingData;
        JsonArray input = jsonFactory.createArrayBuilder().add(data).build();
        JsonObjectBuilder builder = jsonFactory.createObjectBuilder();
        if (OutputAction.sync.equals(configuration.getAction())) {
            builder.add(ATTR_ACTION, configuration.getSyncMethod().name());
            if (configuration.getDedupeBy() != null) {
                builder.add(ATTR_DEDUPE_BY, configuration.getDedupeBy());
            }
        } else {
            builder.add(ATTR_DELETE_BY, configuration.getDeleteBy().name());
        }
        builder.add(ATTR_INPUT, input);

        return builder.build();
    }

    private JsonObject syncCustomObject(JsonObject payload) {
        return handleResponse(customObjectClient.syncCustomObjects(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken,
                customObjectName, payload));
    }

    private JsonObject deleteCustomObject(JsonObject payload) {
        return handleResponse(customObjectClient.deleteCustomObjects(HEADER_CONTENT_TYPE_APPLICATION_JSON, accessToken,
                customObjectName, payload));
    }

}
