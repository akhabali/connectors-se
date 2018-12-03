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
import org.talend.components.marketo.service.ListClient;
import org.talend.components.marketo.service.MarketoService;

import org.talend.sdk.component.api.configuration.Option;

public class ListSource extends MarketoSource {

    private final ListClient listClient;

    private transient static final Logger LOG = getLogger(ListClient.class);

    public ListSource(@Option("configuration") final MarketoInputConfiguration configuration, //
            final MarketoService service) {
        super(configuration, service);
        this.listClient = service.getListClient();
        this.listClient.base(this.configuration.getDataSet().getDataStore().getEndpoint());
    }

    @Override
    public JsonObject runAction() {
        switch (configuration.getListAction()) {
        case list:
            return getLists();
        case get:
            return getListById();
        case isMemberOf:
            return isMemberOfList();
        case getLeads:
            return getLeadsByListId();
        }

        throw new RuntimeException(i18n.invalidOperation());
    }

    private JsonObject getLeadsByListId() {
        Integer listId = Integer.parseInt(configuration.getListName());
        String fields = configuration.getFields() == null ? null : configuration.getFields().stream().collect(joining(","));
        return handleResponse(listClient.getLeadsByListId(accessToken, nextPageToken, listId, fields));
    }

    private JsonObject isMemberOfList() {
        Integer listId = Integer.parseInt(configuration.getListName());
        String leadIds = configuration.getLeadIds();
        return handleResponse(listClient.isMemberOfList(accessToken, listId, leadIds));
    }

    private JsonObject getListById() {
        Integer listId = configuration.getListId();
        return handleResponse(listClient.getListbyId(accessToken, listId));
    }

    private JsonObject getLists() {
        Integer id = null;
        String name = "";
        String workspaceName = configuration.getWorkspaceName();
        String programName = configuration.getProgramName();
        return handleResponse(listClient.getLists(accessToken, nextPageToken, id, name, programName, workspaceName));
    }
}
