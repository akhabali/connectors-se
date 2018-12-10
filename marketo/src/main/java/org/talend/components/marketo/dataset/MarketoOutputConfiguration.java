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
package org.talend.components.marketo.dataset;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@GridLayout({ //
        @GridLayout.Row({ "dataSet" }), //
        @GridLayout.Row({ "action" }), //
        @GridLayout.Row({ "listAction" }), //
        @GridLayout.Row({ "syncMethod" }), //
        @GridLayout.Row({ "lookupField" }), //
        @GridLayout.Row({ "dedupeBy" }), //
        @GridLayout.Row({ "deleteBy" }), //
        @GridLayout.Row({ "customObjectName" }), //
}) //
@Documentation("Marketo Sink Configuration")
@ToString(callSuper = true)
public class MarketoOutputConfiguration implements Serializable {

    public static final String NAME = "MarketoOutputConfiguration";

    public enum OutputAction {
        sync,
        delete
    }

    public enum ListAction {
        addTo,
        removeFrom
    }

    public enum SyncMethod {
        createOnly,
        updateOnly,
        createOrUpdate,
        createDuplicate
    }

    public enum DeleteBy {
        dedupeFields,
        idField
    }

    /*
     * DataSet
     */
    @Option
    @Required
    @Documentation("Marketo DataSet")
    private MarketoDataSet dataSet;

    @Option
    // @ActiveIf(target = "../dataSet/entity", value = { "Lead", "CustomObject", "Company", "Opportunity", "OpportunityRole" })
    @Documentation("Action")
    private OutputAction action;

    /*
     * Lead Entity
     */
    @Option
    @ActiveIf(target = "../dataSet/entity", value = "Lead")
    @ActiveIf(target = "action", value = "sync")
    // @Suggestable(value = FIELD_NAMES, parameters = { "../dataSet/dataStore", "../dataSet/entity", "customObjectName" })
    @Documentation("Lookup Field")
    private String lookupField;

    /*
     * CustomObject Entity
     */
    @Option
    @ActiveIf(target = "../dataSet/entity", value = { "CustomObject" })
    // @Suggestable(value = CUSTOM_OBJECT_NAMES, parameters = { "../dataSet/dataStore" })
    @Documentation("Custom Object Name")
    private String customObjectName;

    /*
     * List Entity
     */
    @Option
    @ActiveIf(target = "../dataSet/entity", value = { "List" })
    @Documentation("List Action")
    private ListAction listAction;

    /*
     * All entities
     */
    @Option
    @ActiveIf(target = "../dataSet/entity", value = { "Lead", "CustomObject", "Company", "Opportunity", "OpportunityRole" })
    @ActiveIf(target = "action", value = { "sync" })
    @Documentation("Synchronization method")
    private SyncMethod syncMethod;

    @Option
    @ActiveIf(target = "action", value = { "sync" })
    @ActiveIf(target = "syncMethod", value = { "updateOnly" })
    @Documentation("Dedupe by")
    private String dedupeBy;

    @Option
    @ActiveIf(target = "../dataSet/entity", value = { "CustomObject", "Company", "Opportunity", "OpportunityRole" })
    @ActiveIf(target = "action", value = { "delete" })
    @Documentation("Field to delete company records by. Key may be dedupeFields or idField")
    private DeleteBy deleteBy;

}
