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

import static org.talend.components.marketo.MarketoApiConstants.ATTR_REASONS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_RESULT;

import javax.annotation.PostConstruct;
import javax.json.JsonObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.marketo.MarketoSourceOrProcessor;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration;
import org.talend.components.marketo.service.MarketoService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;

@Version
@Processor(family = "Marketo", name = "Output")
@Icon(value = Icon.IconType.CUSTOM, custom = "MarketoOutput")
@Documentation("Marketo Output Component")
public class MarketoProcessor extends MarketoSourceOrProcessor {

    protected final MarketoOutputConfiguration dataSet;

    private ProcessorStrategy strategy;

    private transient static final Logger LOG = LoggerFactory.getLogger(MarketoProcessor.class);

    public MarketoProcessor(@Option("configuration") final MarketoOutputConfiguration configuration, //
            final MarketoService service) {
        super(configuration.getDataSet(), service);
        this.dataSet = configuration;

        switch (configuration.getDataSet().getEntity()) {
        case Lead:
            strategy = new LeadStrategy(configuration, service);
            break;
        case List:
            strategy = new ListStrategy(configuration, service);
            break;
        case CustomObject:
            strategy = new CustomObjectStrategy(configuration, service);
            break;
        case Company:
            strategy = new CompanyStrategy(configuration, service);
            break;
        case Opportunity:
        case OpportunityRole:
            strategy = new OpportunityStrategy(configuration, service);
            break;
        }
    }

    @PostConstruct
    @Override
    public void init() {
        strategy.init();
    }

    @ElementListener
    // public void map(final JsonObject data, @Output final OutputEmitter<JsonObject> main, @Output("rejected") final
    // OutputEmitter<JsonObject> rejected) {
    public void map(@Input final Record incomingData) {
        JsonObject data = marketoService.toJson(incomingData);
        LOG.debug("[map] received: {}.", data);
        JsonObject payload = strategy.getPayload(data);
        LOG.debug("[map] payload : {}.", payload);
        JsonObject result = strategy.runAction(payload);
        LOG.debug("[map] result  : {}.", result);
        for (JsonObject status : result.getJsonArray(ATTR_RESULT).getValuesAs(JsonObject.class)) {
            if (strategy.isRejected(status)) {
                // rejected.emit(strategy.createRejectData(status));
                throw new RuntimeException(getErrors(status.getJsonArray(ATTR_REASONS)));
            } else {
                // main.emit(strategy.createMainData(status));
            }
        }
    }

}
