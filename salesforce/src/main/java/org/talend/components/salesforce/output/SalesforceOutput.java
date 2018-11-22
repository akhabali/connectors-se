/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package org.talend.components.salesforce.output;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.salesforce.service.Messages;
import org.talend.components.salesforce.service.SalesforceOutputService;
import org.talend.components.salesforce.service.SalesforceService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "SalesforceOutput")
@Processor(name = "SalesforceOutput", family = "Salesforce")
@Documentation("Salesforce output")
public class SalesforceOutput implements Serializable {

    private final OutputConfiguration outputConfig;

    private final SalesforceService service;

    private final LocalConfiguration localConfiguration;

    private SalesforceOutputService outputService;

    private RecordBuilderFactory recordBuilderFactory;

    private Messages messages;

    public SalesforceOutput(@Option("outputConfig") final OutputConfiguration outputConfig, LocalConfiguration localConfiguration,
            final SalesforceService service, final RecordBuilderFactory recordBuilderFactory, final Messages messages) {
        this.outputConfig = outputConfig;
        this.service = service;
        this.localConfiguration = localConfiguration;
        this.messages = messages;
        this.recordBuilderFactory = recordBuilderFactory;
    }

    @PostConstruct
    public void init() {
        try {
            final PartnerConnection connection = service.connect(outputConfig.getDataStore(), localConfiguration);
            outputService = new SalesforceOutputService(outputConfig, connection, recordBuilderFactory, messages);
            Map<String, Field> fieldMap = service.getFieldMap(outputConfig.getDataStore(), outputConfig.getModuleName(),
                    localConfiguration);
            outputService.setFieldMap(fieldMap);
        } catch (ConnectionException e) {
            throw service.handleConnectionException(e);
        }
    }

    @BeforeGroup
    public void beforeGroup() {
    }

    @ElementListener
    public void onNext(@Input final Record record) throws IOException {
        outputService.write(record);
    }

    @AfterGroup
    public void afterGroup() {
        // symmetric method of the beforeGroup() executed after the chunk processing
        // Note: if you don't need it you can delete it
    }

    @PreDestroy
    public void release() throws IOException {
        outputService.finish();
    }
}