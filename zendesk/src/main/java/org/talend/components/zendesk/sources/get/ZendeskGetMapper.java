package org.talend.components.zendesk.sources.get;

import org.talend.components.zendesk.helpers.ConfigurationHelper;
import org.talend.components.zendesk.service.http.ZendeskAuthHttpClientService;
import org.talend.components.zendesk.service.http.ZendeskHttpClientService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

import static java.util.Collections.singletonList;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "zendesk_get")
@PartitionMapper(name = "Get")
@Documentation("Input mapper class")
public class ZendeskGetMapper implements Serializable {

    private final ZendeskGetConfiguration configuration;

    private final ZendeskHttpClientService zendeskHttpClientService;

    public ZendeskGetMapper(@Option("configuration") final ZendeskGetConfiguration configuration,
            final ZendeskHttpClientService zendeskHttpClientService,
            final ZendeskAuthHttpClientService zendeskAuthHttpClientService) {
        this.configuration = configuration;
        this.zendeskHttpClientService = zendeskHttpClientService;
        ConfigurationHelper.setupServices();
    }

    @Assessor
    public long estimateSize() {
        return 1L;
    }

    @Split
    public List<ZendeskGetMapper> split(@PartitionSize final long bundles) {
        return singletonList(this);
    }

    @Emitter(name = "Input")
    public ZendeskGetSource createWorker() {
        return new ZendeskGetSource(configuration, zendeskHttpClientService);
    }
}