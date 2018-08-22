package org.talend.components.solr.common;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@DataStore("SolrDataStore")
@Checkable("checkSolrConnection")
@GridLayout({ @GridLayout.Row({ "url" }), @GridLayout.Row({ "login" }), @GridLayout.Row({ "password" }) })
public class SolrDataStore {

    @Option
    @Required
    @Pattern("^(http://|https://).*")
    @Documentation("Solr server URL")
    private String url;

    @Option
    @Documentation("Login field")
    private String login;

    @Option
    @Credential
    @Documentation("Password field")
    private String password;

}
