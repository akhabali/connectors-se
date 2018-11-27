package org.talend.components.salesforce;

import static org.talend.components.salesforce.service.SalesforceService.URL;

import java.io.Serializable;

import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.talend.components.salesforce.dataset.QueryDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;

public class SalesforceBaseTest implements Serializable {

    @ClassRule
    public static final SimpleComponentRule COMPONENT_FACTORY = new SimpleComponentRule("org.talend.components.salesforce");

    public static String USER_ID;

    public static String PASSWORD;

    public static String SECURITY_KEY;

    @Rule
    public transient final TestPipeline pipeline = TestPipeline.create();

    protected BasicDataStore dataStore;

    static {
        final MavenDecrypter decrypter = new MavenDecrypter();
        final Server serverWithPassword = decrypter.find("salesforce-password");
        final Server serverWithSecuritykey = decrypter.find("salesforce-securitykey");
        USER_ID = serverWithPassword.getUsername();
        PASSWORD = serverWithPassword.getPassword();
        SECURITY_KEY = serverWithSecuritykey.getPassword();
    }

    @Before
    public void setUp() {
        dataStore = new BasicDataStore();
        dataStore.setEndpoint(URL);
        dataStore.setUserId(USER_ID);
        dataStore.setPassword(PASSWORD);
        dataStore.setSecurityKey(SECURITY_KEY);
    }

}