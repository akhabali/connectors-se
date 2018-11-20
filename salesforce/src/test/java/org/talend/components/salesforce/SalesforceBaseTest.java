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

package org.talend.components.salesforce;

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.ClassRule;
import org.junit.Rule;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;

public class SalesforceBaseTest implements Serializable {

    @ClassRule
    public static final SimpleComponentRule COMPONENT_FACTORY =
            new SimpleComponentRule("org.talend.components.salesforce");

    public static String USER_ID;

    public static String PASSWORD;

    public static String SECURITY_KEY;

    static {
        final MavenDecrypter decrypter = new MavenDecrypter();
        final Server serverWithPassword = decrypter.find("salesforce-password");
        final Server serverWithSecuritykey = decrypter.find("salesforce-securitykey");
        USER_ID = serverWithPassword.getUsername();
        PASSWORD = serverWithPassword.getPassword();
        SECURITY_KEY = serverWithSecuritykey.getPassword();
    }

    @Rule
    public transient final TestPipeline pipeline = TestPipeline.create();

    @Service
    public final RecordBuilderFactory factory = COMPONENT_FACTORY.findService(RecordBuilderFactory.class);

    public final BasicDataStore dataStore = new BasicDataStore();

    public static String createNewRandom() {
        return Integer.toString(ThreadLocalRandom.current().nextInt(1, 1000000));
    }

}