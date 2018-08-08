package org.talend.components.solr.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class SolrConnectionConfigurationTest {

    @Test
    public void testNoSlashSuffix() {
        SolrConnectionConfiguration solrConnectionConfiguration = prepareSolrConnectionConfiguration("http://localhost:8983/solr",
                "testCore");
        assertEquals("http://localhost:8983/solr/testCore", solrConnectionConfiguration.getFullUrl());
    }

    @Test
    public void testSlashSuffix() {
        SolrConnectionConfiguration solrConnectionConfiguration = prepareSolrConnectionConfiguration(
                "http://localhost:8983/solr/", "testCore");
        assertEquals("http://localhost:8983/solr/testCore", solrConnectionConfiguration.getFullUrl());
    }

    @Test
    public void testInverseSlashSuffix() {
        SolrConnectionConfiguration solrConnectionConfiguration = prepareSolrConnectionConfiguration(
                "http://localhost:8983/solr\\", "testCore");
        assertEquals("http://localhost:8983/solr\\testCore", solrConnectionConfiguration.getFullUrl());
    }

    private SolrConnectionConfiguration prepareSolrConnectionConfiguration(String url, String core) {
        SolrConnectionConfiguration solrConnectionConfiguration = new SolrConnectionConfiguration();
        SolrDataStore solrDataStore = new SolrDataStore();
        solrDataStore.setUrl(url);
        solrConnectionConfiguration.setSolrUrl(solrDataStore);
        solrConnectionConfiguration.setCore(core);
        return solrConnectionConfiguration;
    }

}
