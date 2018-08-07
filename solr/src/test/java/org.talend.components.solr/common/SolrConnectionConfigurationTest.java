package org.talend.components.solr.common;


import org.junit.Assert;
import org.junit.Test;

public class SolrConnectionConfigurationTest {

    private SolrConnectionConfiguration solrConnectionConfiguration;
    private SolrDataStore solrDataStore;

    @Test
    public void testNoSlashSuffix() {
        SolrConnectionConfiguration solrConnectionConfiguration =
                prepareSolrConnectionConfiguration("http://localhost:8983/solr", "testCore");
        Assert.assertEquals("http://localhost:8983/solr/testCore", solrConnectionConfiguration.getFullUrl());
    }

    @Test
    public void testSlashSuffix() {
        SolrConnectionConfiguration solrConnectionConfiguration =
                prepareSolrConnectionConfiguration("http://localhost:8983/solr/", "testCore");
        Assert.assertEquals("http://localhost:8983/solr/testCore", solrConnectionConfiguration.getFullUrl());
    }

    @Test
    public void testInverseSlashSuffix() {
        SolrConnectionConfiguration solrConnectionConfiguration =
                prepareSolrConnectionConfiguration("http://localhost:8983/solr\\", "testCore");
        Assert.assertEquals("http://localhost:8983/solr\\testCore", solrConnectionConfiguration.getFullUrl());
    }

    private SolrConnectionConfiguration prepareSolrConnectionConfiguration(String url,  String core) {
        SolrConnectionConfiguration solrConnectionConfiguration = new SolrConnectionConfiguration();
        SolrDataStore solrDataStore = new SolrDataStore();
        solrDataStore.setUrl(url);
        solrConnectionConfiguration.setSolrUrl(solrDataStore);
        solrConnectionConfiguration.setCore(core);
        return solrConnectionConfiguration;
    }

}
