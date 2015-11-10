package com.ikwattro.graphaware.demo.api;


import com.graphaware.test.integration.GraphAwareApiTest;
import com.ikwattro.graphaware.demo.domain.Labels;
import com.ikwattro.graphaware.demo.domain.Properties;
import com.ikwattro.graphaware.demo.engine.BatchEngine;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static com.graphaware.common.util.DatabaseUtils.registerShutdownHook;
import static org.junit.Assert.assertEquals;

public class DemoControllerTest extends GraphAwareApiTest{

    private final int SOURCES = 50000;

    @Test
    public void testRelationshipsAreCreatedThroughApiCall(){

        try (Transaction tx = getDatabase().beginTx()) {
            Node destination = getDatabase().createNode(Labels.Destination);
            destination.setProperty(Properties.uuid, "destination-1");
            int i = 0;
            while (i < SOURCES) {
                Node s = getDatabase().createNode(Labels.Source);
                s.setProperty(Properties.uuid, "source-" + i);
                i++;
            }

            tx.success();
        }

        httpClient.post(getUrl(), HttpStatus.SC_OK);
        try (Transaction tx = getDatabase().beginTx()){
            Node destination = getDatabase().findNode(Labels.Destination, Properties.uuid, "destination-1");
            assertEquals(SOURCES, destination.getDegree());

            tx.success();
        }

    }

    private String getUrl() {
        return baseUrl() + "/batch";
    }
}
