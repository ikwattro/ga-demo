package com.ikwattro.graphaware.demo.engine;

import com.ikwattro.graphaware.demo.domain.Labels;
import com.ikwattro.graphaware.demo.domain.Properties;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

import static com.graphaware.common.util.DatabaseUtils.registerShutdownHook;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchEngineTest{

    private BatchEngine engine;
    private final int SOURCES = 50000;

    private GraphDatabaseService database;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        registerShutdownHook(database);
        engine = new BatchEngine(database);
        try (Transaction tx = database.beginTx()) {
            Node destination = database.createNode(Labels.Destination);
            destination.setProperty(Properties.uuid, "destination-1");
            int i = 0;
            while (i < SOURCES) {
                Node s = database.createNode(Labels.Source);
                s.setProperty(Properties.uuid, "source-" + i);
                i++;
            }

            tx.success();
        }
    }

    @After
    public void tearDown() {
        database.shutdown();
    }

    @Test
    public void testSetupIsOk(){
        AtomicInteger i = new AtomicInteger(0);
        try (Transaction tx = database.beginTx()) {
            Iterator<Node> nodes = database.findNodes(Labels.Source);
            while (nodes.hasNext()) {
                nodes.next();
                i.incrementAndGet();
            }
            tx.success();
        }
        assertEquals(SOURCES, i.get());
    }

    @Test
    public void testRelationshipsAreCreated(){
        try (Transaction tx = database.beginTx()){
            engine.doBatchProcess();
            Node destination = database.findNode(Labels.Destination, Properties.uuid, "destination-1");
            assertEquals(SOURCES, destination.getDegree());

            tx.success();
        }
    }

    @Test
    public void testRelationshipsAreCreatedInBatch(){
        try (Transaction tx = database.beginTx()){
            engine.doBatchProcessWithFrameworkBatcher();
            Node destination = database.findNode(Labels.Destination, Properties.uuid, "destination-1");
            assertEquals(SOURCES, destination.getDegree());

            tx.success();
        }
    }
}
