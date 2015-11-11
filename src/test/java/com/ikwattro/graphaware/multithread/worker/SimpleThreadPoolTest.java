package com.ikwattro.graphaware.multithread.worker;


import com.graphaware.test.integration.DatabaseIntegrationTest;
import com.graphaware.tx.executor.input.AllNodesWithLabel;
import com.ikwattro.graphaware.demo.domain.Labels;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SimpleThreadPoolTest extends DatabaseIntegrationTest{

    private SimpleThreadPool simpleThreadPool;

    @Before
    public void setUp() throws Exception{
        super.setUp();
        this.simpleThreadPool = new SimpleThreadPool(getDatabase());
    }

    @Test
    public void testSimpleCallable(){
        List<Integer> result = simpleThreadPool.aTest();
        final AtomicInteger i = new AtomicInteger(0);
        try (Transaction tx = getDatabase().beginTx()){
            Iterator<Node> nodes = getDatabase().getAllNodes().iterator();
            while (nodes.hasNext()){
                nodes.next();
                i.incrementAndGet();
            }

            tx.success();
        }

        assertEquals(500, i.get());
        assertEquals(5, result.size());
    }

    @Test
    public void testAllNodesIterable(){
        try (Transaction tx = getDatabase().beginTx()){
            for (int i = 0; i < 100; i++){
                getDatabase().createNode(Labels.User);
            }

            Iterator<Node> it = new AllNodesWithLabel(getDatabase(), 10, Labels.User).iterator();
            while (it.hasNext()) {
                Node o = it.next();
                System.out.println(o.getId());
            }

            tx.success();
        }
    }

    @Test
    public void testDifferenceIterators(){
        try (Transaction tx = getDatabase().beginTx()){
            for (int i = 0; i < 10000; ++i){
                getDatabase().createNode(DynamicLabel.label("Difference"));
            }

            tx.success();
        }
        Long time1 = null;
        Long time2 = null;

        try (Transaction tx = getDatabase().beginTx()){
            Long start = System.currentTimeMillis();
            ResourceIterator<Node> it = getDatabase().findNodes(DynamicLabel.label("Difference"));
            while (it.hasNext()){
                it.next();
            }
            time1 = System.currentTimeMillis() - start;
            tx.success();
        }

        try (Transaction tx = getDatabase().beginTx()){
            Long start = System.currentTimeMillis();
            Iterator<Node> it2 = new AllNodesWithLabel(getDatabase(), 1000, DynamicLabel.label("Difference"));
            while (it2.hasNext()){
                it2.next();
            }
            time2 = System.currentTimeMillis() - start;
            tx.success();
        }

        assertTrue(time2 < time1);

    }

    @Test
    public void testNodesAreCreated(){
        simpleThreadPool.test();
        final AtomicInteger i = new AtomicInteger(0);
        try (Transaction tx = getDatabase().beginTx()){
            Iterator<Node> nodes = getDatabase().getAllNodes().iterator();
            while (nodes.hasNext()){
                nodes.next();
                i.incrementAndGet();
            }

            tx.success();
        }

        assertEquals(500, i.get());
    }
}
