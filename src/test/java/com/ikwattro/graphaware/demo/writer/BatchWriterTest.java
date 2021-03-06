package com.ikwattro.graphaware.demo.writer;

import com.graphaware.test.integration.DatabaseIntegrationTest;
import com.graphaware.writer.BatchWriter;
import com.graphaware.writer.DatabaseWriter;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;

import java.util.Iterator;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;


public class BatchWriterTest extends DatabaseIntegrationTest{

    private DatabaseWriter writer;

    @Before
    public void setUp() throws Exception{
        super.setUp();
        this.writer = new BatchWriter(getDatabase(), 100, 10){
            @Override
            protected boolean offer(RunnableFuture<?> futureTask){
                try {
                    return queue.offer(futureTask, 1, TimeUnit.MINUTES);
                } catch (InterruptedException e){
                    return false;
                }
            }
        };
    }

    @Test
    public void testWriterCanRunInSequence(){
        process();
        process();
        int i = 0;
        try (Transaction tx = getDatabase().beginTx()){
            Iterator<Node> it = getDatabase().getAllNodes().iterator();
            while (it.hasNext()){
                ++i;
                it.next();
            }

            tx.success();
        }

        assertEquals(200, i);
    }

    private void process(){
        writer.start();
        writer.write(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 100; ++i){
                    getDatabase().createNode();
                }
            }
        });

        writer.stop();
    }
}
