package com.ikwattro.graphaware.demo.engine;


import com.graphaware.tx.executor.batch.IterableInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
import com.graphaware.tx.executor.input.AllNodesWithLabel;
import com.graphaware.writer.neo4j.BatchWriter;
import com.graphaware.writer.neo4j.Neo4jWriter;
import com.ikwattro.graphaware.demo.domain.Labels;
import com.ikwattro.graphaware.demo.domain.Properties;
import com.ikwattro.graphaware.demo.domain.RelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class AnotherBatchEngine {

    private final GraphDatabaseService database;
    private final Neo4jWriter writer;

    @Autowired
    public AnotherBatchEngine(GraphDatabaseService database) {
        this.database = database;
        this.writer = new BatchWriter(database, 50_000, 1000) {
            @Override
            protected boolean offer(RunnableFuture<?> futureTask) {
                try {
                    return queue.offer(futureTask, 1, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    return false;
                }
            }
        };
    }

    public HashMap<String, Integer> doBatchProcess() {
        HashMap<String, Integer> result = new HashMap<>();
        final AtomicInteger processedSoFar = new AtomicInteger(0);

        Node destination = database.findNode(Labels.Destination, Properties.uuid, "destination-1");
        if (null == destination) {
            return result;
        }
        Iterator<Node> sources = database.findNodes(Labels.Source);
        while (sources.hasNext()) {
            Node source = sources.next();
            source.createRelationshipTo(destination, RelationshipTypes.CONNECTS);
            processedSoFar.incrementAndGet();
        }

        result.put("nodes_processed", processedSoFar.get());

        return result;
    }

    public void doBatchProcessWithFrameworkBatcher() {
        writer.start();

        final ExecutorService executor = Executors.newFixedThreadPool(Math.max(1, Runtime.getRuntime().availableProcessors() - 2));

        final Node destination = database.findNode(Labels.Destination, Properties.uuid, "destination-1");
        Long start = System.currentTimeMillis();
        new IterableInputBatchTransactionExecutor<>(database, 1000,
                new AllNodesWithLabel(database, 1000, Labels.User),
                new UnitOfWork<Node>() {
                    @Override
                    public void execute(GraphDatabaseService graphDatabaseService, final Node node, int batchNumber, int stepNumber) {
                        if (stepNumber == 1) {
                            //System.out.println("Processing nodes in batch " + batchNumber);
                        }
                        writer.write(new Runnable() {
                            @Override
                            public void run() {
                                node.createRelationshipTo(destination, RelationshipTypes.CONNECTS);
                            }
                        });

                    }
                }).execute();

        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        writer.stop();

        Long end = System.currentTimeMillis() - start;
        System.out.println("Time taken with SingleThread : " + end);
    }

    public void doUserBatchProcess() {
        writer.start();

        final ExecutorService executor = Executors.newFixedThreadPool(Math.max(1, Runtime.getRuntime().availableProcessors() - 2));

        new IterableInputBatchTransactionExecutor<>(database, 1000,
                new AllNodesWithLabel(database, 1000, Labels.Source),
                new UnitOfWork<Node>() {
                    @Override
                    public void execute(GraphDatabaseService graphDatabaseService, final Node node, int batchNumber, int stepNumber) {
                        executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                process(node);
                            }
                        });
                    }
                }).execute();

        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        writer.stop();
    }

    private void process(Node node) {
        Set<Node> level1 = new HashSet<>();
        Set<Node> level2 = new HashSet<>();

        for (Relationship relationship : node.getRelationships(RelationshipTypes.KNOWS, Direction.OUTGOING)) {
            Node endNode = relationship.getEndNode();
            level1.add(endNode);

            for (Relationship relationship1 : endNode.getRelationships(RelationshipTypes.KNOWS, Direction.OUTGOING)) {
                level2.add(relationship1.getEndNode());
            }
        }

        process(node, level1, 1);
        process(node, level2, 2);
    }

    private void process(final Node node, final Set<Node> targets, final int level) {
        writer.write(new Runnable() {
            @Override
            public void run() {
                for (Node target : targets) {
                    Relationship r = node.createRelationshipTo(target, RelationshipTypes.MIGHT_NOW);
                    r.setProperty("Level", level);
                }
            }
        });
    }
}
