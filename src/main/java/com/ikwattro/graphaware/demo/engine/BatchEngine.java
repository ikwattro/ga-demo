package com.ikwattro.graphaware.demo.engine;


import com.graphaware.tx.executor.batch.BatchTransactionExecutor;
import com.graphaware.tx.executor.batch.IterableInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.MultiThreadedBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
import com.graphaware.tx.executor.input.AllNodesWithLabel;
import com.ikwattro.graphaware.demo.domain.Labels;
import com.ikwattro.graphaware.demo.domain.Properties;
import com.ikwattro.graphaware.demo.domain.RelationshipTypes;
import org.neo4j.graphdb.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class BatchEngine {

    private final GraphDatabaseService database;

    @Autowired
    public BatchEngine(GraphDatabaseService database) {
        this.database = database;
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

    public void doBatchProcessWithFrameworkBatcher(){
        final Node destination = database.findNode(Labels.Destination, Properties.uuid, "destination-1");
        Long start = System.currentTimeMillis();
        new IterableInputBatchTransactionExecutor<>(database, 1000,
                new AllNodesWithLabel(database, 1000, Labels.Source),
                new UnitOfWork<Node>() {
                    @Override
                    public void execute(GraphDatabaseService graphDatabaseService, Node node, int batchNumber, int stepNumber) {
                        if (stepNumber == 1) {
                            //System.out.println("Processing nodes in batch " + batchNumber);
                        }

                        node.createRelationshipTo(destination, RelationshipTypes.CONNECTS);
                    }
                }).execute();
        Long end = System.currentTimeMillis() - start;
        System.out.println("Time taken with SingleThread : " + end);
    }

    public void doUserBatchProcess(){
        new IterableInputBatchTransactionExecutor<>(database, 1000,
                new AllNodesWithLabel(database, 1000, Labels.User),
                new UnitOfWork<Node>() {
                    @Override
                    public void execute(GraphDatabaseService graphDatabaseService, Node node, int batchNumber, int stepNumber) {
                        if (stepNumber == 1) {
                            //System.out.println("Processing nodes in batch " + batchNumber);
                        }

                        process(node);
                    }
                }).execute();
    }

    private void process(Node node) {

        int level = 1;
        for (Relationship relationship : node.getRelationships(RelationshipTypes.KNOWS, Direction.OUTGOING)){
            Node o = relationship.getOtherNode(node);
            Relationship r = node.createRelationshipTo(o, RelationshipTypes.MIGHT_NOW);
            r.setProperty("Level", level);

            for (Relationship relationship1 : o.getRelationships(RelationshipTypes.KNOWS, Direction.OUTGOING)){
                Node o2 = relationship1.getOtherNode(o);
                Relationship r2 = node.createRelationshipTo(o2, RelationshipTypes.MIGHT_NOW);
                r2.setProperty("Level", level + 1);
            }
        }
    }
}
