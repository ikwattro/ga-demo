package com.ikwattro.graphaware.multithread.worker;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

public class Job implements Callable<Set<Long>> {

    private final GraphDatabaseService database;
    private static final Logger LOG = LoggerFactory.getLogger(Job.class);

    public Job(GraphDatabaseService database){
        this.database = database;
    }

    public Set<Long> call() throws Exception{
        Set<Long> nodeIds = new HashSet<>();


        LOG.warn("Created " + nodeIds.size() + " nodes in thread " + Thread.currentThread());

        return nodeIds;
    }
}
