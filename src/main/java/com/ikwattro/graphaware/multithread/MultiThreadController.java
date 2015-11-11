package com.ikwattro.graphaware.multithread;

import com.ikwattro.graphaware.demo.engine.BatchEngine;
import com.ikwattro.graphaware.multithread.worker.SimpleThreadPool;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;

@RestController
public class MultiThreadController {

    private final GraphDatabaseService database;
    private final SimpleThreadPool simpleThreadPool;

    @Autowired
    public MultiThreadController(GraphDatabaseService database, SimpleThreadPool simpleThreadPool) {
        this.database = database;
        this.simpleThreadPool = simpleThreadPool;
    }

    @RequestMapping(value="/test", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public void batchAction() {
        simpleThreadPool.test();
    }
}
