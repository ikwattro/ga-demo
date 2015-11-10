package com.ikwattro.graphaware.demo.api;

import com.ikwattro.graphaware.demo.engine.BatchEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;

@RestController
public class DemoController {

    private final GraphDatabaseService database;
    private final BatchEngine engine;

    @Autowired
    public DemoController(GraphDatabaseService database, BatchEngine engine) {
        this.database = database;
        this.engine = engine;
    }

    @RequestMapping(value="/batch", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.OK)
    public HashMap<String, Integer> batchAction() {
        HashMap<String, Integer> result = new HashMap<>();
        try (Transaction tx = database.beginTx()) {
            engine.doBatchProcessWithFrameworkBatcher();
            tx.success();
        }

        return result;
    }

    @RequestMapping(value="/userbatch", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.OK)
    public void userBatchAction() {
        try (Transaction tx = database.beginTx()) {
            engine.doUserBatchProcess();
            tx.success();
        }

        return;
    }
}
