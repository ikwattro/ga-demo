package com.ikwattro.graphaware.multithread.worker;


import com.ikwattro.graphaware.demo.domain.Labels;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class WorkerThread implements Runnable{

    private String command;

    private final GraphDatabaseService database;

    public WorkerThread(GraphDatabaseService database, String s){
        this.database = database;
        this.command = s;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread() + command);
        processCommand();

    }

    private void processCommand(){
        try (Transaction tx = database.beginTx()){
            for (int i = 0; i < 1000; i++){
                Node n = database.createNode(Labels.User);
                n.setProperty("name", "user-" + i + "thread" + Thread.currentThread());
            }

            tx.success();
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    @Override
    public String toString(){
        return this.command;
    }
}
