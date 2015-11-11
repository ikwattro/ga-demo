package com.ikwattro.graphaware.multithread.worker;

import com.ikwattro.graphaware.demo.domain.Labels;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class SimpleThreadPool {

    private final GraphDatabaseService database;

    @Autowired
    public SimpleThreadPool(GraphDatabaseService database){
        this.database = database;
    }

    public List<Integer> aTest(){
        ExecutorService executor = Executors.newFixedThreadPool(Math.max(1, Runtime.getRuntime().availableProcessors() -1));
        List<Future<Integer>> futures = new ArrayList<>();
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < 5; i++){
            futures.add(executor.submit(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    int nodesDone = 0;
                    System.out.println("Asynchronous call in thread " + Thread.currentThread().getName());
                    try (Transaction tx = database.beginTx()){
                        for (int i = 0; i < 100; i++){
                            Node n = database.createNode();
                            n.setProperty("time", System.currentTimeMillis());
                            nodesDone++;
                        }

                        tx.success();
                    }
                    return nodesDone;
                }
            }));
        }

        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.MINUTES);
            for (Future<Integer> future : futures){
                result.add(future.get());
            }
        } catch (InterruptedException | ExecutionException e){
            throw new RuntimeException(e);
        }

        return result;
    }

    public void test(){
        ExecutorService executor = Executors.newFixedThreadPool(Math.max(1, Runtime.getRuntime().availableProcessors() -2));
        Set<Long> created = new HashSet<>();

        List<Callable<Set<Long>>> tasks = new ArrayList<Callable<Set<Long>>>();
        for (int i = 0; i < 5; i++){
            tasks.add(new Job(database));
        }

        resolve(executor, tasks);

        System.out.println("Total creation : " + created.size());
    }

    public static void resolve(final ExecutorService executor, List<Callable<Set<Long>>> tasks){
        CompletionService<Set<Long>> completionService = new ExecutorCompletionService<Set<Long>>(executor);
        List<Future<Set<Long>>> futures = new ArrayList<Future<Set<Long>>>();
        Set<Long> nodeIds = new HashSet<>();

        try {
            for (Callable<Set<Long>> task : tasks){
                futures.add(completionService.submit(task));
            }

            for (int i = 0; i < tasks.size(); ++i){
                Set<Long> res = completionService.take().get();
                if (res != null){
                    nodeIds.addAll(res);
                }
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        } finally {
            executor.shutdown();
        }

        System.out.println("Nodes created " + nodeIds.size());
    }
}
