/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.util.*;
import java.util.concurrent.*;


/**
 * @author DMGITAU
 */
public class Main {

    static Logger logger = Logger.getLogger(Main.class.getName());
    Utilities utilities = new Utilities ();


    public static void main(String[] args) throws Exception {

        System.out.println("\n..........................ENGINE LAUNCHED :- G2 ..........................\n");

        //Initialize executor service
        ExecutorService executor = Executors.newFixedThreadPool(20);   //Adjust pool to flavor

        long startAllJob = System.currentTimeMillis();

        //Prepare thread: contents of what to be executed in the thread
        Callable<String> future_callable_1 = () -> {

            System.out.println("\n..........................JOBS THREAD 1 START..........................\n");
            long start = System.currentTimeMillis();

            //Tasks
            SparkJobs sparkJob = new SparkJobs();

            sparkJob.runSPARKTasks(getSparkSession(), args);

            long end = System.currentTimeMillis();
            float sec = (end - start) / 1000F;
            System.out.println("Job execution period:   : " + sec + " seconds");
            System.out.println("\n..........................JOBS THREAD 1 END..........................\n");
            return "JOBS THREAD 1 END";
        };


        System.out.println("\n\n..........................NOW SUBMITTING THREADS..........................\n\n");
        //Submit
        Future<String> dfFuture1 = executor.submit(future_callable_1);


        while (!dfFuture1.isDone()) {
            System.out.println("\nThreads running.\nTasks still not complete...\n" + new Date());
            Thread.sleep(5000);         //5 seconds
        }

        //Get Future
        System.out.println("\n\n..........................NOW GETTING FUTURES (results)..........................\n\n");
        String df1 = dfFuture1.get();

        //Take time mesurements
        long endAllJob = System.currentTimeMillis();
        float sec = (endAllJob - startAllJob) / 1000F;

        System.out.println("All Jobs execution period:   : " + sec + " seconds");

        // Shut down thread pool
        System.out.println("\n\n..........................SHUTTING DOWN EXECUTOR SERVICE..........................\n\n");
        executor.shutdown();

        try {
            // If thread has never returned after 10 days just shutdown executor. Adjust to flavor
            if (!executor.awaitTermination(10, TimeUnit.DAYS)) {
                System.err.println("Threads didn't finish in 10 days!");
                executor.shutdownNow();
            }

        } catch (InterruptedException e) {
            System.err.println("InterruptedException Occurred!" + e.getMessage());
            executor.shutdownNow();
        }

    }

    public static SparkSession getSparkSession() {
        return SparkSession.builder().getOrCreate();
    }


}
