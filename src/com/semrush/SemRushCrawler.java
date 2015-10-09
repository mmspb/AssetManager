package com.semrush;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The SemRushCrawler application
 *
 * Created by maxim on 08.10.15.
 *
 * @author mmamlev
 */
public class SemRushCrawler {
    /**
     * Specifies how many threads are created per CPU core
     */
    public static final int LOAD_KOEFF = 10;

    /**
     * Specifies how many urls need to be collected from a site
     */
    public static final int MAX_PAGES = 100;

    /**
     * Executor Service
     */
    private ExecutorService executorService;

    /**
     * Runnables
     */
    private List<Worker> threads;

    /**
     * Expected results handles
     */
    private Set<Job> completedJobs;

    /**
     * Stores strings loaded from file
     *
     */
    private Set<String> data;

    /**
     * Stores queue of jobs
     */
    private BlockingQueue<Job> jobsQueue;

    /**
     * List of working threds
     */
    private AtomicInteger active;

    /**
     * Input file name
     */
    private static String inputFile;

    /**
     * Output file name
     */
    private static String outputFile;

    /**
     * Start time
     */
    private static long startTime;

    public static void main(String[] args) {

        if(args == null) {
            System.out.println("ERROR: no args specified!");
        }

        SemRushCrawler crawler = new SemRushCrawler();

        start();
        /**
         * 1. Read input data from file
         */
        if(args[0] != null) {
            crawler.load(args[0]);
        } else {
            return;
        }

        /**
         * 2. Start parsing sites
         */
        crawler.launch();

        /**
         * 3. Wait and update the progress in GUI or elsewhere
         */
        crawler.await();

        /**
         * 4. Collect the results and merge in one place
         */
        Set results = crawler.collect();

        System.out.println("[Semrush Crawler] Urls collected: " + results.size());
        /**
         * 5. Shut the workers down
         */
        crawler.shutdown();

        /**
         * 6. Write the results to the output file
         */
        crawler.save(args[1], results);

        end();
    }

    /**
     * Launches workers to process a set of sites
     * @param sites
     * @return reference to a queue with jobs
     */
    public BlockingQueue<Job> launch() {

        jobsQueue = new ArrayBlockingQueue(data.size());
        completedJobs = new HashSet<>();

        for(String site: data) {
            jobsQueue.add(new Job(site, MAX_PAGES));
        }

        int numThreads = Runtime.getRuntime().availableProcessors() * LOAD_KOEFF;

        active = new AtomicInteger(0);
        threads = new ArrayList<>();
        executorService = Executors.newFixedThreadPool(numThreads);

        for(int i = 0; i < numThreads; i++) {
            Worker worker = new Worker(jobsQueue, completedJobs, active);
            threads.add(worker);
            executorService.submit(worker);
        }

        return jobsQueue;
    }

    /**
     * Loads a list of URLs from the specified file
     * @param file contains list of urls
     * @return List of URLs
     */
    public Set<String> load(String fileName) {
        data = Collections.synchronizedSet(new HashSet<>());

        try {
            Path file = Paths.get(fileName);
            List<String> lines = Files.readAllLines(file, StandardCharsets.ISO_8859_1);

            for(String site: lines) {
                if(!data.contains(site)) {
                    data.add(site);
                }
            }
            System.out.println(data.size());
        }catch (Exception e) {
            e.printStackTrace();
        }

        return data;
    }

    /**
     * Saves result set to a file
     * @param fileName
     */
    public void save(String fileName, Set<String> resultSet) {
        try {
            FileWriter fileWriter = new FileWriter(new File(fileName));

            for(String url : resultSet) {
                fileWriter.write(url + "\n");
            }

            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Waits until the process is completed
     * @param queue
     */
    public void await() {
        while(true) {
            try {
                System.out.println("[SemRush Crawler] Queue size: " + jobsQueue.size() + ", Active: " + active.get() + ", Completed sub-tasks: " + completedJobs.size());
                Thread.currentThread().sleep(1000);
                if(jobsQueue.size() == 0 && active.get() == 0) {
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Aggregates data from workers
     * @return
     */
    public Set<String> collect() {

        Set<String> finalResultSet = new HashSet<>();

        for(Job job: completedJobs) {
            try {
                if(job!= null) {
                    finalResultSet.addAll(job.getResults());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return finalResultSet;
    }

    /**
     * Shuts the workers down
     */
    public void shutdown() {
        executorService.shutdownNow();
    }

    /**
     * Is crawler running
     */
    public boolean isRunning() {
        for(Thread thread : threads) {
           if(thread.isAlive()) {
               return true;
           }
        }
        return false;
    }


    /**
     * Writes start time into variable
     */
    private static void start() {
        startTime = System.currentTimeMillis();
    }

    /**
     * Computes and shows total execution time
     */
    private static void end() {
        long endTime = System.currentTimeMillis() - startTime;
        System.out.println("[ TOTAL EXECUTION TIME ] " + endTime/1000 + " seconds");
    }


}
