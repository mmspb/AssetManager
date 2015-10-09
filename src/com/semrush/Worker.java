package com.semrush;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import java.net.URL;

import java.net.URLConnection;
import java.util.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.text.MutableAttributeSet;
import javax.swing.text.html.HTML;
import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;

/**
 * Contains crawler logic
 *
 * Created by maxim on 08.10.15.
 *
 * @author maxim
 */
public class Worker extends Thread implements Runnable {

    public static final String HTTP_PREFIX = "http://";

    public static final int MAX_CONN_TIMEOUT = 500;

    public static final int MAX_READ_TIMEOUT = 2000;

    public static final int REQUEST_INTERVAL = 1000;

    private HashSet<String> collectedUrls;

    private HashSet<String> visitedUrls;

    private BlockingQueue<Job> jobs;

    private Queue<String> urlsToProcess;

    private Job job;

    private String rootUrl;

    private ParserDelegator parser;

    private Set<Job> completed;

    private AtomicInteger active;

    public Worker(BlockingQueue<Job> jobs, Set<Job> completed, AtomicInteger active) {
        this.jobs = jobs;
        this.completed = completed;
        this.active = active;
        this.parser = new ParserDelegator();
    }

    @Override
    public void run() {
        job = null;
        while (!Thread.currentThread().isInterrupted() || jobs.size() > 0) {

            try {
                job = jobs.poll(REQUEST_INTERVAL, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                break;
            }

            if (job == null) {
                break;
            }

            active.incrementAndGet();

            collectedUrls = new HashSet<>();
            visitedUrls = new HashSet<>();
            urlsToProcess = new ArrayDeque<>();

            rootUrl = job.getUrl();

            fetchUrls(rootUrl);

            visitedUrls.add(rootUrl);
            collectedUrls.add(rootUrl);

            while(urlsToProcess.size() > 0) {

                if (collectedUrls.size() >= job.getMaxUrlsOnPage()) {
                    break;
                }

                String url = urlsToProcess.poll();

                if (!visitedUrls.contains(url) && url.contains(rootUrl)) {

                    visitedUrls.add(fetchUrls(url));

                    /**
                     * Pause for the same domain
                     */
                    try {
                        Thread.sleep(REQUEST_INTERVAL);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }
            }

            System.out.println("[SemRush Crawler] ================== JOB DONE ================== ");
            System.out.println("[SemRush Crawler] Collected urls: " + collectedUrls.size());
            System.out.println("[SemRush Crawler] Queue size: " + jobs.size());

            job.setResults(collectedUrls);

            /**
             * Publish completed job
             */
            completed.add(job);

            /**
             * Update counter
             */
            active.decrementAndGet();
        }
    }

    /**
     * Connects to a web server and downloads a page
     *
     * @param targetUrl
     */
    private String fetchUrls(String targetUrl) {

        if(!targetUrl.startsWith(HTTP_PREFIX)) {
            targetUrl = HTTP_PREFIX.concat(targetUrl);
        }

        try {
            URL targetSite = new URL(targetUrl);
            URLConnection connection = targetSite.openConnection();

            connection.setConnectTimeout(MAX_CONN_TIMEOUT);
            connection.setReadTimeout(MAX_READ_TIMEOUT);

            InputStream is = (InputStream) connection.getContent();
            Reader reader = new InputStreamReader(is);

            parser.parse(reader, new HyperLink(), true);

            is.close();
            reader.close();
        } catch (Exception e) {
            collectedUrls.remove(targetUrl);
        }

        visitedUrls.add(targetUrl);

        return targetUrl;
    }

    /**
     * Event based parsing of href attribute
     */
    class HyperLink extends HTMLEditorKit.ParserCallback {

        public void handleStartTag(HTML.Tag t, MutableAttributeSet a, int pos) {
            if (t == HTML.Tag.A) {
                Object value = a.getAttribute(HTML.Attribute.HREF);
                if (value != null) {
                    String url = value.toString();

                    if (url.startsWith("/")) {
                        url = rootUrl.concat(url);
                    }

                    if (!visitedUrls.contains(url) && !collectedUrls.contains(url) && collectedUrls.size() < job.getMaxUrlsOnPage()) {
                        collectedUrls.add(url);
                        if(!urlsToProcess.contains(url)) {
                            urlsToProcess.offer(url);
                        }
                    }
                }
            }
        }
    }
}
