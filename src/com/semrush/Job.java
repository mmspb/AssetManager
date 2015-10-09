package com.semrush;

import java.util.Set;
/**
 * Represents a sub task to perform
 *
 * Created by maxim on 08.10.15.
 *
 * @author maxim
 */
public class Job {
    /**
     * URL of a site to be analyzed
     */
    private String url;

    /**
     * Set of urls found on a site
     */
    private Set results;

    /**
     * Maximum urls to be taken from a web page
     */
    private int maxUrlsOnPage;


    public Job(String url, int maxUrlsOnPage) {
        this.url = url;
        this.maxUrlsOnPage = maxUrlsOnPage;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Set getResults() {
        return results;
    }

    public void setResults(Set results) {
        this.results = results;
    }

    public int getMaxUrlsOnPage() {
        return maxUrlsOnPage;
    }

    public void setMaxUrlsOnPage(int maxUrlsOnPage) {
        this.maxUrlsOnPage = maxUrlsOnPage;
    }
}
