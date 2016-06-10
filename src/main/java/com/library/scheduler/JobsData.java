/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.library.scheduler;

import com.library.httpconnmanager.HttpClientPool;

/**
 *
 * @author smallgod
 */
public class JobsData {
    
    private String remoteUrl;
    private HttpClientPool httpClientPool;

    public String getRemoteUrl() {
        return remoteUrl;
    }

    public void setRemoteUrl(String remoteUrl) {
        this.remoteUrl = remoteUrl;
    }

    public HttpClientPool getHttpClientPool() {
        return httpClientPool;
    }

    public void setHttpClientPool(HttpClientPool httpClientPool) {
        this.httpClientPool = httpClientPool;
    }
}
