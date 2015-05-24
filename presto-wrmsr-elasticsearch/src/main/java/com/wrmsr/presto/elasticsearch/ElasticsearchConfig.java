/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wrmsr.presto.elasticsearch;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.net.URI;

public class ElasticsearchConfig
{
    private String httpUri;

    @NotNull
    public String getHttpUri()
    {
        return httpUri;
    }

    @Config("http-uri")
    public ElasticsearchConfig setHttpUri(String httpUri)
    {
        this.httpUri = httpUri;
        return this;
    }

    private Integer maxTotalConnections;

    public Integer getMaxTotalConnections() {
        return maxTotalConnections;
    }

    @Config("client-max-connections")
    public void setMaxTotalConnections(Integer maxTotalConnections) {
        this.maxTotalConnections = maxTotalConnections;
    }

    private Integer discoveryFrequency;

    public Integer getDiscoveryFrequency() {
        return discoveryFrequency;
    }

    @Config("client-discovery-frequency")
    public void setDiscoveryFrequency(Integer discoveryFrequency) {
        this.discoveryFrequency = discoveryFrequency;
    }

    private boolean discoveryEnabled;

    public boolean getDiscoveryEnabled() {
        return discoveryEnabled;
    }

    @Config("client-discovery-enabled")
    public void setDiscoveryEnabled(boolean discoveryEnabled) {
        this.discoveryEnabled = discoveryEnabled;
    }

    private boolean multiThreaded;

    public boolean getMultiThreaded() {
        return multiThreaded;
    }

    @Config("client-multithreaded")
    public void setMultiThreaded(boolean multiThreaded) {
        this.multiThreaded = multiThreaded;
    }

    private Integer connTimeout;

    public Integer getConnTimeout() {
        return connTimeout;
    }

    @Config("client-multithreaded")
    public void setConnTimeout(Integer connTimeout) {
        this.connTimeout = connTimeout;
    }

    private Integer readTimeout;

    public Integer getReadTimeout() {
        return readTimeout;
    }

    @Config("client-read-timeout")
    public void setReadTimeout(Integer readTimeout) {
        this.readTimeout = readTimeout;
    }

    private Integer maxConnectionIdleTime;

    public Integer getMaxConnectionIdleTime() {
        return maxConnectionIdleTime;
    }

    @Config("client-connection-idle-time")
    public void setMaxConnectionIdleTime(Integer maxConnectionIdleTime) {
        this.maxConnectionIdleTime = maxConnectionIdleTime;
    }
}
