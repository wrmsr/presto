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
package com.wrmsr.presto.elasticsearch.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import static org.elasticsearch.common.collect.Maps.newHashMap;

public class HttpClient
{
    protected final URL baseUrl;

    protected final String encodedAuthorization;

    public HttpClient(TransportAddress transportAddress)
    {
        this(transportAddress, null, null);
    }

    public HttpClient(TransportAddress transportAddress, String username, String password)
    {
        InetSocketAddress address = ((InetSocketTransportAddress) transportAddress).address();
        try {
            baseUrl = new URL("http", address.getHostName(), address.getPort(), "/");
        }
        catch (MalformedURLException e) {
            throw new ElasticsearchException("", e);
        }
        if (username != null) {
            String userPassword = username + ":" + password;
            encodedAuthorization = DatatypeConverter.printBase64Binary(userPassword.getBytes());
        }
        else {
            encodedAuthorization = null;
        }
    }

    public HttpClientResponse request(String path)
    {
        return request("GET", path, (byte[]) null);
    }

    public HttpClientResponse request(String method, String path)
    {
        return request(method, path, (byte[]) null);
    }

    @SuppressWarnings({"unchecked"})
    public HttpClientResponse request(String method, String path, byte[] data)
    {
        ObjectMapper mapper = new ObjectMapper();
        URL url;
        try {
            url = new URL(baseUrl, path);
        }
        catch (MalformedURLException e) {
            throw new ElasticsearchException("Cannot parse " + path, e);
        }

        HttpURLConnection urlConnection;
        try {
            urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestMethod(method);
            if (data != null) {
                urlConnection.setDoOutput(true);
            }
            if (encodedAuthorization != null) {
                urlConnection.setRequestProperty("Authorization", "Basic " +
                        encodedAuthorization);
            }

            urlConnection.connect();
        }
        catch (IOException e) {
            throw new ElasticsearchException("", e);
        }

        if (data != null) {
            OutputStream outputStream = null;
            try {
                outputStream = urlConnection.getOutputStream();
                outputStream.write(data);
            }
            catch (IOException e) {
                throw new ElasticsearchException("", e);
            }
            finally {
                if (outputStream != null) {
                    try {
                        outputStream.close();
                    }
                    catch (IOException e) {
                        throw new ElasticsearchException("", e);
                    }
                }
            }
        }

        int responseCode = -1;
        try {
            responseCode = urlConnection.getResponseCode();
            InputStream inputStream = urlConnection.getInputStream();
            return new HttpClientResponse(mapper.readValue(inputStream, Map.class), responseCode, null);
        }
        catch (IOException e) {
            InputStream errStream = urlConnection.getErrorStream();
            String body = null;
            try {
                body = Streams.copyToString(new InputStreamReader(errStream));
            }
            catch (IOException e1) {
                throw new ElasticsearchException("problem reading error stream", e1);
            }
            Map m = newHashMap();
            m.put("body", body);
            return new HttpClientResponse(m, responseCode, e);
        }
        finally {
            urlConnection.disconnect();
        }
    }

    @SuppressWarnings({"unchecked"})
    public HttpClientResponse request(String method, String path, Map<String, Object> data)
    {
        ObjectMapper mapper = new ObjectMapper();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            mapper.writeValue(out, data);
        }
        catch (IOException e) {
            throw new ElasticsearchException("", e);
        }
        return request(method, path, out.toByteArray());
    }

    public HttpClientResponse requestRetrying(String method, String path, long millis, long sleepMillis)
    {
        long startTime = System.currentTimeMillis();
        while (true) {
            HttpClientResponse response = request(method, path);
            if (response.isSuccess()) {
                return response;
            }
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (elapsedTime >= millis) {
                throw new IllegalStateException();
            }
            try {
                Thread.sleep(sleepMillis);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public HttpClientResponse requestRetrying(String path, long millis, long sleepMillis)
    {
        return requestRetrying("GET", path, millis, sleepMillis);
    }

    public HttpClientResponse requestRetrying(String path, long millis)
    {
        return requestRetrying(path, millis, 100);
    }
}
