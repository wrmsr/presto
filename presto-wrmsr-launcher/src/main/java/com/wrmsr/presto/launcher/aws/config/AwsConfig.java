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
package com.wrmsr.presto.launcher.aws.config;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Throwables;
import org.ini4j.Ini;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class AwsConfig
{
    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.WRAPPER_OBJECT
            // defaultImpl = UnknownConfigNode.class
    )
    @JsonSubTypes({
            @JsonSubTypes.Type(value = PropertiesCredentials.class, name = "properties"),
            @JsonSubTypes.Type(value = IniCredentials.class, name = "ini"),
            @JsonSubTypes.Type(value = BasicCredentials.class, name = "basic"),
    })
    public static abstract class Credentials
    {
        public abstract AWSCredentials getAwsCredentials();
    }

    public static final class PropertiesCredentials extends Credentials
    {
        private final String path;

        @JsonCreator
        public static PropertiesCredentials valueOf(String path)
        {
            return new PropertiesCredentials(path);
        }

        @JsonCreator
        public PropertiesCredentials(
                @JsonProperty("path") String path)
        {
            this.path = path;
        }

        @JsonProperty
        public String getPath()
        {
            return path;
        }

        @Override
        public AWSCredentials getAwsCredentials()
        {
            try {
                return new com.amazonaws.auth.PropertiesCredentials(new File(path));
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public static final class IniCredentials extends Credentials
    {
        private final String path;
        private final String key;

        @JsonCreator
        public IniCredentials(
                @JsonProperty("path") String path,
                @JsonProperty("key") String key)
        {
            this.path = path;
            this.key = key;
        }

        @JsonProperty
        public String getPath()
        {
            return path;
        }

        @JsonProperty
        public String getKey()
        {
            return key;
        }

        @Override
        public AWSCredentials getAwsCredentials()
        {
            try {
                Ini i = new Ini(new File(path));
                Map<String, String> m = i.get(key);
                return new BasicAWSCredentials(m.get("aws_access_key_id"), m.get("aws_secret_access_key"));
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public static final class BasicCredentials extends Credentials
    {
        private final String accessKey;
        private final String secretKey;

        @JsonCreator
        public BasicCredentials(
                @JsonProperty("access") String accessKey,
                @JsonProperty("secret") String secretKey)
        {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
        }

        @JsonProperty("access")
        public String getAccessKey()
        {
            return accessKey;
        }

        @JsonProperty("secret")
        public String getSecretKey()
        {
            return secretKey;
        }

        @Override
        public AWSCredentials getAwsCredentials()
        {
            return new BasicAWSCredentials(accessKey, secretKey);
        }
    }
}
