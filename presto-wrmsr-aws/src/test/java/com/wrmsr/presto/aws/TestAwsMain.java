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
package com.wrmsr.presto.aws;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.core.type.TypeReference;
import com.wrmsr.presto.aws.ec2.Ec2InstanceTypeDetails;
import com.wrmsr.presto.util.Serialization;
import org.ini4j.Ini;
import org.testng.annotations.Test;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class TestAwsMain
{
    @Test
    public void test() throws Throwable
    {
        Ini i = new Ini(new File("/Users/spinlock/Dropbox/yelp_wtimoney_boto"));
        // Map<String, String> m = i.get("profile devc");
        Map<String, String> m = i.get("profile devc");
        AmazonS3 s3Client = new AmazonS3Client(new BasicAWSCredentials(m.get("aws_access_key_id"), m.get("aws_secret_access_key")));
        s3Client.listBuckets();
    }

    @Test
    public void testEc2() throws Throwable
    {
        Ini i = new Ini(new File("/Users/spinlock/Dropbox/yelp_wtimoney_boto"));
        Map<String, String> m = i.get("profile devc");
        AmazonEC2 ec2Client = new AmazonEC2Client(new BasicAWSCredentials(m.get("aws_access_key_id"), m.get("aws_secret_access_key")));
        ec2Client.describeRegions();
        ec2Client.describeInstances();
        ec2Client.describeInstanceStatus();
    }

    @Test
    public void testFoo() throws Throwable
    {
        List<Ec2InstanceTypeDetails> l;
        try (InputStream in = TestAwsMain.class.getClassLoader().getResourceAsStream("com/wrmsr/presto/aws/ec2/instance-types.json")) {
            l = Serialization.JSON_OBJECT_MAPPER.get().readValue(in, new TypeReference<List<Ec2InstanceTypeDetails>>(){});
        }
        System.out.println(l);
    }
}
