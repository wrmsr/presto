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
package com.wrmsr.presto.launcher;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryRequest;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.wrmsr.presto.launcher.aws.ec2.Ec2InstanceTypeDetails;
import org.ini4j.Ini;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

public class TestAwsMain
{
    @Test
    public void test() throws Throwable
    {
        Ini i = new Ini(new File("/Users/wtimoney/.boto"));
        // Map<String, String> m = i.get("profile devc");
        Map<String, String> m = i.get("Credentials");
        AmazonS3 s3Client = new AmazonS3Client(new BasicAWSCredentials(m.get("aws_access_key_id"), m.get("aws_secret_access_key")));
        s3Client.listBuckets();
    }

    @Test
    public void testEc2() throws Throwable
    {
        Ini i = new Ini(new File("/Users/wtimoney/.boto"));
        Map<String, String> m = i.get("Credentials");
        AmazonEC2 ec2Client = new AmazonEC2Client(new BasicAWSCredentials(m.get("aws_access_key_id"), m.get("aws_secret_access_key")));
        ec2Client.setRegion(Region.getRegion(Regions.US_WEST_2));
        ec2Client.describeRegions();
        DescribeInstancesResult dir = ec2Client.describeInstances();

        List<Reservation> reservations = dir.getReservations();
        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();
            for (Instance instance : instances) {
                System.out.println(instance.getInstanceId());
            }
        }

        ec2Client.describeInstanceStatus();
    }

    @Test
    public void testFoo() throws Throwable
    {
        Map<String, Ec2InstanceTypeDetails> m = Ec2InstanceTypeDetails.read();
        System.out.println(m);
    }

    @Test
    public void testSpotPrices() throws Throwable
    {
        Ini ini = new Ini(new File("/Users/wtimoney/.boto"));
        Map<String, String> m = ini.get("Credentials");
        AmazonEC2 ec2 = new AmazonEC2Client(new BasicAWSCredentials(m.get("aws_access_key_id"), m.get("aws_secret_access_key")));

        // Get the spot price history
        String nextToken = "";
        do {
            // Prepare request (include nextToken if available from previous result)
            DescribeSpotPriceHistoryRequest request = new DescribeSpotPriceHistoryRequest().withNextToken(nextToken);

            // Perform request
            DescribeSpotPriceHistoryResult result = ec2.describeSpotPriceHistory(request);
            for (int i = 0; i < result.getSpotPriceHistory().size(); i++) {
                System.out.println(result.getSpotPriceHistory().get(i));
            }

            // 'nextToken' is the string marking the next set of results returned (if any),
            // it will be empty if there are no more results to be returned.
            nextToken = result.getNextToken();

        } while (!nextToken.isEmpty());
    }
}
