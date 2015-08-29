package com.wrmsr.presto.aws;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.ini4j.Ini;
import org.testng.annotations.Test;

import java.io.File;
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
}
