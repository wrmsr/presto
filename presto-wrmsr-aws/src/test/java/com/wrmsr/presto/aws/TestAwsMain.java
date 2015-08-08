package com.wrmsr.presto.aws;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.testng.annotations.Test;

public class TestAwsMain
{
    @Test
    public void test() throws Throwable
    {
        AmazonS3 s3Client = new AmazonS3Client();
    }
}
