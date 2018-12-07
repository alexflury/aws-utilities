package com.alexflury.aws.s3.utils;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import com.alexflury.testing.mocking.aws.s3.MockAmazonS3Client;
import com.alexflury.testing.mocking.aws.s3.MockS3Object;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AmazonS3ClientHelperTest {

    @Test
    public void testInputStream() throws Exception {
        AmazonS3Client mockClient = MockAmazonS3Client.builder()
                .addObject(MockS3Object.builder()
                        .withBucket("mock-bucket-one")
                        .withKey("mock/key/one.txt")
                        .withContent("mockcontentone".getBytes())
                        .build())
                .addObject(MockS3Object.builder()
                        .withBucket("mock-bucket-two")
                        .withKey("mock/key/two.txt")
                        .withContent("mockcontenttwo".getBytes())
                        .build())
                .addObject(MockS3Object.builder()
                        .withBucket("mock-bucket-three")
                        .withKey("mock/key/three.txt")
                        .withContent("mockcontentthree".getBytes())
                        .build()).build();

        AmazonS3ClientHelper helper = AmazonS3ClientHelper.withClient(mockClient);

        assertEquals("mockcontentone", IOUtils.toString(helper.inputStream("mock-bucket-one", "mock/key/one.txt")));
        assertEquals("mockcontenttwo", IOUtils.toString(helper.inputStream("mock-bucket-two", "mock/key/two.txt")));
        assertEquals("mockcontentthree", IOUtils.toString(helper.inputStream("mock-bucket-three", "mock/key/three.txt")));
    }

    @Test
    public void testOutputStream() throws Exception {
        AmazonS3Client mockClient = MockAmazonS3Client.create();
        AmazonS3ClientHelper helper = AmazonS3ClientHelper.withClient(mockClient);
        OutputStream os = helper.outputStream("mock-bucket", "mock/key.txt");
        os.write("mockcontent".getBytes());
        os.close();

        assertEquals("mockcontent", IOUtils.toString(helper.inputStream("mock-bucket", "mock/key.txt")));
    }

    @Test
    public void testLargeOutputStream() throws Exception {
        AmazonS3Client mockClient = MockAmazonS3Client.create();
        AmazonS3ClientHelper helper = AmazonS3ClientHelper.withClient(mockClient);
        final byte[] digits = "0123456789abcdef".getBytes();
        OutputStream os = helper.outputStream("mock-bucket", "mock/key.txt");
        for (int i = 0; i < 1024 * 1024; i++) {
            os.write(digits);
        }
        os.close();

        InputStream is = helper.inputStream("mock-bucket", "mock/key.txt");
        byte[] data = IOUtils.toByteArray(is);
        assertEquals(16 * 1024 * 1024, data.length);
    }

    @Test(expected = IOException.class)
    public void testWriteAfterClose() throws Exception {
        AmazonS3Client mockClient = MockAmazonS3Client.create();
        AmazonS3ClientHelper helper = AmazonS3ClientHelper.withClient(mockClient);
        OutputStream os = helper.outputStream("mock-bucket", "mock/key.txt");
        os.write("mockcontent".getBytes());
        os.close();
        os.write("mockcontent".getBytes());
    }

    @Test
    public void testListObjects() {
        MockAmazonS3Client.Builder builder = MockAmazonS3Client.builder().withObjectListingPageSize(3);
        for (int i = 0; i < 10; i++) {
            builder.addObject(MockS3Object.builder()
                    .withBucket("mock-bucket")
                    .withKey("mock/object/listing/object-" + Integer.toString(i))
                    .withContent(("object-content-" + Integer.toString(i)).getBytes())
                    .build());
        }
        builder.addObject(MockS3Object.builder()
                .withBucket("wrong-mock-bucket")
                .withKey("mock/object/listing/wrong-bucket-object")
                .withContent("wrong-bucket-object-content".getBytes())
                .build());
        builder.addObject(MockS3Object.builder()
                .withBucket("mock-bucket")
                .withKey("wrong/prefix/wrong-prefix-object")
                .withContent("wrong-prefix-object-content".getBytes())
                .build());
        AmazonS3Client client = builder.build();

        List<S3ObjectSummary> summaries = Lists.newArrayList(AmazonS3ClientHelper.withClient(client).listObjects(new ListObjectsRequest()
                .withBucketName("mock-bucket").withPrefix("mock/object/listing/")));

        System.out.println(summaries);
        assertEquals(10, summaries.size());
    }

}
