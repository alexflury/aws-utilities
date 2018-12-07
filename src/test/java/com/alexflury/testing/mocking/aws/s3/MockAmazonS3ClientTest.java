package com.alexflury.testing.mocking.aws.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class MockAmazonS3ClientTest {

    @Test
    public void testBuilder() throws Exception {
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

        assertEquals("mockcontentone", IOUtils.toString(
                mockClient.getObject("mock-bucket-one", "mock/key/one.txt").getObjectContent()));
        assertEquals("mockcontenttwo", IOUtils.toString(
                mockClient.getObject("mock-bucket-two", "mock/key/two.txt").getObjectContent()));
        assertEquals("mockcontentthree", IOUtils.toString(
                mockClient.getObject("mock-bucket-three", "mock/key/three.txt").getObjectContent()));
    }

    @Test
    public void testPutObjectAndGetObject() throws Exception {
        AmazonS3Client mockClient = MockAmazonS3Client.create();
        final String mockContentOne = "mockcontentone";
        final String mockContentTwo = "mockcontenttwo";
        final ObjectMetadata mockMetadataOne = new ObjectMetadata();
        final ObjectMetadata mockMetadataTwo = new ObjectMetadata();
        mockMetadataOne.setContentLength(mockContentOne.getBytes().length);
        mockMetadataTwo.setContentLength(mockContentTwo.getBytes().length);
        mockClient.putObject("mock-bucket-one", "mock/key/one.txt",
                new ByteArrayInputStream(mockContentOne.getBytes()), mockMetadataOne);
        mockClient.putObject(new PutObjectRequest("mock-bucket-two", "mock/key/two.txt",
                new ByteArrayInputStream(mockContentTwo.getBytes()), mockMetadataTwo));

        assertEquals(mockContentOne, IOUtils.toString(mockClient.getObject(new GetObjectRequest(
                "mock-bucket-one", "mock/key/one.txt")).getObjectContent()));
        assertEquals(mockContentTwo, IOUtils.toString(mockClient.getObject(
                "mock-bucket-two", "mock/key/two.txt").getObjectContent()));
    }

    @Test
    public void testConstraintsNotMet() throws Exception {
        AmazonS3Client mockClient = MockAmazonS3Client.builder()
                .addObject(MockS3Object.builder()
                        .withBucket("mock-bucket")
                        .withKey("mock/key.txt")
                        .withContent("mockcontent".getBytes())
                        .build()).build();

        assertNull(mockClient.getObject("mock-bucket", "wrong/mock/key.txt"));
    }

    @Test
    public void testMultipartUpload() throws Exception {
        AmazonS3Client mockClient = MockAmazonS3Client.create();

        List<String> uploadIDs = new ArrayList<>();
        InitiateMultipartUploadResult initiateResult;
        final List<List<PartETag>> partETags = new ArrayList<>();
        byte[] partData;
        UploadPartResult uploadResult;

        initiateResult = mockClient.initiateMultipartUpload(
                new InitiateMultipartUploadRequest("mock-bucket", "mock/key/one.txt"));
        uploadIDs.add(initiateResult.getUploadId());
        partETags.add(new ArrayList<PartETag>());

        initiateResult = mockClient.initiateMultipartUpload(
                new InitiateMultipartUploadRequest("mock-bucket", "mock/key/two.txt"));
        uploadIDs.add(initiateResult.getUploadId());
        partETags.add(new ArrayList<PartETag>());

        partData = "partone".getBytes();
        uploadResult = mockClient.uploadPart(new UploadPartRequest()
                .withBucketName("mock-bucket")
                .withKey("mock/key/one.txt")
                .withUploadId(uploadIDs.get(0))
                .withPartNumber(1)
                .withPartSize(partData.length)
                .withInputStream(new ByteArrayInputStream(partData)));
        partETags.get(0).add(uploadResult.getPartETag());

        partData = "parttwo".getBytes();
        uploadResult = mockClient.uploadPart(new UploadPartRequest()
                .withBucketName("mock-bucket")
                .withKey("mock/key/two.txt")
                .withUploadId(uploadIDs.get(1))
                .withPartNumber(1)
                .withPartSize(partData.length)
                .withInputStream(new ByteArrayInputStream(partData)));
        partETags.get(1).add(uploadResult.getPartETag());

        partData = "partthree".getBytes();
        uploadResult = mockClient.uploadPart(new UploadPartRequest()
                .withBucketName("mock-bucket")
                .withKey("mock/key/one.txt")
                .withUploadId(uploadIDs.get(0))
                .withPartNumber(2)
                .withPartSize(partData.length)
                .withInputStream(new ByteArrayInputStream(partData)));
        partETags.get(0).add(uploadResult.getPartETag());

        partData = "partfour".getBytes();
        uploadResult = mockClient.uploadPart(new UploadPartRequest()
                .withBucketName("mock-bucket")
                .withKey("mock/key/two.txt")
                .withUploadId(uploadIDs.get(1))
                .withPartNumber(2)
                .withPartSize(partData.length)
                .withInputStream(new ByteArrayInputStream(partData)));
        partETags.get(1).add(uploadResult.getPartETag());

        partData = "partfive".getBytes();
        uploadResult = mockClient.uploadPart(new UploadPartRequest()
                .withBucketName("mock-bucket")
                .withKey("mock/key/one.txt")
                .withUploadId(uploadIDs.get(0))
                .withPartNumber(3)
                .withPartSize(partData.length)
                .withInputStream(new ByteArrayInputStream(partData)));
        partETags.get(0).add(uploadResult.getPartETag());

        partData = "partsix".getBytes();
        uploadResult = mockClient.uploadPart(new UploadPartRequest()
                .withBucketName("mock-bucket")
                .withKey("mock/key/two.txt")
                .withUploadId(uploadIDs.get(1))
                .withPartNumber(3)
                .withPartSize(partData.length)
                .withInputStream(new ByteArrayInputStream(partData)));
        partETags.get(1).add(uploadResult.getPartETag());

        mockClient.completeMultipartUpload(new CompleteMultipartUploadRequest(
                "mock-bucket", "mock/key/one.txt", uploadIDs.get(0), partETags.get(0)));
        mockClient.completeMultipartUpload(new CompleteMultipartUploadRequest(
                "mock-bucket", "mock/key/two.txt", uploadIDs.get(1), partETags.get(1)));

        assertEquals("partonepartthreepartfive",
                IOUtils.toString(mockClient.getObject("mock-bucket", "mock/key/one.txt").getObjectContent()));
        assertEquals("parttwopartfourpartsix",
                IOUtils.toString(mockClient.getObject("mock-bucket", "mock/key/two.txt").getObjectContent()));

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

        ObjectListing listing = client.listObjects(new ListObjectsRequest()
                .withBucketName("mock-bucket").withPrefix("mock/object/listing/"));
        assertEquals(3, listing.getObjectSummaries().size());
        assertTrue(listing.isTruncated());
        listing = client.listNextBatchOfObjects(listing);
        assertEquals(3, listing.getObjectSummaries().size());
        assertTrue(listing.isTruncated());
        listing = client.listNextBatchOfObjects(listing);
        assertEquals(3, listing.getObjectSummaries().size());
        assertTrue(listing.isTruncated());
        listing = client.listNextBatchOfObjects(listing);
        assertEquals(1, listing.getObjectSummaries().size());
        assertFalse(listing.isTruncated());
    }

}
