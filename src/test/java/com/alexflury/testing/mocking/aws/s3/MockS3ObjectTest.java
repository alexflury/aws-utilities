package com.alexflury.testing.mocking.aws.s3;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MockS3ObjectTest {

    @Test
    public void testBuilder() throws Exception {
        S3Object mockObject = MockS3Object.builder()
                .withBucket("mock-bucket")
                .withKey("mock/key.txt")
                .withContent("mockcontent".getBytes())
                .build();

        assertEquals("mock-bucket", mockObject.getBucketName());
        assertEquals("mock/key.txt", mockObject.getKey());
        assertEquals("mockcontent", IOUtils.toString(mockObject.getObjectContent()));
        assertEquals("mockcontent", IOUtils.toString(mockObject.getObjectContent()));
    }

}
