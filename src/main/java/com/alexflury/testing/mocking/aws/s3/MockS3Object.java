package com.alexflury.testing.mocking.aws.s3;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;

import static org.mockito.Mockito.*;

/**
 * Creates a mock instance of {@link S3Object}.
 *
 * @author Alex Flury
 */
public class MockS3Object {

    // The bucket of the mock object.
    private final String bucket;

    // The key of the mock object.
    private final String key;

    // The content of the mock object.
    private final byte[] content;

    /**
     * Not a public constructor.  Use {@link #builder}
     */
    private MockS3Object(Builder builder) {
        bucket = builder.bucket;
        key = builder.key;
        content = builder.content;
    }

    /**
     * A mock implementation of {@link S3Object#getObjectContent}.
     *
     * @return an input stream which contains content of the mock object
     */
    private S3ObjectInputStream getObjectContent() {
        return new S3ObjectInputStream(new ByteArrayInputStream(content), null);
    }

    /**
     * Returns an object which builds a mock instance of an S3 object.
     *
     * @return an object which builds a mock instance of an S3 object
     */
    public static MockS3Object.Builder builder() {
        return new Builder();
    }

    /**
     * This class builds a mock instance of an S3 object.
     */
    public static class Builder {
        private String bucket;
        private String key;
        private byte[] content;

        /**
         * Not a public constructor.  Use {@link #builder} to get an instance.
         */
        private Builder() {

        }

        /**
         * Sets the bucket of the S3 object.
         *
         * @param bucket the bucket of the S3 object
         * @return the {@link Builder} instance
         */
        public Builder withBucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        /**
         * Sets the key of the S3 object.
         *
         * @param key the key of the S3 object
         * @return the {@link Builder} instance
         */
        public Builder withKey(String key) {
            this.key = key;
            return this;
        }

        /**
         * Sets the content of the S3 object.
         *
         * @param content the content of the S3 object
         * @return the {@link Builder} instance
         */
        public Builder withContent(byte[] content) {
            this.content = content;
            return this;
        }

        /**
         * Returns a mock instance of {@link S3Object} with the specified parameters.
         *
         * @return a mock instance of the {@link S3Object} with the specified parameters.
         */
        public S3Object build() {
            final MockS3Object mockS3ObjectImpl = new MockS3Object(this);
            S3Object mockS3Object = mock(S3Object.class);
            doReturn(key).when(mockS3Object).getKey();
            doReturn(bucket).when(mockS3Object).getBucketName();
            doAnswer(new Answer<S3ObjectInputStream>() {
                public S3ObjectInputStream answer(InvocationOnMock invocation) {
                    return mockS3ObjectImpl.getObjectContent();
                }
            }).when(mockS3Object).getObjectContent();
            return mockS3Object;
        }
    }

}
