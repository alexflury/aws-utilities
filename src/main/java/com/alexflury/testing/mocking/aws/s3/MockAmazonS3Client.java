package com.alexflury.testing.mocking.aws.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Creates a mock instance of {@link AmazonS3Client}.  Basic operations such as {@link AmazonS3Client#getObject} and
 * {@link AmazonS3Client#putObject} are supported using a simple in-memory S3 implementation.  This mock can be used to
 * test functions which interact with Amazon S3.
 *
 * @author Alex Flury
 */
public class MockAmazonS3Client {

    // An in-memory set of all the S3 object that have been added to the S3 client, organized by bucket.
    private final TreeMap<String, TreeSet<S3Object>> objects = new TreeMap<>();

    // An in-memory list of all multi-part uploads which are in progress.
    private final Map<Integer, Map<Integer, UploadPartRequest>> multipartUploads = new HashMap<>();

    // The number of results to return on each page for a ListObjectsRequest.
    private final int objectListingPageSize;

    /**
     * Not a public constructor.  Use {@link #builder} or {@link #create} to get an instance.
     *
     * @param builder a {@link Builder} instance
     */
    private MockAmazonS3Client(Builder builder) {
        for (S3Object object: builder.objects) {
            addObject(object);
        }
        objectListingPageSize = builder.objectListingPageSize;
    }

    /**
     * A mock implementation of {@link AmazonS3Client#getObject(String, String)} which returns an S3 object from an
     * in-memory S3 implementation.
     *
     * @param bucket the bucket of the S3 object
     * @param key the key of the S3 object
     * @return the S3 object with the specified bucket and key, or {@code null} if the constraints are not met
     */
    private S3Object getObject(String bucket, String key) {
        return getObject(new GetObjectRequest(bucket, key));
    }

    /**
     * A mock implementation of {@link AmazonS3Client#getObject(GetObjectRequest)} which returns an S3 object from an
     * in-memory S3 implementation.
     *
     * @param request a {@link GetObjectRequest} instance
     * @return the S3 object with the specified constraints, or {@code null} if the constraints are not met
     */
    private S3Object getObject(GetObjectRequest request) {
        if (objects.containsKey(request.getBucketName())) {
            for (S3Object object : objects.get(request.getBucketName())) {
                if (request.getKey().equals(object.getKey())) {
                    return object;
                }
            }
        }

        return null;
    }

    /**
     * A mock implementation of {@link AmazonS3Client#putObject(String, String, InputStream, ObjectMetadata)} which
     * stores an S3 object in an in-memory S3 implementation.
     *
     * @param bucket the bucket of the new S3 object
     * @param key the key of the new S3 object
     * @param content the content of the new S3 object
     * @param metadata metadata for the new S3 object
     * @return {@code null}
     */
    private PutObjectResult putObject(String bucket, String key, InputStream content, ObjectMetadata metadata) {
        return putObject(new PutObjectRequest(bucket, key, content, metadata));
    }

    /**
     * A mock implementation of {@link AmazonS3Client#putObject(PutObjectRequest)} which stores an S3 object in an
     * in-memory S3 implementation.
     *
     * @param request a {@link PutObjectRequest} instance
     * @return {@code null}
     */
    private PutObjectResult putObject(PutObjectRequest request) {
        try {
            addObject(MockS3Object.builder()
                    .withBucket(request.getBucketName())
                    .withKey(request.getKey())
                    .withContent(IOUtils.toByteArray(request.getInputStream()))
                    .build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * A mock implementation of {@link AmazonS3Client#listObjects(ListObjectsRequest)}.
     *
     * @param request a {@link ListObjectsRequest} instance
     * @return an {@link ObjectListing} instance
     */
    private ObjectListing listObjects(ListObjectsRequest request) {
        return getObjectListing(request.getBucketName(), request.getPrefix(), null);
    }

    /**
     * A mock implementation of {@link AmazonS3Client#listNextBatchOfObjects(ObjectListing)}.
     *
     * @param listing an {@link ObjectListing} instance
     * @return an {@link ObjectListing} instance
     */
    private ObjectListing listNextBatchOfObjects(ObjectListing listing) {
        if (listing.isTruncated()) {
            return getObjectListing(listing.getBucketName(), listing.getPrefix(), listing.getNextMarker());
        } else {
            return null;
        }
    }

    /**
     * For internal use only.  Adds an object to the in-memory object set.
     *
     * @param object the object to add to the in-memory object set
     */
    private void addObject(S3Object object) {
        if (!objects.containsKey(object.getBucketName())) {
            objects.put(object.getBucketName(), new TreeSet<>(new S3ObjectComparator()));
        }
        objects.get(object.getBucketName()).add(object);
    }

    /**
     * For internal use only.  Returns an object listing starting from a given marker.
     *
     * @param bucket the bucket of the list objects request
     * @param prefix the prefix of the list objects request
     * @param marker the marker of the list objects request, or {@code null} if there is no marker
     * @return an {@link ObjectListing} instance which contains the requested object summaries
     */
    private ObjectListing getObjectListing(String bucket, String prefix, String marker) {
        ObjectListing listing = mock(ObjectListing.class);
        doReturn(bucket).when(listing).getBucketName();
        doReturn(prefix).when(listing).getPrefix();
        doReturn(marker).when(listing).getMarker();
        List<S3ObjectSummary> summaries = new ArrayList<>();
        boolean isTruncated = false;
        if (objects.containsKey(bucket)) {
            for (S3Object object : objects.get(bucket)) {
                if (object.getKey().startsWith(prefix) && (marker == null || object.getKey().compareTo(marker) > 0)) {
                    if (summaries.size() < objectListingPageSize) {
                        S3ObjectSummary summary = new S3ObjectSummary();
                        summary.setBucketName(object.getBucketName());
                        summary.setKey(object.getKey());
                        summaries.add(summary);
                    } else {
                        isTruncated = true;
                        break;
                    }
                }
            }
        }
        doReturn(summaries).when(listing).getObjectSummaries();
        doReturn(summaries.get(summaries.size() - 1).getKey()).when(listing).getNextMarker();
        doReturn(isTruncated).when(listing).isTruncated();
        return listing;
    }

    /**
     * A mock implementation of {@link AmazonS3Client#initiateMultipartUpload(InitiateMultipartUploadRequest)}.
     *
     * @param request a {@link InitiateMultipartUploadRequest} instance
     * @return a {@link InitiateMultipartUploadResult} instance
     */
    private InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request) {
        final int uploadID = multipartUploads.size();
        multipartUploads.put(uploadID, new TreeMap<Integer, UploadPartRequest>());
        InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
        result.setUploadId(Integer.toString(uploadID));
        return result;
    }

    /**
     * A mock implementation of {@link AmazonS3Client#uploadPart(UploadPartRequest)}.
     *
     * @param request a {@link UploadPartRequest} instance
     * @return a {@link UploadPartResult} instance
     */
    private UploadPartResult uploadPart(UploadPartRequest request) {
        final int uploadID = Integer.parseInt(request.getUploadId());
        final int partNumber = request.getPartNumber();
        multipartUploads.get(uploadID).put(partNumber, request);
        UploadPartResult result = new UploadPartResult();
        result.setPartNumber(partNumber);
        result.setETag(Integer.toString(partNumber));
        return result;
    }

    /**
     * A mock implementation of {@link AmazonS3Client#completeMultipartUpload(CompleteMultipartUploadRequest)}.
     *
     * @param request a {@link CompleteMultipartUploadRequest} instance
     * @return a {@link CompleteMultipartUploadResult} instance
     */
    private CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) {
        try {
            final int uploadID = Integer.parseInt(request.getUploadId());
            final Map<Integer, UploadPartRequest> uploadPartRequests = multipartUploads.get(uploadID);
            final Map<Integer, UploadPartRequest> uploadParts = new TreeMap<>();
            for (PartETag partETag : request.getPartETags()) {
                final int partNumber = partETag.getPartNumber();
                uploadParts.put(partNumber, uploadPartRequests.get(partNumber));
            }
            ByteArrayOutputStream content = new ByteArrayOutputStream();
            for (UploadPartRequest uploadPart : uploadParts.values()) {
                final int partSize = (int) uploadPart.getPartSize();
                byte[] partData = new byte[partSize];
                final InputStream partInput = uploadPart.getInputStream();
                partInput.read(partData, 0, partSize);
                partInput.close();
                content.write(partData, 0, partSize);
            }
            addObject(MockS3Object.builder()
                    .withBucket(request.getBucketName())
                    .withKey(request.getKey())
                    .withContent(content.toByteArray()).build());
            multipartUploads.remove(uploadID);
            return new CompleteMultipartUploadResult();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns an object which builds a mock instance of {@link AmazonS3Client}.
     *
     * @return an object which builds a mock instance of {@link AmazonS3Client}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a mock instance of {@link AmazonS3Client} with no S3 objects.
     *
     * @return a mock instance of {@link AmazonS3Client} with no S3 objects
     */
    public static AmazonS3Client create() {
        return new Builder().build();
    }

    private static class S3ObjectComparator implements Comparator<S3Object> {

        public int compare(S3Object a, S3Object b) {
            int cmp = a.getBucketName().compareTo(b.getBucketName());
            if (cmp != 0) {
                return cmp;
            }
            return a.getKey().compareTo(b.getKey());
        }

    }

    /**
     * This class builds a mock instance of {@link AmazonS3Client}
     */
    public static class Builder {

        // An in-memory list of S3 objects which the client can access.
        private Set<S3Object> objects = new HashSet<>();

        // How many objects to return on each page of results for a ListObjectsRequest.
        private int objectListingPageSize;

        /**
         * Not a public constructor.  Use {@link #builder}.
         */
        private Builder() {

        }

        /**
         * Adds an S3 object to the in-memory S3 implementation.
         *
         * @param object an S3 object
         * @return the {@link Builder} instance
         */
        public Builder addObject(S3Object object) {
            objects.add(object);
            return this;
        }

        /**
         * Sets the number of objects on each page of results for a {@code ListObjectsRequest}.
         *
         * @param objectListingPageSize the number of objects to return on each page of results for a
         * {@code ListObjectsRequest}
         * @return the {@link Builder} instance
         */
        public Builder withObjectListingPageSize(int objectListingPageSize) {
            this.objectListingPageSize = objectListingPageSize;
            return this;
        }

        /**
         * Returns a mock instance of {@link AmazonS3Client} which can access the specified S3 objects.
         *
         * @return an mock instance of {@link AmazonS3Client} which can access the specified S3 objects
         */
        public AmazonS3Client build() {
            final MockAmazonS3Client mockClientImpl = new MockAmazonS3Client(this);
            AmazonS3Client mockClient = mock(AmazonS3Client.class);
            doAnswer(new Answer<S3Object>() {
                public S3Object answer(InvocationOnMock invocation) {
                    String bucket = (String) invocation.getArguments()[0];
                    String key = (String) invocation.getArguments()[1];
                    return mockClientImpl.getObject(bucket, key);
                }
            }).when(mockClient).getObject(anyString(), anyString());
            doAnswer(new Answer<S3Object>() {
                public S3Object answer(InvocationOnMock invocation) {
                    GetObjectRequest request = (GetObjectRequest) invocation.getArguments()[0];
                    return mockClientImpl.getObject(request);
                }
            }).when(mockClient).getObject(any(GetObjectRequest.class));
            doAnswer(new Answer<PutObjectResult>() {
                public PutObjectResult answer(InvocationOnMock invocation) {
                    String bucket = (String) invocation.getArguments()[0];
                    String key = (String) invocation.getArguments()[1];
                    InputStream content = (InputStream) invocation.getArguments()[2];
                    ObjectMetadata metadata = (ObjectMetadata) invocation.getArguments()[3];
                    return mockClientImpl.putObject(bucket, key, content, metadata);
                }
            }).when(mockClient).putObject(
                    anyString(), anyString(), any(InputStream.class), any(ObjectMetadata.class));
            doAnswer(new Answer<PutObjectResult>() {
                public PutObjectResult answer(InvocationOnMock invocation) {
                    PutObjectRequest request = (PutObjectRequest) invocation.getArguments()[0];
                    return mockClientImpl.putObject(request);
                }
            }).when(mockClient).putObject(any(PutObjectRequest.class));
            doAnswer(new Answer<InitiateMultipartUploadResult>() {
                public InitiateMultipartUploadResult answer(InvocationOnMock invocation) {
                    InitiateMultipartUploadRequest request = (InitiateMultipartUploadRequest) invocation.getArguments()[0];
                    return mockClientImpl.initiateMultipartUpload(request);
                }
            }).when(mockClient).initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
            doAnswer(new Answer<UploadPartResult>() {
                public UploadPartResult answer(InvocationOnMock invocation) {
                    UploadPartRequest request = (UploadPartRequest) invocation.getArguments()[0];
                    return mockClientImpl.uploadPart(request);
                }
            }).when(mockClient).uploadPart(any(UploadPartRequest.class));
            doAnswer(new Answer<CompleteMultipartUploadResult>() {
                public CompleteMultipartUploadResult answer(InvocationOnMock invocation) {
                    CompleteMultipartUploadRequest request = (CompleteMultipartUploadRequest) invocation.getArguments()[0];
                    return mockClientImpl.completeMultipartUpload(request);
                }
            }).when(mockClient).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            doAnswer(new Answer<ObjectListing>() {
                public ObjectListing answer(InvocationOnMock invocation) {
                    ListObjectsRequest request = (ListObjectsRequest) invocation.getArguments()[0];
                    return mockClientImpl.listObjects(request);
                }
            }).when(mockClient).listObjects(any(ListObjectsRequest.class));
            doAnswer(new Answer<ObjectListing>() {
                public ObjectListing answer(InvocationOnMock invocation) {
                    ObjectListing listing = (ObjectListing) invocation.getArguments()[0];
                    return mockClientImpl.listNextBatchOfObjects(listing);
                }
            }).when(mockClient).listNextBatchOfObjects(any(ObjectListing.class));
            return mockClient;
        }

    }

}
