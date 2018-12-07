package com.alexflury.aws.s3.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.alexflury.utils.executors.ExecutorUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * This output stream writes data to a file in S3.
 *
 * <p>If 5 MB or less is written to the stream, the whole output is buffered in memory and uploaded as single part
 * upload.  If more than 5 MB is written to the stream, blocks of 5 MB of data are buffered in memory before uploading
 * to S3 as a multi-part upload.  Up to three worker threads are used to upload parts to S3 concurrently.  Each worker
 * thread has a 5 MB buffer, and the main thread has an additional 5 MB buffer, which it hands off to the next worker
 * thread when it becomes available.  The total buffer size when all worker threads are at capacity is therefore 20 MB.
 * </p>
 *
 * @author Alex Flury
 */
class AmazonS3OutputStream extends OutputStream {

    // The maximum size for each part of a multi-part upload.
    private static final long PART_SIZE = 1024 * 1024 * 5;

    // The number of threads to use to upload parts of a multi-part upload.
    private static final int NUM_THREADS = 3;

    // The Amazon S3 client.
    private final AmazonS3 client;

    // The bucket of the output file.
    private final String bucket;

    // The key of the output file.
    private final String key;

    // This buffer contains data which has been written to the stream, but has not been uploaded to S3.
    private final ByteArrayOutputStream buf;

    // This flag indicates if the output stream has been closed.
    private boolean isClosed;

    // The ID of the multi-part upload.
    private String uploadID;

    // The next part number in a multi-part upload.
    private int nextPartNumber = 1;

    // The return values from the part uploads.
    private final List<Future<UploadPartResult>> uploadResults = new ArrayList<>();

    // A thread pool for uploading parts asynchronously.
    private ExecutorService uploadExecutor;

    /**
     * Not a public constructor.  Use {@link AmazonS3ClientHelper#outputStream} to get an instance.
     *
     * @param client the Amazon S3 client
     * @param bucket the output bucket
     * @param key the output key
     */
    AmazonS3OutputStream(AmazonS3 client, String bucket, String key) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        buf = new ByteArrayOutputStream();
        isClosed = false;
    }

    @Override
    public void write(int b) throws IOException {
        if (isClosed) {
            throw new IOException(String.format(
                    "Attempted to write to S3 after the output stream was closed: %s", path()));
        } else {
            if (buf.size() == PART_SIZE) {
                if (uploadID == null) {
                    initiateMultipartUpload();
                }
                uploadPart();
            }
            buf.write(b);
        }
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            if (uploadID == null) {
                uploadSinglePart();
            } else {
                uploadLastPart();
                completeMultipartUpload();
            }
        }
        isClosed = true;
    }

    /**
     * Uploads the whole file with a single-part upload for files up to 5 MB.
     *
     * @throws IOException if an I/O error occurs
     */
    private void uploadSinglePart() throws IOException {
        try {
            byte[] bytes = buf.toByteArray();
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(bytes.length);
            client.putObject(bucket, key, new ByteArrayInputStream(bytes), metadata);
        } catch (AmazonClientException e) {
            throw new IOException(e);
        }
    }

    /**
     * Initiates a multi-part upload for files bigger than 5 MB.
     *
     * @throws IOException if an I/O error occurs
     */
    private void initiateMultipartUpload() throws IOException {
        try {
            InitiateMultipartUploadResult initiateResult = client.initiateMultipartUpload(
                    new InitiateMultipartUploadRequest(bucket, key));
            uploadID = initiateResult.getUploadId();
            uploadExecutor = ExecutorUtils.blockingFixedThreadPool(NUM_THREADS);
        } catch (AmazonClientException e) {
            throw new IOException(e);
        }
    }

    /**
     * Uploads one part of a multi-part upload.
     *
     * @throws IOException if an I/O error occurs
     */
    private void uploadPart() throws IOException {
        try {
            uploadResults.add(uploadExecutor.submit(new UploadPartTask(new UploadPartRequest()
                    .withBucketName(bucket)
                    .withKey(key)
                    .withUploadId(uploadID)
                    .withPartNumber(nextPartNumber++)
                    .withPartSize(buf.size())
                    .withInputStream(new ByteArrayInputStream(buf.toByteArray())))));
            buf.reset();
        } catch (AmazonClientException e) {
            abortMultipartUpload();
            throw new IOException(e);
        }
    }

    /**
     * Uploads the last part of a multi-part upload.
     *
     * @throws IOException if an I/O error occurs
     */
    private void uploadLastPart() throws IOException {
        try {
            uploadResults.add(uploadExecutor.submit(new UploadPartTask(new UploadPartRequest()
                    .withBucketName(bucket)
                    .withKey(key)
                    .withUploadId(uploadID)
                    .withPartNumber(nextPartNumber++)
                    .withLastPart(true)
                    .withPartSize(buf.size())
                    .withInputStream(new ByteArrayInputStream(buf.toByteArray())))));
        } catch (AmazonClientException e) {
            abortMultipartUpload();
            throw new IOException(e);
        }
    }

    /**
     * Assembles all the parts of a multi-part upload as one file in S3.
     *
     * @throws IOException if an I/O error occurs
     */
    private void completeMultipartUpload() throws IOException {
        try {
            final List<PartETag> partETags = new ArrayList<>();
            for (Future<UploadPartResult> uploadResult : uploadResults) {
                partETags.add(uploadResult.get().getPartETag());
            }
            uploadExecutor.shutdown();
            client.completeMultipartUpload(new CompleteMultipartUploadRequest(
                    bucket, key, uploadID, partETags));
        } catch (AmazonClientException | InterruptedException | ExecutionException e) {
            abortMultipartUpload();
            throw new IOException(e);
        }
    }

    /**
     * Aborts the current multi-part upload.
     *
     * @throws IOException if an I/O error occurs
     */
    private void abortMultipartUpload() throws IOException {
        try {
            client.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadID));
        } catch (AmazonClientException e) {
            throw new IOException(e);
        }
    }

    /**
     * Returns the full path of the output file.
     *
     * @return the full path of the output file
     */
    private String path() {
        return String.format("s3://%s/%s", bucket, key);
    }

    /**
     * This task uploads one part of a multi-part upload to S3.
     */
    private class UploadPartTask implements Callable<UploadPartResult> {

        // The upload request
        private final UploadPartRequest request;

        /**
         * @param request the upload request
         */
        public UploadPartTask(UploadPartRequest request) {
            this.request = request;
        }

        @Override
        public UploadPartResult call() {
            return client.uploadPart(request);
        }

    }

}
