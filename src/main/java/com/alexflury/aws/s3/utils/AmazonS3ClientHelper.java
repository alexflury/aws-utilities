package com.alexflury.aws.s3.utils;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * This class provides helper methods to perform basic operations in Amazon S3.
 *
 * @author Alex Flury
 */
public class AmazonS3ClientHelper {

    // The Amazon S3 client.
    private final AmazonS3 client;

    /**
     * Not a public constructor.  Use {@link #withClient} or {@link #withNewClient} to get an instance.
     *
     * @param client the Amazon S3 client
     */
    private AmazonS3ClientHelper(final AmazonS3 client) {
        this.client = client;
    }

    /**
     * Creates a new Amazon S3 client and returns a {@link AmazonS3ClientHelper} instance for the new client.
     *
     * @return a {@link AmazonS3ClientHelper} instance for a new Amazon S3 client
     */
    public static AmazonS3ClientHelper withNewClient() {
        return new AmazonS3ClientHelper(AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build());
    }

    /**
     * Returns an {@link AmazonS3ClientHelper} instance for a given {@link AmazonS3Client}.
     *
     * @param client an Amazon S3 client
     * @return a {@link AmazonS3ClientHelper} instance for {@code client}
     */
    public static AmazonS3ClientHelper withClient(final AmazonS3 client) {
        return new AmazonS3ClientHelper(client);
    }

    /**
     * Returns an input stream which reads data from the specified file in S3.
     *
     * @param bucket the bucket of the input file
     * @param key the key of the input file
     * @return an input stream which reads data from the specified file in S3, or {@code null} if the file does not
     * exist
     */
    public InputStream inputStream(String bucket, String key) {
        S3Object object = client.getObject(bucket, key);
        return object == null ? null : object.getObjectContent();
    }

    /**
     * Returns an output stream which writes data to the specified file in S3.
     *
     * @param bucket the bucket of the output file
     * @param key the key of the output file
     * @return an output stream which writes data to the specified file in S3
     */
    public OutputStream outputStream(String bucket, String key) {
        return new AmazonS3OutputStream(client, bucket, key);
    }

    /**
     * Returns an iterable list of results for a list object request.
     *
     * @param request a list object request
     * @return an iterable list of results
     */
    public Iterable<S3ObjectSummary> listObjects(ListObjectsRequest request) {
        return new IterableObjectListing(client, request);
    }

}
