package com.alexflury.aws.s3.utils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Iterators;

import java.util.Iterator;

/**
 * This class paginates through the results of a {@code ListObjectsRequest} to create an iterable list of results.
 *
 * @author Alex Flury
 */
class IterableObjectListing implements Iterable<S3ObjectSummary> {

    // The S3 client
    private final AmazonS3 client;

    // The ListObjectRequest
    private final ListObjectsRequest request;

    /**
     * Not a public constructor.  Use {@link AmazonS3ClientHelper#listObjects} to get an instance.
     *
     * @param client the S3 client
     * @param request a {@code ListObjectsRequest} instance
     */
    IterableObjectListing(AmazonS3 client, ListObjectsRequest request) {
        this.client = client;
        this.request = request;
    }

    /**
     * Returns an iterator which provides the results of the {@code ListObjectsRequest}.
     *
     * @return an iterator which provides the results of the {@code ListObjectsRequest}.
     */
    public Iterator<S3ObjectSummary> iterator() {
        return Iterators.concat(Iterators.transform(
                new ObjectListingIterator(request), IterableObjectListing::objectIterator));
    }

    /**
     * Returns an iterator which provides one page of results from a {@code ListObjectsRequest}.
     *
     * @param listing an {@code ObjectListing} instance containing one page of results
     * @return an iterator which provides one page of results
     */
    private static Iterator<S3ObjectSummary> objectIterator(ObjectListing listing) {
        return listing.getObjectSummaries().iterator();
    }

    /**
     * This class iterates through the pages of results from a {@code ListObjectsRequest}.
     */
    private class ObjectListingIterator implements Iterator<ObjectListing> {

        // The current page of results
        private ObjectListing next;

        /**
         * @param request a {@code ListObjectsRequest} instance
         */
        private ObjectListingIterator(ListObjectsRequest request) {
            this.next = client.listObjects(request);
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public ObjectListing next() {
            ObjectListing current = next;
            if (current.isTruncated()) {
                next = client.listNextBatchOfObjects(current);
            } else {
                next = null;
            }
            return current;
        }

    }

}
