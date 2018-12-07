# AWS Utilities

## `AmazonS3ClientHelper`

This class contains helper methods to perform common operations in Amazon S3 such as reading and writing files.
 
To get an instance using an existing S3 client:

```java
AmazonS3ClientHelper s3Helper = AmazonS3ClientHelper.withClient(s3Client);
```

To get an instance with a new S3 client:
 
 ```java
AmazonS3ClientHelper s3Helper = AmazonS3ClientHelper.withNewClient();
 ```

### Writing to S3

#### The Classic Approach

To write to a file in S3 using the S3 client directly, you would perform the following steps:

```java
// First, write the data to an buffer in memory.
ByteArrayOutputStream os = new ByteArrayOutputStream();
PrintWriter writer = new PrintWriter(new OutputStreamWriter(os));
writer.println("Hello, world!");
writer.close();

// Next, wrap the data in an input stream to upload to S3.
byte[] content = os.toByteArray();
ObjectMetadata metadata = new ObjectMetadata();
metadata.setContentLength(content.length);
s3Client.putObject("my-bucket", "output/file.txt", new ByteArrayInputStream(content), metadata);
```

There are a couple problems with this approach.

1.  You need create an input stream to write to a file.  This is a counterintuitive requirement, which makes the code convoluted to write and difficult to understand.
1.  You need to buffer the entire file in memory before writing to S3.  This is entirely impractical for writing large files.  For large files, you have two options:
    1.  First, write the data to a large file on disk, and then upload the file to S3 using `AmazonS3Client::uploadFile`.  This is not always possible because there might not be enough disk space on the system, or your code may be running in an environment where the local file system is not accessible, such as a Lambda function.
    1.  Use the multi-part upload API to upload the data in blocks of 5 MB.  There is a non-trivial learning curve to this approach, and the code is complicated.

#### The AWS Utilities Approach

The `AmazonS3ClientHelper` class provides a simple method for getting an output stream to stream data to a file in S3.

```java
OutputStream os = AmazonS3ClientHelper.withClient(s3Client).outputStream("my-bucket", "output/file.txt");
PrintWriter writer = new PrintWriter(os);
writer.println("Hello, world!");
writer.close();
```

If the amount of data written to the stream is 5 MB or less, the whole stream is buffered in memory and uploaded to S3 as a single-part upload when the stream is closed.  If more than 5 MB is written to the stream, the stream performs a multi-part upload behind the scenes.  The data written to the stream is uploaded to S3 in blocks of 5 MB and assembled as a single file when the stream is closed.  The stream uses a thread pool to upload up to three 5 MB blocks of data to S3 simultaneously.  At a given time, up to three blocks of data may be uploading, and a fourth block of data may be buffering, so the maximum amount of data buffered in memory is 20 MB.  This output stream can write files up to 5 TB, which is the maximum file size allowed by S3.

### Reading from S3

To read from a file in S3 using the S3 client directly, you would perform the following steps:

```java
S3Object object = client.getObject("my-bucket", "input/file.txt");
if (object != null) {
    InputStream input = object.getObjectContent();
}
```

The `AmazonS3ClientHelper` class contains a lightweight convenience method `inputStream`, which returns the desired input stream, or `null` if the file does not exist.

```java
InputStream is = AmazonS3ClientHelper.withClient(s3Client).inputStream("my-bucket", "input/file.txt");
```