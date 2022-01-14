package com.mydata.helper;

public class S3Helper {
  /*  private S3HelperRequest s3HelperRequest;

    public S3Helper(S3HelperRequest s3HelperRequest) {
        this.s3HelperRequest = s3HelperRequest;
    }

    @Override
    public void moveObject() {
        System.out.println("Moving object-Start");
        CopyObjectRequest copy = new CopyObjectRequest();
        copy.setSourceBucketName(s3HelperRequest.getFromBucket());
        copy.setDestinationBucketName(s3HelperRequest.getToBucket());
        copy.setSourceKey(s3HelperRequest.getFromKey());
        copy.setDestinationKey(s3HelperRequest.getToKey());

        CopyObjectResult copyResult = getS3().copyObject(copy);
        System.out.println("Moving object-End");

    }

    @Override
    public void deleteObject() {
        DeleteObjectRequest delete = new DeleteObjectRequest(s3HelperRequest.getFromBucket(),s3HelperRequest.getFromKey());
        getS3().deleteObject(delete);
    }

    @Override
    public S3HelperResponse readObject() {
        S3HelperResponse s3HelperResponse = new S3HelperResponse();
        s3HelperResponse.setOriginalRequest(s3HelperRequest);
        GetObjectRequest req = new GetObjectRequest(s3HelperRequest.getFromBucket(), s3HelperRequest.getFromKey());
        S3Object s3Object = getS3().getObject(req);
        s3HelperResponse.setObjectStream(s3Object.getObjectContent());
        return s3HelperResponse;
    }


    private AmazonS3 getS3() {
        return AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
    }

   */
}
