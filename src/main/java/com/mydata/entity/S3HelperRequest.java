package com.mydata.entity;

import java.io.InputStream;

public class S3HelperRequest {
    private String fromBucket;
    private String fromKey;
    private String toBucket;
    private String toKey;
    private InputStream objectStream;
    private String fileName;
    private GlobalConstant.SOURCE_KEY sourceTypeKey;

    public String getFromBucket() {
        return fromBucket;
    }

    public void setFromBucket(String fromBucket) {
        this.fromBucket = fromBucket;
    }

    public String getFromKey() {
        return fromKey;
    }

    public void setFromKey(String fromKey) {
        this.fromKey = fromKey;
    }

    public String getToBucket() {
        return toBucket;
    }

    public void setToBucket(String toBucket) {
        this.toBucket = toBucket;
    }

    public String getToKey() {
        return toKey;
    }

    public void setToKey(String toKey) {
        this.toKey = toKey;
    }

    public InputStream getObjectStream() {
        return objectStream;
    }

    public void setObjectStream(InputStream objectStream) {
        this.objectStream = objectStream;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public GlobalConstant.SOURCE_KEY getSourceTypeKey() {
        return sourceTypeKey;
    }

    public void setSourceTypeKey(GlobalConstant.SOURCE_KEY sourceTypeKey) {
        this.sourceTypeKey = sourceTypeKey;
    }

    /**
     * Returns a string representation of the object. In general, the
     * {@code toString} method returns a string that
     * "textually represents" this object. The result should
     * be a concise but informative representation that is easy for a
     * person to read.
     * It is recommended that all subclasses override this method.
     * <p>
     * The {@code toString} method for class {@code Object}
     * returns a string consisting of the name of the class of which the
     * object is an instance, the at-sign character `{@code @}', and
     * the unsigned hexadecimal representation of the hash code of the
     * object. In other words, this method returns a string equal to the
     * value of:
     * <blockquote>
     * <pre>
     * getClass().getName() + '@' + Integer.toHexString(hashCode())
     * </pre></blockquote>
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
        return String.format("From Bucket: %s. From Key: %s. ToBucket: %s. ToKey:%s FileName: %s SourceTypeKey: %s", fromBucket, fromKey, toBucket, toKey, fileName, sourceTypeKey.toString());
    }
}
