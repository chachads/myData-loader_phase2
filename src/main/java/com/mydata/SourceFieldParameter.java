package com.mydata;

import com.mydata.common.GlobalConstant;

public class SourceFieldParameter {
    GlobalConstant.SOURCE_KEY sourceKey;
    String parameterName;
    GlobalConstant.PSQL_PARAMETER_TYPE parameterType;
    Integer parameterOrder;
    Boolean etlField;
    String dateFormat;
    String timestampFormat;

    public SourceFieldParameter() {
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public String getTimestampFormat() {
        return timestampFormat;
    }

    public void setTimestampFormat(String timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    public void setSourceKey(GlobalConstant.SOURCE_KEY sourceKey) {
        this.sourceKey = sourceKey;
    }

    public void setParameterName(String parameterName) {
        this.parameterName = parameterName;
    }

    public void setParameterType(GlobalConstant.PSQL_PARAMETER_TYPE parameterType) {
        this.parameterType = parameterType;
    }

    public void setParameterOrder(Integer parameterOrder) {
        this.parameterOrder = parameterOrder;
    }

    public void setEtlField(Boolean etlField) {
        this.etlField = etlField;
    }

    public String getParameterName() {
        return parameterName;
    }

    public GlobalConstant.PSQL_PARAMETER_TYPE getParameterType() {
        return parameterType;
    }

    public Integer getParameterOrder() {
        return parameterOrder;
    }

    public GlobalConstant.SOURCE_KEY getSourceKey() {
        return sourceKey;
    }

    public Boolean getEtlField() {
        return etlField;
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
        return String.format("Source Key %s. parameterName: %s parameterType: %s parameterOrder: %d etlField %b", sourceKey.toString(), parameterName, parameterType.toString(), parameterOrder, etlField);

    }
}
