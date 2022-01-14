package com.mydata.entity;

public class SourceFieldParameter {
    GlobalConstant.SOURCE_KEY sourceKey;
    String parameterName;
    GlobalConstant.PSQL_PARAMETER_TYPE parameterType;
    String precision;
    Integer parameterOrder;
    Boolean etlField;

    public SourceFieldParameter(GlobalConstant.SOURCE_KEY sourceKey, String parameterName, GlobalConstant.PSQL_PARAMETER_TYPE parameterType, String precision, Integer parameterOrder, Boolean etlField) {
        this.sourceKey = sourceKey;
        this.parameterName = parameterName;
        this.parameterType = parameterType;
        this.precision = precision;
        this.parameterOrder = parameterOrder;
        this.etlField = etlField;
    }

    public SourceFieldParameter(GlobalConstant.SOURCE_KEY sourceKey, String parameterName, GlobalConstant.PSQL_PARAMETER_TYPE parameterType, String precision, Integer parameterOrder) {
        this.sourceKey = sourceKey;
        this.parameterName = parameterName;
        this.parameterType = parameterType;
        this.precision = precision;
        this.parameterOrder = parameterOrder;
        this.etlField = false;
    }

    public String getParameterName() {
        return parameterName;
    }

    public GlobalConstant.PSQL_PARAMETER_TYPE getParameterType() {
        return parameterType;
    }


    public String getPrecision() {
        return precision;
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
        return String.format("Source Key %s. parameterName: %s parameterType: %s precision %s parameterOrder: %d etlField %b", sourceKey.toString(), parameterName, parameterType.toString(), precision, parameterOrder, etlField);

    }
}
