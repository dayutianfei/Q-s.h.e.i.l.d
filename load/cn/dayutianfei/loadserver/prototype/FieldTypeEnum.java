package cn.dayutianfei.loadserver.prototype;

public enum FieldTypeEnum {

    TYPE_TINYINT(0),
    TYPE_SMALLINT(1),
    TYPE_INT(2),
    TYPE_INTEGER(3),
    TYPE_BIGINT(4),
    TYPE_DECIMAL(5),
    TYPE_DOUBLE(6),
    TYPE_REAL(7),
    TYPE_FLOAT(8),
    TYPE_CHAR(9),
    TYPE_VARCHAR(10),
    TYPE_STRING(11),
    TYPE_BOOLEAN(12),
    TYPE_TIMESTAMP(13),
    TYPE_BINARY(14);

    @SuppressWarnings("unused")
    private final int val;


    FieldTypeEnum(int val) {
        this.val = val;
    }


    public static boolean isNumericField(FieldTypeEnum fieldType) {
        if (fieldType == FieldTypeEnum.TYPE_TINYINT || fieldType == FieldTypeEnum.TYPE_SMALLINT
                || fieldType == FieldTypeEnum.TYPE_INT || fieldType == FieldTypeEnum.TYPE_INTEGER) {
            return true;
        }
        if (fieldType == FieldTypeEnum.TYPE_BIGINT || fieldType == FieldTypeEnum.TYPE_DOUBLE
                || fieldType == FieldTypeEnum.TYPE_REAL || fieldType == FieldTypeEnum.TYPE_FLOAT) {
            return true;
        }
        return false;
    }


    // public static boolean isIpField(FieldTypeEnum fieldType) {
    // return fieldType == FieldTypeEnum.TYPE_IPV4_ADDR || fieldType ==
    // FieldTypeEnum.TYPE_IPV6_ADDR;
    // }

    public static boolean isTimeField(FieldTypeEnum fieldType) {
        return fieldType == FieldTypeEnum.TYPE_TIMESTAMP;
    }


    public static String TableType2AvroType(FieldTypeEnum tableType) {
        try {
            switch (tableType) {
            case TYPE_TINYINT:
            case TYPE_SMALLINT:
            case TYPE_INT:
            case TYPE_INTEGER:
                return "int";
            case TYPE_BIGINT:
                return "long";
            case TYPE_DECIMAL:
            case TYPE_CHAR:
            case TYPE_VARCHAR:
            case TYPE_STRING:
            case TYPE_TIMESTAMP:
                return "string";
            case TYPE_DOUBLE:
            case TYPE_REAL:
                return "string";
            case TYPE_FLOAT:
                return "string";
            case TYPE_BOOLEAN:
                return "boolean";
            case TYPE_BINARY:
                return "bytes";
            default:
                return null;
            }
        }
        catch (Exception e) {
            return "string";
        }
    }


    public static FieldTypeEnum str2FieldTypeEnum(String str) {
        if (str.equalsIgnoreCase("tinyint")) {
            return FieldTypeEnum.TYPE_TINYINT;
        }
        else if (str.equalsIgnoreCase("smallint")) {
            return FieldTypeEnum.TYPE_SMALLINT;
        }
        else if (str.equalsIgnoreCase("int")) {
            return FieldTypeEnum.TYPE_INT;
        }
        else if (str.equalsIgnoreCase("bigint")) {
            return FieldTypeEnum.TYPE_BIGINT;
        }
        else if (str.equalsIgnoreCase("boolean")) {
            return FieldTypeEnum.TYPE_BOOLEAN;
        }
        else if (str.equalsIgnoreCase("float")) {
            return FieldTypeEnum.TYPE_FLOAT;
        }
        else if (str.equalsIgnoreCase("double")) {
            return FieldTypeEnum.TYPE_DOUBLE;
        }
        else if (str.contains("decimal")) {
            return FieldTypeEnum.TYPE_DECIMAL;
        }
        else if (str.equalsIgnoreCase("string")) {
            return FieldTypeEnum.TYPE_STRING;
        }
        else if (str.contains("varchar")) {
            // TODO
            return FieldTypeEnum.TYPE_VARCHAR;
        }
        else if (str.contains("char")) {
            return FieldTypeEnum.TYPE_CHAR;
        }
        else if (str.equalsIgnoreCase("timestamp")) {
            return FieldTypeEnum.TYPE_TIMESTAMP;
        }
        else if (str.equalsIgnoreCase("binary")) {
            return FieldTypeEnum.TYPE_BINARY;
        }
        return null;
    }
}
