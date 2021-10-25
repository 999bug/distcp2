package org.apache.hadoop.tools.appendnf.consts;


public class I2BBAction {

    public static final String BLOB_BEGIN_MARK = "<$>";
    public static final String BLOB_TXT_MARK = "<&>";
    /**
     * <@>:存在记录  <#>:不存在记录
     */
    public static final String CHECK_EXIT = "<@>";
    public static final String CHECK_NO_EXIT = "<#>";

    public static final short BLOB_TYPE_SOURCE_INIT = 0;
    public static final short BLOB_TYPE_DATA = 1;

    public static void addByte(byte[] data, byte value, int offset) {
        data[offset + 0] = value;
    }



    public static void addData(byte[] data, byte[] value, int offset) {
        System.arraycopy(value, 0, data, offset, value.length);
    }

    public static void addBlobHeader(byte[] msg) {
        byte[] bHeader = BLOB_BEGIN_MARK.getBytes();
        for (int i = 0; i < 3; i++) {
            msg[i] = bHeader[i];
        }
    }
}
