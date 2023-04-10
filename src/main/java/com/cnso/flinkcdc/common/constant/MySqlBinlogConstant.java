package com.cnso.flinkcdc.common.constant;

public class MySqlBinlogConstant {

    /**
     * 操作，eg: CREATE, UPDATE, DELETE
     */
    public static final String OP= "op";

    /**
     * 接收到的源
     */
    public static final String SOURCE = "source";

    /**
     * 操作的库
     */
    public static final String DB = "db";

    /**
     * 操作的表
     */
    public static final String TABLE = "table";

    /**
     * 变化前
     */
    public static final String BEFORE = "before";

    /**
     * 变化后
     */
    public static final String AFTER = "after";

    /**
     * 主键
     */
    public static final String PRIMARY_KEY = "key";

    /**
     *
     */
    public static final String DATA = "data";

    /**
     * 操作时间
     */
    public static final String TS_MS = "ts_ms";

}
