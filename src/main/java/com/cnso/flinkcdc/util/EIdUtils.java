package com.cnso.flinkcdc.util;

/**
 * @author jia.xd
 * @date 2023/2/9
 */
public class EIdUtils {

    private static final String UUID = "8c5016a0-b379-4625-a3e6-b7e8e8ff4b52";

    private static final String ZZ_EID_KEY = "zzsj20230301";

    public static String generateEid(String eid){
        String str = UUID + eid + ZZ_EID_KEY;
        return Md5Utils.hash(str);
    }
}
