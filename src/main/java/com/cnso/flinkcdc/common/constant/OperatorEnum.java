package com.cnso.flinkcdc.common.constant;

public enum OperatorEnum {

    READ("c"),
    CREATE("c"),
    UPDATE("u"),
    DELETE("d"),
    TRUNCATE("d")
    ;

    private final String code;

    private OperatorEnum(String code) {
        this.code = code;
    }

    public String code() {
        return this.code;
    }

    public static OperatorEnum forName(String name) {
        OperatorEnum[] var1 = values();
        int var2 = var1.length;

        for(int var3 = 0; var3 < var2; ++var3) {
            OperatorEnum op = var1[var3];
            if (op.name().equalsIgnoreCase(name)) {
                return op;
            }
        }

        return null;
    }

}
