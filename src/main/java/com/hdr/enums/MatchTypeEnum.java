package com.hdr.enums;

public enum MatchTypeEnum {
    EQ("=", "等于"), LIKE("like", "相似"), GT(">", "大于"), GE(">=", "大于等于"), LT("<", "小于"), LE("<=", "小于等于"), NE("<>",
            "不等于"), IN("in", "包含"), NOTIN("nin", "不包含"), ISNULL("isnull", "为空"), NOTNULL("notnull", "为空"), INLIKE(
            "inlike", "IN相似"), NOTLIKE("nlike", "不包含相似"), STARTWITH("sw", "以此开始"), ENDWITH("ew", "以此结束");
    private String operation;
    private String label;

    private MatchTypeEnum(String op, String label) {
        this.operation = op;
        this.label = label;
    }

    /**
     * 取得英文符号
     * @return
     */
    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    /**
     * 取得汉语名称
     * @return
     */
    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }
}
