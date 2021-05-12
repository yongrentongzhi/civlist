package com.hdr.enums;

import com.hdr.basic.EnumType;

import java.math.BigDecimal;
import java.util.Date;

public enum PropertyTypeEnum {
    STRING(String.class), INT(Integer.class), LONG(Long.class), DOUBLE(Double.class), DATE(Date.class), BOOL(
            Boolean.class), SHORT(Short.class), BIGDECIMAL(BigDecimal.class),ENUM(EnumType.class);

    private Class<?> clazz;

    PropertyTypeEnum(Class<?> clazz) {
        this.clazz = clazz;
    }

    public Class<?> getValue() {
        return clazz;
    }
}