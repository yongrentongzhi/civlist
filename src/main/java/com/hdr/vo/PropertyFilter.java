package com.hdr.vo;


import com.hdr.enums.PropertyTypeEnum;
import com.hdr.utils.ReflectionUtils;

/**
 * 与具体ORM实现无关的属性过滤条件封装类.
 *
 * PropertyFilter主要记录页面中简单的搜索过滤条件,比Hibernate的Criterion要简单.
 *
 * @author malongbiao
 */
public class PropertyFilter {

    private String propertyName = null;
    private String propertyType = null;
    private Object propertyValue = null;
    private String matchType = null;
    public static String NAME_CONNECT_STR_OR = "_@OR@_";

    //private String enumType = null;

    public PropertyFilter() {
    }

    /**
     * @param filterName
     * 属性过滤器构造函数，非枚举类型构造
     * @param filterName 属性名称
     * @param propertyTypeCode 属性数据类型，包括：字符串、数字、布尔等
     * @param matchTypeCode 匹配方式：等于、大于等
     * @param value 属性值
     */
    public PropertyFilter(final String filterName, final String propertyTypeCode, final String matchTypeCode,
                          final String value) {
        propertyName = filterName;
        matchType = matchTypeCode;
        propertyType = propertyTypeCode;

        //按entity property中的类型将字符串转化为实际类型.
        if (PropertyTypeEnum.ENUM.name().equals(propertyType)) {
            return;
        }
        Class<?> type = Enum.valueOf(PropertyTypeEnum.class, propertyType).getValue();
        this.propertyValue = ReflectionUtils.convertStringToObject(value, type);
    }

    //	/**
    //	 * @param filterName
    //	 * 属性过滤器构造函数，枚举类型构造
    //	 * @param filterName 属性名称
    //	 * @param propertyTypeCode 属性数据类型，包括：字符串、数字、布尔、枚举等
    //	 * @param matchTypeCode 匹配方式：等于、大于等
    //	 * @param value 属性值
    //	 * @param enumType 枚举类型
    //	 */
    //	public PropertyFilter(final String filterName, final String propertyTypeCode, final String matchTypeCode,
    //			final String value, final String enumTypeCode) {
    //		propertyName = filterName;
    //		matchType = matchTypeCode;
    //		propertyType = propertyTypeCode;
    //		enumType = enumTypeCode;
    //		//按entity property中的类型将字符串转化为实际类型.
    //		Class<?> type = Enum.valueOf(PropertyType.class, propertyType).getValue();
    //		this.propertyValue = ReflectionUtils.convertStringToObject(value, type);
    //	}

    public String getPropertyName() {
        return propertyName;
    }

    public String getPropertyType() {
        return propertyType;
    }

    public void setPropertyType(String propertyType) {
        this.propertyType = propertyType;
    }

    public Object getPropertyValue() {
        return propertyValue;
    }

    public void setPropertyValue(Object propertyValue) {
        this.propertyValue = propertyValue;
    }

    public String getMatchType() {
        return matchType;
    }

    public void setMatchType(String matchType) {
        this.matchType = matchType;
    }

    /**
     * @param propertyName the propertyName to set
     */
    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    //	/**
    //	 * 获取enumType
    //	 * @return the enumType
    //	 */
    //	public String getEnumType() {
    //		return enumType;
    //	}
    //
    //	/**
    //	 * 设置enumType
    //	 * @param enumType the enumType to set
    //	 */
    //	public void setEnumType(String enumType) {
    //		this.enumType = enumType;
    //	}

}
