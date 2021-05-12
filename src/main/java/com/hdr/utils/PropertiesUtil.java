package com.hdr.utils;



import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description
 * 类描述：配置文件操作类
 * @author malongbiao
 * @Date 2015年10月25日
 * @modify
 * 修改记录：
 *
 */
public class PropertiesUtil {

    protected static Logger logger = LoggerFactory.getLogger("com.goodwill.core.utils.PropertiesUtils");

    public static Properties getProperties(String fileName) {
        Properties properties = null;
        //默认所有配置文件都放在根目录下
        InputStream inputStream = PropertiesUtil.class.getResourceAsStream("/" + fileName);
        try {
            properties = new Properties();
            properties.load(inputStream);
        } catch (IOException e) {
            logger.error("加载配置文件：" + fileName + "失败！请检查根目录下是否存在该文件。", e);
        }
        if (properties == null || properties.isEmpty()) {
            logger.error("加载配置文件：" + fileName + "不存在！请检查根目录下是否存在该文件。");
        }
        return properties;
    }

    /**
     * 指定配置项名称，返回配置值
     * @param propName
     * @return
     */
    public static String getPropertyValue(String fileName, String propName) {
        if (propName == null || propName.equals("") || propName.equals("null"))
            return "";
        Properties properties = getProperties(fileName);
        return properties.getProperty(propName);
    }

    /**
     * 设置配置项名称及其值
     * @param propName
     * @param value
     */
    public static void addPropertyValue(String fileName, String propName, String value) {
        try {
            Properties properties = getProperties(fileName);
            properties.setProperty(propName, value);
            //String srcPath =System.getProperty("user.dir")+"\\src" + filename;
            File file = new File(PropertiesUtil.class.getResource("/" + fileName).getPath());
            OutputStream out = new FileOutputStream(file);
            properties.store(out, "");
            out.close();
        } catch (IOException ex) {
            logger.error("无法保存指定的配置文件:" + fileName + "，指定文件不存在。", ex);
        }
    }

    public static void main(String args[]) {
        PropertiesUtil.addPropertyValue("test.properties", "hello", "world");
        String value = PropertiesUtil.getPropertyValue("test.properties", "hello");
        System.out.println(value);

    }

}
