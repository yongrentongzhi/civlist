package com.hdr.utils;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class MysqlDBCP {

	protected static Logger logger = LoggerFactory.getLogger(MysqlDBCP.class);

	/**
	 * 在java中，编写数据库连接池需实现java.sql.DataSource接口，每一种数据库连接池都是DataSource接口的实现
	 * DBCP连接池就是java.sql.DataSource接口的一个具体实现
	 */
	private static DataSource ds = null;
	//在静态代码块中创建数据库连接池
	static {
		try {
			//加载dbcpconfig.properties配置文件
			InputStream in = MysqlDBCP.class.getClassLoader().getResourceAsStream("dbcp.properties");
			Properties prop = new Properties();
			prop.load(in);
			//创建数据源
			ds = BasicDataSourceFactory.createDataSource(prop);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	/**
	* @Method: getConnection
	* @Description: 从数据源中获取数据库连接
	* @return Connection
	* @throws SQLException
	*/
	public static Connection getConnection() throws SQLException {
		//从数据源中获取数据库连接
		return ds.getConnection();
	}

	/**
	* @Method: release
	* @Description: 释放资源，
	* 释放的资源包括Connection数据库连接对象，负责执行SQL命令的Statement对象，存储查询结果的ResultSet对象
	* @param conn
	* @param st
	* @param rs
	*/
	public static void release(Connection conn, Statement st, ResultSet rs) {
		if (rs != null) {
			try {
				//关闭存储查询结果的ResultSet对象
				rs.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			rs = null;
		}
		if (st != null) {
			try {
				//关闭负责执行SQL命令的Statement对象
				st.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		if (conn != null) {
			try {
				//将Connection连接对象还给数据库连接池
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
