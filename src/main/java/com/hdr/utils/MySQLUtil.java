package com.hdr.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySQLUtil {

	//	private static final String CONFIG_FILE_NAME = "jdbc.properties";
	//	static final String URL = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME, "jdbc.url");
	//	static final String USERNAME = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME, "jdbc.username");
	//	static final String PASSWORD = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME, "jdbc.password");
	//	static final String DRIVER = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME, "jdbc.driverClassName");
	//	private static Connection connection = null;
	//	private boolean connected = false;

	protected static Logger logger = LoggerFactory.getLogger(MySQLUtil.class);

	public MySQLUtil() {
		//		try {
		//			Class.forName(DRIVER);
		//		} catch (ClassNotFoundException e) {
		//			logger.error(e.getMessage(), e);
		//		}
		//		try {
		//			connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		//			connected = true;
		//		} catch (SQLException e) {
		//			logger.error(e.getMessage(), e);
		//		}
	}

	//	public static Connection getConnection() {
	//		Connection conn = null;
	//		try {
	//			Class.forName(DRIVER);
	//			conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
	//		} catch (ClassNotFoundException e) {
	//			logger.error(e.getMessage(), e);
	//		} catch (SQLException e) {
	//			logger.error(e.getMessage(), e);
	//		}
	//		return conn;
	//	}

	public static String insert(String sql) {
		int lineNum = 0;
		Connection conn = null;
		ResultSet resultSet = null;
		Integer id = 0;
		try {
			conn = MysqlDBCP.getConnection();
			PreparedStatement preStmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			lineNum = preStmt.executeUpdate();
			resultSet = preStmt.getGeneratedKeys();
			if (resultSet.next()) {
				id = resultSet.getInt(1);
			}
			preStmt.close();
		} catch (SQLException e) {
			logger.error(e.getMessage(), e);
		} finally {
			MysqlDBCP.release(conn, null, null);
		}
		return id.toString();
	}

	public static int update(String sql) {
		int lineNum = 0;
		Connection conn = null;
		try {
			conn = MysqlDBCP.getConnection();
			PreparedStatement preStmt = conn.prepareStatement(sql);
			lineNum = preStmt.executeUpdate();
			preStmt.close();
		} catch (SQLException e) {
			logger.error(e.getMessage(), e);
		} finally {
			MysqlDBCP.release(conn, null, null);
		}
		return lineNum;
	}

	public static ArrayList<Map<String, String>> select(String sql, String tableName) {
		ArrayList<Map<String, String>> result = new ArrayList<>();
		Connection conn = null;
		try {
			conn = MysqlDBCP.getConnection();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			String[] frame = getFrame(tableName);
			while (rs.next()) {
				Map<String, String> tmp = new HashMap<>();
				for (String key : frame) {
					if (key == "#")
						break;
					tmp.put(key, rs.getString(key));
				}
				result.add(tmp);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			logger.error(e.getMessage(), e);
		} finally {
			MysqlDBCP.release(conn, null, null);
		}
		return result;
	}

	public static ArrayList<Map<String, String>> query(String sql, String[] columns) {
		ArrayList<Map<String, String>> result = new ArrayList<>();
		Connection conn = null;
		try {
			conn = MysqlDBCP.getConnection();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			String[] frame = columns;
			while (rs.next()) {
				Map<String, String> tmp = new HashMap<>();
				for (String key : frame) {
					if (key == "#")
						break;
					tmp.put(key, rs.getString(key));
				}
				result.add(tmp);
			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			logger.error(e.getMessage(), e);
		} finally {
			MysqlDBCP.release(conn, null, null);
		}
		return result;
	}

	public static int delete(String sql) {
		Connection conn = null;
		int lineNum = 0;
		try {
			conn = MysqlDBCP.getConnection();
			Statement stmt = conn.createStatement();
			lineNum = stmt.executeUpdate(sql);
			stmt.close();
		} catch (SQLException e) {
			logger.error(e.getMessage(), e);
		}
		return lineNum;
	}

	// 获取当前表的关键字，并以字符串数组的形式返回：如“username”，“id“等
	private static String[] getFrame(String tableName) {
		Connection conn = null;
		String[] result = new String[100];
		try {
			conn = MysqlDBCP.getConnection();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("show columns from " + tableName);
			int i = 0;
			while (rs.next()) {
				result[i++] = rs.getString(1);
			}
			result[i] = "#";
			stmt.close();
		} catch (SQLException e) {
			logger.error(e.getMessage(), e);
		} finally {
			MysqlDBCP.release(conn, null, null);
		}
		return result;
	}

	public static Integer existsData(String pid, String vid, String orderNo) {
		//TODO 按照 患者号，住院次，医嘱号来识别是否为是否数据重复
		String sqlString = "select count(*) as cnt from (select t.pk_event_id "
				+ "from adr_monitor_event t join adr_monitor_labinfo t2 on t.pk_event_id = t2.pk_event_id "
				+ "where t.patient_id='" + pid + "' and t.visit_id='" + vid + "' and t2.order_no='" + orderNo + "' ) a";
		List<Map<String, String>> list = query(sqlString, new String[] { "cnt" });
		Integer countLong = Integer.parseInt(list.get(0).get("cnt"));
		return countLong.intValue();
	}

	public static String insertAdrMonitorEvent(Map<String, String> event) {
		String resultString = "0";
		if (event == null) {
			return resultString;
		}
		String sqlString = "insert into adr_monitor_event(event_name,patient_id,visit_id,visit_type,person_name,sex,dept_code,dept_name,adt_time,diag,monitor_time,status) values ("
				+ "'"
				+ event.get("event_name")
				+ "','"
				+ event.get("patient_id")
				+ "','"
				+ event.get("visit_id")
				+ "','"
				+ event.get("visit_type")
				+ "','"
				+ event.get("person_name")
				+ "','"
				+ event.get("sex")
				+ "','"
				+ event.get("dept_code")
				+ "','"
				+ event.get("dept_name")
				+ "','"
				+ event.get("adt_time")
				+ "','"
				+ event.get("diag") + "','" + event.get("monitor_time") + "','" + event.get("status") + "')";
		resultString = insert(sqlString);
		System.out.println("insert event:" + sqlString);
		return resultString;
	}

	public static String insertAdrMonitorLabInfo(Map<String, String> labinfo) {
		String resultString = "0";
		if (labinfo == null) {
			return resultString;
		}
		String sqlString = "insert into adr_monitor_labinfo(pk_event_id,order_no,order_item_code,order_item_name,lab_sub_item_code,lab_sub_item_name,lab_result_value,lab_result_unit) values ("
				+ "'"
				+ labinfo.get("pk_event_id")
				+ "','"
				+ labinfo.get("order_no")
				+ "','"
				+ labinfo.get("order_item_code")
				+ "','"
				+ labinfo.get("order_item_name")
				+ "','"
				+ labinfo.get("lab_sub_item_code")
				+ "','"
				+ labinfo.get("lab_sub_item_name")
				+ "','"
				+ labinfo.get("lab_result_value") + "','" + labinfo.get("lab_result_unit") + "')";
		resultString = insert(sqlString);

		return resultString;
	}

	public static String insertAdrMonitorLabInfoDrug(Map<String, String> drug) {
		String resultString = "0";
		if (drug == null) {
			return resultString;
		}
		String sqlString = "insert into adr_monitor_drug(pk_event_id,order_no,drug_code,drug_type,drug_name,common_name,license_no,manufacturer,adr_usage,medication_start_end_time,medication_start_time,medication_end_time) values ("
				+ "'"
				+ drug.get("pk_event_id")
				+ "','"
				+ drug.get("order_no")
				+ "','"
				+ drug.get("drug_code")
				+ "','"
				+ drug.get("drug_type")
				+ "','"
				+ drug.get("drug_name")
				+ "','"
				+ drug.get("common_name")
				+ "','"
				+ drug.get("license_no")
				+ "','"
				+ drug.get("manufacturer")
				+ "','"
				+ drug.get("adr_usage")
				+ "','"
				+ drug.get("medication_start_end_time")
				+ "','"
				+ drug.get("medication_start_time")
				+ "','"
				+ drug.get("medication_end_time") + "')";
		resultString = insert(sqlString);

		return resultString;
	}
}
