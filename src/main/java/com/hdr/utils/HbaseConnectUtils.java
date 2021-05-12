package com.hdr.utils;

import com.goodwill.core.orm.MatchType;
import com.goodwill.core.orm.PropertyFilter;
import com.goodwill.core.utils.PropertiesUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

public class HbaseConnectUtils {
	protected static Logger logger = LoggerFactory.getLogger(HbaseConnectUtils.class);

	private static final String CONFIG_FILE_NAME = "hbase.properties";
	private static final String HBASE_ZK_LIST = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME, "hbase.zk.host");
	private static final String HBASE_ROOTDIR = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME, "hbase.rootdir");
	private static final String POOL_MAX_TOTAL = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME, "pool.max.total");
	private static final String default_family_name = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME,
			"default_column_family");
	private static final String org_oid = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME, "org_oid");
	private static final String PATIENT_ID_START = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME,
			"patient_id.start");
	private static final String PATIENT_ID_END = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME, "patient_id.end");

	//Free pool capacity
	//生产环境修改为100
	private static int FREE_POOL_SIZE = Integer.valueOf(POOL_MAX_TOTAL);
	private static int patient_id_start = Integer.valueOf(PATIENT_ID_START);
	private static int patient_id_end = Integer.valueOf(PATIENT_ID_END);
	//Pool owned by class
	private static HConnection[] factory = null;
	private static int countFree;
	private static Configuration conf;
	private static Admin admin = null;

	public static String ROWKEY = "ROWKEY";

	public static synchronized HConnection getInstance() {
		HConnection result = null;
		if (countFree == 0) {
			try {
				//连接池一次获取连接数量
				factory = new HConnection[FREE_POOL_SIZE];
				for (int i = 0; i < FREE_POOL_SIZE; i++) {
					conf = getConfigurationTY();//通用配置
					factory[countFree++] = HConnectionManager.createConnection(conf);
				}
				result = factory[--countFree];

			} catch (IOException e) {
				logger.error("Hbase连接池初始化失败!请检查Zookeeper等配置是否正确。", e);
			}
		} else {
			result = factory[--countFree];
		}
		return result;
	}

	/**
	 * @Description
	 * 方法描述:通用方法，从配置文件中获取配置信息对象
	 * @return 返回类型： Configuration
	 * @return
	 */
	public static Configuration getConfigurationTY() {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", HBASE_ZK_LIST);
		conf.set("hbase.rootdir", HBASE_ROOTDIR);
		// 提高RPC通信时长
		conf.setLong("hbase.rpc.timeout", 600000);
		// 设置Scan缓存
		conf.setLong("hbase.client.scanner.caching", 1000);
		return conf;
	}

	private static Configuration getConfiguration() {
		Configuration configuration = HBaseConfiguration.create();
		// 提高RPC通信时长
		configuration.setLong("hbase.rpc.timeout", 600000);
		// 设置Scan缓存
		configuration.setLong("hbase.client.scanner.caching", 1000);
		return configuration;
	}

	public static synchronized void freeInstance(HConnection conn) {
		if (countFree < FREE_POOL_SIZE)
			factory[countFree++] = conn;
	}

	/**
	 * @Description
	 * 方法描述: 创建HTable
	 * @return 返回类型： HTableInterface
	 * @param tableName
	 * @param connection
	 * @return
	 */
	public static HTableInterface getHTable(String tableName) {

		HTableInterface t = null;
		HConnection connection = getInstance();
		try {
			t = connection.getTable(tableName);
		} catch (Exception ex) {
			logger.error("创建HTable发生错误，无法连接HBase！", ex);
		} finally {
			freeInstance(connection);
		}
		return t;
	}

	public static Admin getAdmin() {

		HConnection connection = getInstance();
		try {
			if (admin == null) {
				admin = connection.getAdmin();
			}
		} catch (Exception ex) {
			logger.error("创建HTable发生错误，无法连接HBase！", ex);
		} finally {
			freeInstance(connection);
		}

		return admin;
	}

	public static void releaseTable(HTableInterface table) {
		try {
			if (table == null) {
				return;
			}
			table.close();
		} catch (IOException ex) {
			logger.error("释放HTable发生错误，无法连接HBase！", ex);
		}
	}

	public static int getCountFree() {
		return countFree;
	}

	/**
	 * @Description 鏂规硶鎻忚堪:鏍规嵁浼犲叆patient_id鐢熸垚rowkey鍓嶇紑
	 * @return 杩斿洖绫诲瀷锛�String
	 * @param patient_id
	 * @return
	 */
	public static String getRowkeyPrefix(String patient_id) {
		return getRowkeyPrefix(patient_id, getOrg_oid());
	}

	public static String getOrg_oid() {
		return org_oid;
	}

	public static String getDefaultFamilyName() {
		return default_family_name;
	}

	/**
	 * @Description 鏂规硶鎻忚堪: 鏍规嵁浼犲叆patient_id鐢熸垚rowkey鍓嶇紑
	 * @return 杩斿洖绫诲瀷锛�String
	 * @param patient_id
	 * @param oid
	 * @return
	 */
	public static String getRowkeyPrefix(String patient_id, String oid) {
		String salt = getSalt(patient_id);
		return salt + "|" + oid + "|" + patient_id;
	}

	/**
	 * 鐢熸垚鏁ｅ垪閿�鍓嶇紑
	 * */
	private static String getSalt(String patient_id) {
		String salt;
		int patient_id_start_2 = -1;
		int patient_id_end_2 = -1;
		if (patient_id_start == -1000) {
			patient_id_start = Integer.parseInt(PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME, "patient_id.start"));

		}
		if (patient_id_end == -1000) {
			patient_id_end = Integer.parseInt(PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME, "patient_id.end"));

		}
		if (patient_id_start < 0) {
			patient_id_start_2 = patient_id.length() + patient_id_start;
		}
		if (patient_id_end <= 0) {
			patient_id_end_2 = patient_id.length() + patient_id_end;
		}

		if (patient_id_end > patient_id.length() || patient_id_end_2 > patient_id.length()) {
			return "zzzz";
		}
		if (patient_id_start_2 >= 0 && patient_id_end_2 > 0) {
			StringBuffer sb = new StringBuffer(patient_id.substring(patient_id_start_2, patient_id_end_2));
			salt = sb.reverse().toString();
			return salt;
		} else {
			StringBuffer sb = new StringBuffer(patient_id.substring(patient_id_start, patient_id_end));
			salt = sb.reverse().toString();
			return salt;
		}
	}

	/**
	 *益都中心测试
	 * @return
	 */
	private static Configuration getConfigurationYDZX() {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "node6,node4,node5");
		conf.set("hbase.rootdir", "hdfs://nameservice1/hbase");
		// 提高RPC通信时长
		conf.setLong("hbase.rpc.timeout", 600000);
		// 设置Scan缓存
		conf.setLong("hbase.client.scanner.caching", 1000);
		return conf;
	}

	private static Configuration getConfigurationNTFY() {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "10.0.4.77,10.0.4.80,10.0.4.81");
		conf.set("hbase.rootdir", "hdfs://nameservice1/hbase");
		// 提高RPC通信时长
		conf.setLong("hbase.rpc.timeout", 600000);
		// 设置Scan缓存
		conf.setLong("hbase.client.scanner.caching", 1000);
		return conf;
	}

	/**
	 * @Description
	 * 方法描述:获取配置信息对象 北医三院
	 * @return 返回类型： Configuration
	 * @return
	 */
	private static Configuration getConfigurationHS() {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "172.16.80.188,172.16.80.189,172.16.80.190,172.16.80.191,172.16.80.192");
		conf.set("hbase.rootdir", "hdfs://cdr08:8020/hbase");
		// 提高RPC通信时长
		conf.setLong("hbase.rpc.timeout", 600000);
		// 设置Scan缓存
		conf.setLong("hbase.client.scanner.caching", 1000);
		return conf;
	}

	/**
	 * @Description
	 * 方法描述:获取配置信息对象 北医三院
	 * @return 返回类型： Configuration
	 * @return
	 */
	public static Configuration getConfigurationBYSY() {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "hadoop03,hadoop04,hadoop06,hadoop01,hadoop02");
		conf.set("hbase.rootdir", "hdfs://nameservice1/hbase");
		// 提高RPC通信时长
		conf.setLong("hbase.rpc.timeout", 600000);
		// 设置Scan缓存
		conf.setLong("hbase.client.scanner.caching", 1000);
		return conf;
	}

	/**
	 * @Description
	 * 方法描述:获取配置信息对象 北医三院新集群
	 * @return 返回类型： Configuration
	 * @return
	 */
	public static Configuration getConfigurationBYSYNew() {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "10.2.3.71,10.2.3.72,10.2.3.73,10.2.3.74,10.2.3.75");
		conf.set("hbase.rootdir", "hdfs://bysycluster2/hbase");
		// 提高RPC通信时长
		conf.setLong("hbase.rpc.timeout", 600000);
		// 设置Scan缓存
		conf.setLong("hbase.client.scanner.caching", 1000);
		return conf;
	}

	/**
	 * @Description
	 * 方法描述:获取配置信息对象 杭州妇保
	 * @return 返回类型： Configuration
	 * @return
	 */
	private static Configuration getConfigurationHZFB() {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "hadoop05,hadoop04,hadoop03");
		conf.set("hbase.rootdir", "hdfs://nameservice1/hbase");
		// 提高RPC通信时长
		conf.setLong("hbase.rpc.timeout", 600000);
		// 设置Scan缓存
		conf.setLong("hbase.client.scanner.caching", 1000);
		return conf;
	}

	/**
	 * @Description
	 * 方法描述: 先根据rowkey前缀进行过滤，然后按照condition条件进行筛选数据
	 * propertyFilter的matchtype支持：=,>,>=,<,<=,like,nlike,<>,in,nin(not in),sw(startWith),ew(endWith)
	 * @param tablename 表名称
	 * @param rowkeyprefix rowkey前缀
	 * @param filters 过滤条件 PorpertyFilter的List
	 * @param columns 需要输出的列名List
	 * @return 返回值 list 存放键值对map
	 */
	public static List<Map<String, String>> findByCondition(String tablename, String rowkeyprefix,
			List<PropertyFilter> filters, final String... columns) {

		byte[] startRow = (rowkeyprefix).getBytes(getCharset());
		byte[] stopRow = (rowkeyprefix + "}").getBytes(getCharset());
		HTableInterface htable = null;
		// Filter filter = new PrefixFilter(Bytes.toBytes(rowkeyprefix));
		Scan scan = new Scan();
		scan.setStartRow(startRow);
		scan.setStopRow(stopRow);

		for (String column : columns) {
			scan.addColumn(Bytes.toBytes(getDefaultFamilyName()), column.getBytes(getCharset()));
		}
		//查询条件添加到返回列中
		if (columns != null && columns.length > 0) {
			for (PropertyFilter filter : filters) {
				scan.addColumn(Bytes.toBytes(getDefaultFamilyName()), filter.getPropertyName().getBytes(getCharset()));
			}
		}
		//过滤条件
		if (filters != null && filters.size() > 0) {
			scan.setFilter(genHbaseFilters(filters));
		}

		ResultScanner scanner = null;
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		try {
			htable = getHTable(tablename);
			scanner = htable.getScanner(scan);
			for (Result result : scanner) {
				Map<String, String> map = new HashMap<String, String>();
				map.put(ROWKEY, new String(result.getRow()));
				for (Cell cell : result.rawCells()) {
					map.put(new String(CellUtil.cloneQualifier(cell)),
							new String(CellUtil.cloneValue(cell), getCharset()));
				}
				list.add(map);
			}
		} catch (IOException e) {
			logger.error("通过htable获取Scan发生IO错误!", e);
		} finally {
			scan = null;
			if (scanner != null)
				scanner.close();
			releaseTable(htable);
		}
		return list;
	}

	/**
	 * @Description
	 * 方法描述:传入rowkey进行get操作
	 * @return 返回类型： Map<String,String>
	 * @param tableName
	 * @param rowKey
	 * @param columns
	 * @return
	 */
	public static Map<String, String> get(String tableName, String rowKey, final String... columns) {
		Get get = new Get(Bytes.toBytes(rowKey));
		Map<String, String> map = new HashMap<String, String>();
		HTableInterface htable = null;
		try {
			htable = getHTable(tableName);
			Result result = htable.get(get);
			for (Cell cell : result.rawCells()) {
				map.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell), getCharset()));
			}
		} catch (IOException e) {
			logger.error("通过htable获取Scan发生IO错误!", e);
		} finally {
			get = null;
			releaseTable(htable);
		}
		return map;
	}

	/**
	 * @Description 鏂规硶鎻忚堪: 鎻掑叆涓�鏁版嵁
	 * @return 杩斿洖绫诲瀷锛�void
	 * @param tableName
	 * @param familyName
	 * @param rowkey
	 *            涓婚敭
	 * @param mapValue
	 *            鍏蜂綋鐨勯敭鍊煎锛�鍒楀悕绉�鍊�
	 */
	public static void put(String tableName, String familyName, String rowkey, Map<String, String> mapValue) {
		HTableInterface htable = null;
		try {
			htable = getHTable(tableName);
			// htable.setAutoFlush(false, false);
			// htable.setWriteBufferSize(1024 * 1024 * 20);
			Put put = map2Put(familyName, rowkey, mapValue);
			htable.put(put);
		} catch (IOException e) {
			logger.error("閫氳繃htable鍐欏叆缂撳瓨鍙戠敓IO閿欒!", e);
			// msg = e.getMessage();
		} finally {
			releaseTable(htable);
		}
	}

	/**
	 * 鍩轰簬浼犲叆鐨勫垪鏃忥紝rowkey鐨凪ap 鏁版嵁鐨凪ap 鐢熸垚涓�釜Put
	 * */
	private static Put map2Put(String columnFamily, String rowkey, Map<String, String> dataMap) {
		Iterator<Map.Entry<String, String>> it = dataMap.entrySet().iterator();
		Put put = new Put(rowkey.getBytes(getCharset()));
		put.setDurability(Durability.SYNC_WAL);
		while (it.hasNext()) {
			Map.Entry<String, String> entry = (Map.Entry<String, String>) it.next();
			String key = entry.getKey();
			String value = entry.getValue();
			if (!ROWKEY.equals(key) && value != null) {
				put.add(columnFamily.getBytes(getCharset()), key.getBytes(getCharset()), value.getBytes(getCharset()));
			}
		}
		return put;
	}

	static public Charset getCharset() {
		return Charset.forName("UTF-8");
	}

	/**
	 * @Description 鏂规硶鎻忚堪:鏍规嵁propertyFilter杞崲涓篐base Filter
	 * @return 杩斿洖绫诲瀷锛�FilterList
	 * @param filters
	 * @return
	 */
	public static FilterList genHbaseFilters(List<PropertyFilter> filters) {
		List<Filter> listFileters = new ArrayList<Filter>();
		for (PropertyFilter filter : filters) {
			String matchtype = filter.getMatchType();
			CompareOp op = null;
			if (MatchType.EQ.getOperation().equals(matchtype)) {
				op = CompareOp.EQUAL;
			} else if (MatchType.GT.getOperation().equals(matchtype)) {
				op = CompareOp.GREATER;
			} else if (MatchType.GE.getOperation().equals(matchtype)) {
				op = CompareOp.GREATER_OR_EQUAL;
			} else if (MatchType.LT.getOperation().equals(matchtype)) {
				op = CompareOp.LESS;
			} else if (MatchType.LE.getOperation().equals(matchtype)) {
				op = CompareOp.LESS_OR_EQUAL;
			} else if (MatchType.NE.getOperation().equals(matchtype)) {
				op = CompareOp.NOT_EQUAL;
			} else if (MatchType.NE.getOperation().equals(matchtype)) {
				op = CompareOp.NOT_EQUAL;
			}
			// //ISNULL
			// else if (MatchType.ISNULL.getOperation().equals(matchtype)) {
			// op = CompareFilter.CompareOp.EQUAL;
			// SingleColumnValueFilter columnValueFilter = new
			// SingleColumnValueFilter(
			// Bytes.toBytes(getDefaultFamilyName()),
			// Bytes.toBytes(filter.getPropertyName()), op,
			// Bytes.toBytes(""));
			// //Bytes.toBytes("0")涔熻兘浠ｈ〃绌�
			// columnValueFilter.setFilterIfMissing(true);
			// listFileters.add(columnValueFilter);
			// } else if (MatchType.NOTNULL.getOperation().equals(matchtype)) {
			// op = CompareFilter.CompareOp.NOT_EQUAL;
			// SingleColumnValueFilter columnValueFilter = new
			// SingleColumnValueFilter(
			// Bytes.toBytes(getDefaultFamilyName()),
			// Bytes.toBytes(filter.getPropertyName()), op,
			// new NullComparator());
			// columnValueFilter.setFilterIfMissing(true);
			// listFileters.add(columnValueFilter);
			// }
			// LIKE浣跨敤姝ｅ垯琛ㄨ揪寮忓尮閰�
			if (MatchType.LIKE.getOperation().equals(matchtype)) {
				op = CompareOp.EQUAL;
				SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(
						Bytes.toBytes(getDefaultFamilyName()), Bytes.toBytes(filter.getPropertyName()), op,
						new RegexStringComparator(".*" + filter.getPropertyValue().toString() + ".*"));
				columnValueFilter.setFilterIfMissing(true);
				listFileters.add(columnValueFilter);
			} else if (MatchType.NOTLIKE.getOperation().equals(matchtype)) {
				op = CompareOp.NOT_EQUAL;
				SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(
						Bytes.toBytes(getDefaultFamilyName()), Bytes.toBytes(filter.getPropertyName()), op,
						new RegexStringComparator(".*" + filter.getPropertyValue().toString() + ".*"));
				columnValueFilter.setFilterIfMissing(true);
				listFileters.add(columnValueFilter);
			}

			// INLIKE浣跨敤姝ｅ垯琛ㄨ揪寮忓尮閰�
			else if (MatchType.INLIKE.getOperation().equals(matchtype)) {
				op = CompareOp.EQUAL;
				String[] split = filter.getPropertyValue().toString().split(",");
				String regx = "";
				for (int i = 0; i < split.length; i++) {
					if (i > 0) {
						regx += "|";
					}
					regx += "(?:" + split[i] + ")";
				}
				SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(
						Bytes.toBytes(getDefaultFamilyName()), Bytes.toBytes(filter.getPropertyName()), op,
						new RegexStringComparator(regx));
				columnValueFilter.setFilterIfMissing(true);
				listFileters.add(columnValueFilter);
			}

			// IN浣跨敤姝ｅ垯琛ㄨ揪寮忓尮閰�
			else if (MatchType.IN.getOperation().equals(matchtype)) {
				op = CompareOp.EQUAL;
				String[] split = filter.getPropertyValue().toString().split(",");
				String regx = "";
				for (int i = 0; i < split.length; i++) {
					if (i > 0) {
						regx += "|";
					}
					regx += "(^" + split[i] + "$)";
				}
				SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(
						Bytes.toBytes(getDefaultFamilyName()), Bytes.toBytes(filter.getPropertyName()), op,
						new RegexStringComparator(regx));
				columnValueFilter.setFilterIfMissing(true);
				listFileters.add(columnValueFilter);
			}
			// NotIN浣跨敤姝ｅ垯琛ㄨ揪寮忓尮閰�
			else if (MatchType.NOTIN.getOperation().equals(matchtype)) {
				op = CompareOp.NOT_EQUAL;
				String[] split = filter.getPropertyValue().toString().split(",");
				String regx = "";
				for (int i = 0; i < split.length; i++) {
					if (i > 0) {
						regx += "|";
					}
					regx += "(^" + split[i] + "$)";
				}
				SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(
						Bytes.toBytes(getDefaultFamilyName()), Bytes.toBytes(filter.getPropertyName()), op,
						new RegexStringComparator(regx));
				columnValueFilter.setFilterIfMissing(true);
				listFileters.add(columnValueFilter);
			}
			// STARTWITH浣跨敤姝ｅ垯琛ㄨ揪寮忓尮閰�
			else if (MatchType.STARTWITH.getOperation().equals(matchtype)) {
				op = CompareOp.EQUAL;
				String[] split = filter.getPropertyValue().toString().split(",");
				String regx = "(";
				for (int i = 0; i < split.length; i++) {
					if (i > 0) {
						regx += "|";
					}
					regx += split[i];
				}
				regx += ")";
				SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(
						Bytes.toBytes(getDefaultFamilyName()), Bytes.toBytes(filter.getPropertyName()), op,
						new RegexStringComparator("^" + regx + ".*"));
				columnValueFilter.setFilterIfMissing(true);
				listFileters.add(columnValueFilter);
			}
			// ENDWITH浣跨敤姝ｅ垯琛ㄨ揪寮忓尮閰�
			else if (MatchType.ENDWITH.getOperation().equals(matchtype)) {
				op = CompareOp.EQUAL;
				String[] split = filter.getPropertyValue().toString().split(",");
				String regx = "(";
				for (int i = 0; i < split.length; i++) {
					if (i > 0) {
						regx += "|";
					}
					regx += split[i];
				}
				regx += ")";
				SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(
						Bytes.toBytes(getDefaultFamilyName()), Bytes.toBytes(filter.getPropertyName()), op,
						new RegexStringComparator(".*" + regx + "$"));
				columnValueFilter.setFilterIfMissing(true);
				listFileters.add(columnValueFilter);
			} else {
				SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(
						Bytes.toBytes(getDefaultFamilyName()), Bytes.toBytes(filter.getPropertyName()), op,
						Bytes.toBytes(filter.getPropertyValue().toString()));

				columnValueFilter.setFilterIfMissing(true);
				listFileters.add(columnValueFilter);
			}
		}
		return new FilterList(listFileters);
	}

	/**
	 * @Description
	 * 方法描述: 判断表是否存在
	 * @return 返回类型： boolean
	 * @param tableName
	 * @return
	 */
	public static boolean getTableExists(String tableName) {
		TableName tn = TableName.valueOf(tableName);
		boolean isExists = false;
		try {
			getAdmin();
			isExists = admin.tableExists(tn);

		} catch (IOException e) {
			logger.error("获取HRegionInf报错", e);
		}
		return isExists;
	}

	/**
	 * @Description
	 * 方法描述:获取表的region信息
	 * @return 返回类型： Map<String,String>
	 * @param tableName
	 * @return
	 */
	public static List<HRegionInfo> getRegionInfo(String tableName) {

		TableName tn = TableName.valueOf(tableName);
		List<HRegionInfo> regionList = new ArrayList<HRegionInfo>();
		try {
			getAdmin();
			regionList = admin.getTableRegions(tn);
			//			System.out.println("List<HRegionInfo> size:" + regionList.size());
			//			for (HRegionInfo hri : regionList) {
			//				System.out.println(hri);
			//				System.out.println("----------------");
			//			}

		} catch (IOException e) {
			logger.error("获取HRegionInf报错", e);
		}
		return regionList;
	}

	/**
	 * @Description
	 * 方法描述: 创建Filter
	 * @return 返回类型： SingleColumnValueFilter
	 * @param columnName
	 * @param value
	 * @param oper
	 * @param list1
	 * @return
	 */
	public static SingleColumnValueFilter createStringValueFilter(String columnName, String value,
			CompareOp oper, List<Filter> list1) {
		SingleColumnValueFilter ft = new SingleColumnValueFilter(Bytes.toBytes(default_family_name),
				Bytes.toBytes(columnName), oper, Bytes.toBytes(value));
		ft.setFilterIfMissing(true);
		list1.add(ft);

		return ft;
	}

	/**
	 * 字符查询传转换为filter查询对象
	 * @param filterString 查询字符串 column|in|080,079,004,035,088,134
	 * @param filters 
	 * @param strSplit 分隔每个查询条件的分隔符
	 */
	public static void strToFilter(String filterString, List<PropertyFilter> filters, String strSplit) {
		//将配置字符串转换为查询
		if (StringUtils.isNotBlank(filterString)) {
			String[] filterStrings = filterString.split(strSplit);
			for (String filterItemString : filterStrings) {
				String[] tempString = filterItemString.split("\\|");
				createPropertyFilter(tempString[0], tempString[2], tempString[1], filters);
			}
		}
	}

	/**
	 * 创建Filter
	 * @param columnName
	 * @param keyword
	 * @param filters
	 */
	public static void createPropertyFilter(String columnName, String keyword, String MatchType,
			List<PropertyFilter> filters) {
		if (StringUtils.isNotBlank(keyword)) {
			PropertyFilter filter1 = new PropertyFilter();
			filter1.setMatchType(MatchType);
			filter1.setPropertyName(columnName);
			filter1.setPropertyValue(keyword);
			filters.add(filter1);
		}
	}

	//	public static void main(String[] args) {
	//		getRegionInfo("TMP_CMR_HDR_INP_SUMMARY_ICUAA");
	//
	//	}

}
