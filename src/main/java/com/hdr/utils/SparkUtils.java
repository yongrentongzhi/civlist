package com.hdr.utils;

import com.goodwill.core.orm.MatchType;
import com.goodwill.core.orm.PropertyFilter;
import com.goodwill.hadoop.hbase.HbaseCURDUtils;
import com.goodwill.hdr.cdssutil.hbase.HDRTableConstant;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Description
 * 类描述：Spark工具类 
 * @author malongbiao
 * @Date 2015年10月30日
 * @modify
 * 修改记录：
 * 
 */
public class SparkUtils {
	private static final Log logger = LogFactory.getLog(SparkUtils.class);

	/**
	 * @Description
	 * 方法描述:获取当前日期的年月 ‘yyyy-MM’格式
	 * @return 返回类型： String
	 * @return
	 */
	public static String getNowYearMonth() {
		Calendar cal = Calendar.getInstance();
		int month = cal.get(Calendar.MONTH) + 1;
		String monthStr = "";
		if (month < 10) {
			monthStr = "0" + month;
		} else {
			monthStr = "" + month;
		}
		return cal.get(Calendar.YEAR) + "-" + monthStr;
	}

	public static JavaSparkContext getSparkJavaContext(String appName, boolean onYarn) {
		if (onYarn) {
			return getSparkJavaContextYarn(appName, new String[] { "/home/hdrbi/spark/hdr-parallel-1.0-SNAPSHOT.jar"
					//					,"/home/hbase/oozie-client-4.0.0-cdh5.2.0.jar" 
			});
		} else {
			return getSparkJavaContextLocal(appName, 1, 1);
		}
	}

	/**
	 * 获取JavaSparkContext的对象，如果是在Yarn集群上运行，onYarn为true，在本地local模式，onYarn为false
	 * @param appName
	 * @param jars 运行的jar路径数组
	 * @return
	 */
	public static JavaSparkContext getSparkJavaContextYarn(String appName, String[] jars) {
		//生成SparkContext对象
		SparkConf sparkConf = new SparkConf().setAppName(appName);

		/*
		Spark yarn部署模式  在命令行workspace/hdr-parallel目录下执行 mvn package -P spark ，然后将打的jar包放到集群的 /home/hbase目录下执行下面的命令
		spark-submit --class com.goodwill.hdr.parallel.pvis.SparkVisitSplit --master yarn --deploy-mode client --executor-memory 4g 【注：这里没有换行，放不开了】
		--driver-memory 4g --num-executors 12 --executor-cores 4 hdr-parallel-1.0.1-SNAPSHOT.jar
		 */

		//sparkConf.setJars(jars);
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

		return new JavaSparkContext(sparkConf);
	}

	/**
	 * @Description
	 * 方法描述:获取本地JavaSparkContext的对象
	 * @return 返回类型： JavaSparkContext
	 * @param appName 应用名称
	 * @param thread  线程数
	 * @param memory 内存数
	 * @return
	 */
	public static JavaSparkContext getSparkJavaContextLocal(String appName, int thread, int memory) {
		//生成SparkContext对象
		SparkConf sparkConf = new SparkConf().setAppName(appName);
		sparkConf.setMaster("local[" + thread + "]");
		sparkConf.set("spark.executor.memory", memory + "g");
		return new JavaSparkContext(sparkConf);
	}

	/**
	 * 得到指定类型患者标识对应的字段集合
	 * @Description
	 * 方法描述:根据传入的患者标识类型，返回此类型对应的保存字段的集合，用于取出此类数据的值
	 * @return 返回类型： String[]
	 * @param patientIdentifier
	 * @return
	 */
	public static String[] getPatientIdentifierFields(String patientIdentifier) {
		String[] result = null;
		if ("PATIENT_IDENTIFIER".equals(patientIdentifier)) {
			result = new String[] { "IN_PATIENT_ID", "OUT_PATIENT_ID", "PATIENT_ID", "PATIENT_IDENTIFIER" };//患者使用就医卡指定的患者号 存储的列名
		} else if ("HEALTH_CARD_NO".equals(patientIdentifier)) {
			result = new String[] { "HEALTH_CARD_NO" };
		} else if ("CASE_NO".equals(patientIdentifier)) {
			result = new String[] { "CASE_NO" };
		} else if ("HOSPTIAL_NO".equals(patientIdentifier)) {
			result = new String[] { "INP_NO", "OUTP_NO" };
		}
		return result;
	}

	/**
	 * 从传入的数组字段中，依次获取值  找到值就返回
	 * @param result
	 * @return
	 */
	public static String getOneValueFromResult(Result result, String[] targetFields) {
		String target_value = null;
		for (String field : targetFields) {
			byte[] bytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(field));
			if (bytes != null && bytes.length != 0) {
				target_value = Bytes.toString(bytes);
				break;
			}
		}
		return target_value;
	}

	/**
	 * 获取存储到Hbase表的conf对象
	 * @param tableName
	 * @return
	 */
	public static Configuration getHbaseConfiguration(String tableName) {
		//hbase配置从hbase.properties中读取。
		Configuration configuration = HbaseConnectUtils.getConfigurationTY();
		// 提高RPC通信时长；MapReduce任务时长，客户端client超时时长
		configuration.setLong("hbase.rpc.timeout", 600000);
		configuration.setLong("mapreduce.task.timeout", 1200000);
		configuration.setLong("hbase.client.scanner.timeout.period", 600000);
		// 设置Scan缓存
		configuration.setLong("hbase.client.scanner.caching", 100);
		configuration.set(TableInputFormat.INPUT_TABLE, tableName);
		configuration.setClass("mapreduce.outputformat.class", TableOutputFormat.class, OutputFormat.class);
		configuration.set(TableOutputFormat.OUTPUT_TABLE, tableName);
		return configuration;
	}

	/**
	 * @Description
	 * 方法描述:传入访问方式编码，找到对应的访问方式
	 * @return 返回类型： String
	 * @param visit_type_code
	 * @return
	 */
	public static String getVisitTypeByCode(String visit_type_code) {
		String visit_type;
		if (HDRTableConstant.VISIT_TYPE_OUTPV_CODE.equals(visit_type_code)) {
			visit_type = HDRTableConstant.VISIT_TYPE_OUTPV;
		} else if (HDRTableConstant.VISIT_TYPE_INPV_CODE.equals(visit_type_code)) {
			visit_type = HDRTableConstant.VISIT_TYPE_INPV;
		} else if (HDRTableConstant.VISIT_TYPE_PHY_CODE.equals(visit_type_code)) {
			visit_type = HDRTableConstant.VISIT_TYPE_PHY;
		} else {
			visit_type = HDRTableConstant.VISIT_TYPE_OTHER;
		}
		return visit_type;
	}

	/**
	 * @Description
	 * 方法描述:根据传入的访问方式，返回后续需要的访问方式的Map--如果是如“住院医嘱”类似的，确定访问方式的，可以少取一个字段到内存
	 * @param visitTypeField：访问方式
	 * @param partDeal： 是否进行过滤，如果传入true，则访问编码字段必须返回
	 * @return 返回类型： Map<String,String>
	 */
	public static Map<String, String> getVisitTypeMapping(String visitTypeField, boolean partDeal) {
		Map<String, String> visitType = new HashMap<String, String>();
		//传入visit_type,则一定不需要过滤，并且需要判断visit_type
		if (HDRTableConstant.VISIT_TYPE.equals(visitTypeField)) {
			visitType.put(HDRTableConstant.VISIT_TYPE_FIELD, "VISIT_TYPE_CODE");
			visitType.put(HDRTableConstant.VISIT_TYPE_OUTPV_CODE, HDRTableConstant.VISIT_TYPE_OUTPV); //01 门诊
			visitType.put(HDRTableConstant.VISIT_TYPE_INPV_CODE, HDRTableConstant.VISIT_TYPE_INPV); //02 住院
			visitType.put(HDRTableConstant.VISIT_TYPE_PHY_CODE, HDRTableConstant.VISIT_TYPE_PHY); //03 体检
			visitType.put(HDRTableConstant.VISIT_TYPE_OTHER_CODE, HDRTableConstant.VISIT_TYPE_OTHER); //09其他
			return visitType;
		}
		//不是visit_type情况下，如果需要过滤，首先把过滤字段放入Map中
		if (partDeal) {
			visitType.put(HDRTableConstant.VISIT_TYPE_FIELD, "VISIT_TYPE_CODE");
		}
		//按需放入访问方式和过滤字段
		if (HDRTableConstant.VISIT_TYPE_INPV.equals(visitTypeField)) {
			visitType.put(HDRTableConstant.VISIT_TYPE, HDRTableConstant.VISIT_TYPE_INPV);
			if (partDeal) {
				visitType.put(HDRTableConstant.VISIT_TYPE_FILTER_VALUE, HDRTableConstant.VISIT_TYPE_INPV_CODE);
			}
		} else if (HDRTableConstant.VISIT_TYPE_OUTPV.equals(visitTypeField)) {
			visitType.put(HDRTableConstant.VISIT_TYPE, HDRTableConstant.VISIT_TYPE_OUTPV);
			if (partDeal) {
				visitType.put(HDRTableConstant.VISIT_TYPE_FILTER_VALUE, HDRTableConstant.VISIT_TYPE_OUTPV_CODE);
			}
		}
		return visitType;
	}

	/**
	 * 通过传入的访问方式，查找对应的编码
	 */
	public static String getVisitTypeCode(String visitTypeField) {
		if (HDRTableConstant.VISIT_TYPE_INPV.equals(visitTypeField)) {
			return HDRTableConstant.VISIT_TYPE_INPV_CODE; //02 住院

		} else if (HDRTableConstant.VISIT_TYPE_OUTPV.equals(visitTypeField)) {
			return HDRTableConstant.VISIT_TYPE_OUTPV_CODE; //01 门诊

		} else if (HDRTableConstant.VISIT_TYPE_PHY.equals(visitTypeField)) {
			return HDRTableConstant.VISIT_TYPE_PHY_CODE; //03 体检

		} else if (HDRTableConstant.VISIT_TYPE_OTHER.equals(visitTypeField)) {
			return HDRTableConstant.VISIT_TYPE_OTHER_CODE; //09其他

		} else {
			return null;
		}
	}

	/**
	 * 从Result中取出字段为Field的值【列族默认为cf】
	 * @param result
	 * @param field
	 * @return
	 */
	public static String getValueFromResult(Result result, String field) {
		if (result == null || StringUtils.isBlank(field)) {
			return null;
		}
		byte[] valueBytes = result.getValue(Bytes.toBytes(HbaseCURDUtils.getDefaultFamilyName()), Bytes.toBytes(field));
		if (valueBytes == null) {
			return null;
		}
		return Bytes.toString(valueBytes);
	}

	/**
	 * 判断value不为空，将value值按name字段存入put中，列族是 默认'cf'
	 * 如果value为空，则不做任何处理
	 * for:如果Put中含有了为空的value，跑批量任务会报错
	 * @param put
	 * @param name
	 * @param value
	 */
	public static int insertIntoPut(Put put, String name, String value) {
		if (StringUtils.isNotBlank(value)) {
			put.addColumn(Bytes.toBytes(HbaseCURDUtils.getDefaultFamilyName()), Bytes.toBytes(name),
					Bytes.toBytes(value));
			return 1;
		}
		return 0;
	}

	/**
	 * @Description
	 * 方法描述:将result中对应feildName字段的二进制值直接放入put中
	 * @return 返回类型： int
	 * @param put
	 * @param result
	 * @param fieldName
	 * @return
	 */
	public static int checkAndPut(Put put, Result result, String fieldName, String saveName) {
		if (StringUtils.isBlank(saveName)) {
			return 0;
		}
		byte[] valueBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(fieldName));
		if (valueBytes != null && StringUtils.isNotBlank(Bytes.toString(valueBytes))) {
			put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(saveName), valueBytes);
		} else {
			return 0;
		}
		return 1;
	}

	/**
	 * 判断mapValue不为空，将mapValue按mapKey字段放入Map中
	 * @param map
	 * @param mapKey
	 * @param mapValue
	 */
	public static void putToMap(Map<String, String> map, String mapKey, String mapValue) {
		if (StringUtils.isNotBlank(mapValue) && StringUtils.isNotBlank(mapKey)) {
			map.put(mapKey, mapValue);
		}
	}

	/**
	 * @Description
	 * 方法描述:将result对应feildName字段的值按saveName存储到infos里面
	 * @return 返回类型： int
	 * @param infos
	 * @param result
	 * @param fieldName
	 * @param saveName
	 * @return
	 */
	public static int checkAndPutToMap(Map<String, String> infos, Result result, String fieldName, String saveName) {
		if (StringUtils.isBlank(saveName)) {
			return 0;
		}
		byte[] valueBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes(fieldName));
		if (valueBytes != null) {
			infos.put(saveName, Bytes.toString(valueBytes));
		} else {
			return 0;
		}
		return 1;
	}

	/**
	 * 从Result中取出fieldName字段的值，如果不为空，按mapKey放入到Map中去
	 * @param result
	 * @param fieldName
	 * @param map
	 * @param mapKey
	 */
	public static void getFieldValueAndPutToMap(Result result, String fieldName, Map<String, String> map,
			String mapKey) {
		if (result == null || StringUtils.isBlank(fieldName)) {
			return;
		}
		byte[] valueBytes = result.getValue(Bytes.toBytes(HbaseCURDUtils.getDefaultFamilyName()),
				Bytes.toBytes(fieldName));
		if (valueBytes == null) {
			return;
		}
		map.put(mapKey, Bytes.toString(valueBytes));
	}

	/**
	 * 从Result中取出fieldName字段的值，如果不为空，按mapKey放入到Map中去
	 * @param result
	 * @param fieldName
	 * @param map
	 * @param mapKey
	 */
	public static void getFieldValueAndPutToMap(Result result, String fieldName, Map<String, String> map) {
		if (result == null || StringUtils.isBlank(fieldName)) {
			return;
		}
		byte[] valueBytes = result.getValue(Bytes.toBytes(HbaseCURDUtils.getDefaultFamilyName()),
				Bytes.toBytes(fieldName));
		if (valueBytes == null) {
			return;
		}
		map.put(fieldName, Bytes.toString(valueBytes));
	}

	public static String getVisitRowKeyPrefix(String eid) {
		String salt;
		int length = eid.length();
		StringBuffer sb = new StringBuffer(eid.substring(length - 4, length));
		salt = sb.reverse().toString();
		return salt + "|" + HbaseCURDUtils.getOrg_oid() + "|" + eid;
	}

	/**
	 * @Description
	 * 方法描述:判断生成visitRowKey的字段全不为空，然后按<br>"患者翻转|OID|患者|访问方式|访问次|EID"<br>方式组装visitRowKey
	 * @return 返回类型： String
	 * @param patient_identifier
	 * @param visit_type
	 * @param visit_id
	 * @param eid
	 * @return
	 * @modify:
	 *   visitRowkey利用EID后四位进行翻转 by: xiehongwei 2016年1月4日
	 */
	public static String getVisitRowKey(String patient_identifier, String visit_type, String visit_id, String eid) {
		if (StringUtils.isBlank(patient_identifier) || StringUtils.isBlank(visit_type) || StringUtils.isBlank(visit_id)
				|| StringUtils.isBlank(eid)) {
			return null;
		}
		return getVisitRowKeyPrefix(eid) + "|" + visit_type + "|" + visit_id + "|" + patient_identifier;
	}

	/**
	 * 从Hbase获取部分数据到内存生成Spark的RDD[一般在需要指定filterIfMissing参数时用此方法]
	 * @Description
	 * 方法描述:传入Hbase的封装好的Filters，从指定表中提取目标数据到内存生成Spark的RDD
	 * @return 返回类型： JavaPairRDD<ImmutableBytesWritable,Result>
	 * @param ctx
	 * @param tableName
	 * @param hbaseFilters
	 * @param columns
	 * @return
	 * @throws IOException
	 */
	public static JavaPairRDD<ImmutableBytesWritable, Result> findFilteredRDDFromHbase(JavaSparkContext ctx,
			String tableName, List<Filter> hbaseFilters, FilterList.Operator filterListOper, final String... columns)
			throws IOException {
		Scan scan = new Scan();
		if (columns.length != 0) {
			//设置只返回关键列
			for (String column : columns) {
				logger.error(column);
				if (column == null) {
					continue;
				}
				scan.addColumn(Bytes.toBytes(HbaseCURDUtils.getDefaultFamilyName()),
						column.getBytes(HbaseCURDUtils.getCharset()));
			}
		}

		if (hbaseFilters != null && hbaseFilters.size() != 0) {
			scan.setFilter(new FilterList(filterListOper, hbaseFilters));
		}

		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		String ScanToString = Base64.encodeBytes(proto.toByteArray());

		Configuration conf = getHbaseConfiguration(tableName);

		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		conf.set(TableInputFormat.SCAN, ScanToString);

		//读取业务数据表
		JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = ctx.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);
		return hbaseRDD;
	}

	/**
	 * 从HBASE过滤查询数据到内存生成RDD
	 * @Description
	 * 方法描述:传入bjgoodwill-core-orm filters过滤条件和要返回的列集合，过滤查询数据到内存生成RDD
	 * @return 返回类型： JavaPairRDD<ImmutableBytesWritable,Result>
	 * @param ctx
	 * @param tableName
	 * @param filters
	 * @param columns
	 * @return
	 * @throws IOException
	 */
	public static JavaPairRDD<ImmutableBytesWritable, Result> findFilterRDDFromHbase(JavaSparkContext ctx,
			String tableName, List<PropertyFilter> filters, final String... columns) throws IOException {
		Scan scan = new Scan();
		if (columns.length != 0) {
			//设置只返回关键列
			for (String column : columns) {
				scan.addColumn(Bytes.toBytes(HbaseCURDUtils.getDefaultFamilyName()),
						column.getBytes(HbaseCURDUtils.getCharset()));
			}
		}
		//过滤条件
		if (filters != null && filters.size() > 0) {
			scan.setFilter(HbaseCURDUtils.genHbaseFilters(filters));
		}

		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		String ScanToString = Base64.encodeBytes(proto.toByteArray());

		Configuration conf = getHbaseConfiguration(tableName);

		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		conf.set(TableInputFormat.SCAN, ScanToString);

		//读取业务数据表
		JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = ctx.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);
		return hbaseRDD;
	}

	/**
	 * 从HBase集群中取得tableName对应的全部数据
	 * @param ctx
	 * @param tableName
	 * @param columns
	 * @return JavaPairRDD<ImmutableBytesWritable, Result>
	 * @throws IOException
	 */
	public static JavaPairRDD<ImmutableBytesWritable, Result> findAllRDDFromHbase(JavaSparkContext ctx,
			String tableName, final String... columns) throws IOException {
		Scan scan = new Scan();
		//测试时使用，实际在yarn上部署需要注释掉
		/*String rowKeyPr = "1234";
		scan.setStartRow(Bytes.toBytes(rowKeyPr));
		scan.setStopRow(Bytes.toBytes(rowKeyPr + "}"));*/
		if (columns.length != 0) {
			//设置只返回关键列
			for (String column : columns) {
				if (StringUtils.isBlank(column)) {
					continue;
				}
				scan.addColumn(Bytes.toBytes(HbaseCURDUtils.getDefaultFamilyName()),
						column.getBytes(HbaseCURDUtils.getCharset()));
			}
		}
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		String ScanToString = Base64.encodeBytes(proto.toByteArray());

		Configuration conf = getHbaseConfiguration(tableName);

		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		conf.set(TableInputFormat.SCAN, ScanToString);

		conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 600000);
		//读取业务数据表
		JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = ctx.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);
		return hbaseRDD;
	}

	/**
	 * 从HBase集群中取得tableName对应的全部数据,如果配了增量字段，那么按预置的逻辑进行增量抽取
	 * @param ctx
	 * @param tableName
	 * @param columns
	 * @return JavaPairRDD<ImmutableBytesWritable, Result>
	 * @throws IOException
	 */
	public static JavaPairRDD<ImmutableBytesWritable, Result> findIncRDDFromHbase(JavaSparkContext ctx,
			String tableName, String timeField, final String... columns) throws IOException {
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("cf"));
		/*String rowKeyPr = "1234";
		scan.setStartRow(Bytes.toBytes(rowKeyPr));
		scan.setStopRow(Bytes.toBytes(rowKeyPr + "}"));*/
		scan.setMaxResultSize(1000);
		scan.setCaching(200);
		if (StringUtils.isNotBlank(timeField)) {
			Calendar cal = Calendar.getInstance();
			int month = cal.get(Calendar.MONTH) + 1;
			if (month > 3) {//获取当前月份，如果当前月份大于一季度3月，则只算几年的数据，认为以前的数据不再发生变化
				logger.error("*******增量计算表：" + tableName + "," + cal.get(Calendar.YEAR) + " 年至今的数据********");
				SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"),
						Bytes.toBytes(timeField), CompareFilter.CompareOp.GREATER_OR_EQUAL,
						Bytes.toBytes(String.valueOf(cal.get(Calendar.YEAR))));
				columnValueFilter.setFilterIfMissing(true);
				scan.setFilter(columnValueFilter);
			} else {//在第一季度的，获取今年和上一年的数据
				logger.error("*******增量计算表：" + tableName + "," + (cal.get(Calendar.YEAR) - 1) + " 年至今的数据********");
				SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"),
						Bytes.toBytes(timeField), CompareFilter.CompareOp.GREATER_OR_EQUAL,
						Bytes.toBytes(String.valueOf(cal.get(Calendar.YEAR) - 1)));
				columnValueFilter.setFilterIfMissing(true);
				scan.setFilter(columnValueFilter);
			}
		}
		if (columns.length != 0) {
			//设置只返回关键列
			for (String column : columns) {
				if (StringUtils.isBlank(column)) {
					continue;
				}
				scan.addColumn(Bytes.toBytes(HbaseCURDUtils.getDefaultFamilyName()),
						column.getBytes(HbaseCURDUtils.getCharset()));
			}
		}
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		String ScanToString = Base64.encodeBytes(proto.toByteArray());

		Configuration conf = getHbaseConfiguration(tableName);

		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		conf.set(TableInputFormat.SCAN, ScanToString);

		conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 600000);
		//读取业务数据表
		JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = ctx.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);
		return hbaseRDD;
	}

	/**
	 * 根据条件过滤RDD
	 * @Description
	 * 方法描述: 传入目标RDD【从HBASE获取的未转换的RDD】，过滤条件，然后根据过滤条件对RDD进行filter操作
	 * @return 返回类型： JavaPairRDD<ImmutableBytesWritable,Result>
	 * @param rdd 
	 * @param filter PropertyFilter封装,matchtype支持：=,>,>=,<,<=,like,<>
	 * @return
	 */
	public static JavaPairRDD<ImmutableBytesWritable, Result> filterRDDByCondition(
			JavaPairRDD<ImmutableBytesWritable, Result> rdd, final PropertyFilter filter) {
		return rdd.filter(new Function<Tuple2<ImmutableBytesWritable, Result>, Boolean>() {
			private static final long serialVersionUID = 1675910195155359112L;

			@Override
			public Boolean call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
				Result result = v1._2;
				String value = getValueFromResult(result, filter.getPropertyName());
				String matchtype = filter.getMatchType();
				String propertyValue = filter.getPropertyValue().toString();
				boolean flag = false;
				if (MatchType.EQ.getOperation().equals(matchtype)) {
					if (value != null && value.equals(propertyValue)) {
						flag = true;
					}
				} else if (MatchType.GT.getOperation().equals(matchtype)) {
					if (value != null && value.compareTo(propertyValue) > 0) {
						flag = true;
					}
				} else if (MatchType.GE.getOperation().equals(matchtype)) {
					if (value != null && value.compareTo(propertyValue) >= 0) {
						flag = true;
					}
				} else if (MatchType.LT.getOperation().equals(matchtype)) {
					if (value != null && value.compareTo(propertyValue) < 0) {
						flag = true;
					}
				} else if (MatchType.LE.getOperation().equals(matchtype)) {
					if (value != null && value.compareTo(propertyValue) <= 0) {
						flag = true;
					}
				} else if (MatchType.NE.getOperation().equals(matchtype)) {
					if (value != null && !value.equals(propertyValue)) {
						flag = true;
					}
				} else if (MatchType.LIKE.getOperation().equals(matchtype)) {
					if (value != null && value.contains(propertyValue)) {
						flag = true;
					}
				}

				return flag;
			}
		});

	}

	/**
	 * @Description
	 * 方法描述: 将查询结果转换为<STRING,STRING>形式返回
	 *  返回顺序和传入参数顺序相同
	 * @return 返回类型： JavaPairRDD<String,String>
	 * @param rdd
	 * @param keyField 第一个返回字段名
	 * @param valueField 第二个返回字段名
	 * @return
	 */
	public static JavaPairRDD<String, String> flatMapToPairKV(JavaPairRDD<ImmutableBytesWritable, Result> rdd,
			final String keyField, final String valueField) {
		return rdd.flatMapToPair(new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String, String>() {
			private static final long serialVersionUID = -4105815334656013383L;

			@Override
			public Iterable<Tuple2<String, String>> call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
				List<Tuple2<String, String>> covertList = new ArrayList<Tuple2<String, String>>();
				Result result = t._2;
				String key = getValueFromResult(result, keyField);
				String value = getValueFromResult(result, valueField);
				//如果值或者value是空的话，此条记录返回一个空List
				if (StringUtils.isBlank(key) || StringUtils.isBlank(value)) {
					return covertList;
				}
				covertList.add(new Tuple2<String, String>(key, value));
				return covertList;
			}
		});
	}

	/**
	 * @Description
	 * 方法描述: 将查询结果转换为<STRING,STRING>形式,并且去重后返回
	 *  返回顺序和传入参数顺序相同
	 * @return 返回类型： JavaPairRDD<String,String>
	 * @param rdd
	 * @param keyField 第一个返回字段名
	 * @param valueField 第二个返回字段名
	 * @return
	 */
	public static JavaPairRDD<String, String> flatMapToPairKVAndCutRepeat(
			JavaPairRDD<ImmutableBytesWritable, Result> rdd, final String keyField, final String valueField) {
		//将RDD转换为Key-value形式
		JavaPairRDD<String, String> kvRDD = rdd
				.flatMapToPair(new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String, String>() {
					private static final long serialVersionUID = -4105815334656013383L;

					@Override
					public Iterable<Tuple2<String, String>> call(Tuple2<ImmutableBytesWritable, Result> t)
							throws Exception {
						List<Tuple2<String, String>> covertList = new ArrayList<Tuple2<String, String>>();
						Result result = t._2;
						String key = getValueFromResult(result, keyField);
						String value = getValueFromResult(result, valueField);
						//如果值或者value是空的话，此条记录返回一个空List
						if (StringUtils.isBlank(key) || StringUtils.isBlank(value)) {
							return covertList;
						}
						covertList.add(new Tuple2<String, String>(key, value));
						return covertList;
					}
				});
		//按Key聚合，然后转换为Key：value格式（去重）
		return kvRDD.groupByKey()
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
					private static final long serialVersionUID = -7295972315172753796L;

					@Override
					public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
						List<Tuple2<String, String>> coverts = new ArrayList<Tuple2<String, String>>();
						String value = null;
						Iterator<String> itor = t._2.iterator();
						while (itor.hasNext()) {
							value = itor.next();
							if (StringUtils.isNotBlank(value)) {
								break;
							}
						}
						if (value != null) {
							coverts.add(new Tuple2<String, String>(t._1, value));
						}
						return coverts;
					}
				});
	}

	/**
	 * @Description
	 * 方法描述:将查询结果转换为<STRING,MAP>形式返回
	 * <br>每一条记录的rowkey也以'rowkey'的名称存入MAP中
	 * @return 返回类型： JavaPairRDD<String,Map<String,String>>
	 * @param rdd
	 * @param columns Map中包含的字段名
	 * @return
	 */
	public static JavaPairRDD<String, Map<String, String>> flatMapToPairMap(
			JavaPairRDD<ImmutableBytesWritable, Result> rdd, final String... columns) {
		return rdd.flatMapToPair(
				new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String, Map<String, String>>() {
					private static final long serialVersionUID = 5626507389162138448L;

					@Override
					public Iterable<Tuple2<String, Map<String, String>>> call(Tuple2<ImmutableBytesWritable, Result> t)
							throws Exception {
						List<Tuple2<String, Map<String, String>>> converts = new ArrayList<Tuple2<String, Map<String, String>>>();
						String rowKey = Bytes.toString(t._1.get());
						Result result = t._2;
						//将Result相关信息转换为Map

						Map<String, String> map = new HashMap<String, String>();
						map.put("rowkey", rowKey);
						for (String column : columns) {
							map.put(column, getValueFromResult(result, column));
						}
						return converts;
					}
				});
	}

	/**
	 * 将刚拉取到内存的RDD按照
	 * @param rdd  源RDD
	 * @param mappings 存储的对照   Key是要存的名称，value是数据库字段
	 * @return <患者id,Map><br>
	 * 每条记录生成一个Map，记录的rowKey以“rowkey”为Key放入结果Map中
	 */
	public static JavaPairRDD<String, Map<String, String>> flatSourceRDDToPairMap(
			JavaPairRDD<ImmutableBytesWritable, Result> rdd, final Map<String, String> mappings) {
		return rdd.flatMapToPair(
				new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String, Map<String, String>>() {
					private static final long serialVersionUID = -7295972315172753796L;

					@Override
					public Iterable<Tuple2<String, Map<String, String>>> call(Tuple2<ImmutableBytesWritable, Result> t)
							throws Exception {
						List<Tuple2<String, Map<String, String>>> converts = new ArrayList<Tuple2<String, Map<String, String>>>();
						String rowKey = Bytes.toString(t._1.get());
						Map<String, String> map = new HashMap<String, String>();
						map.put("rowkey", rowKey);
						Result result = t._2;
						for (Map.Entry<String, String> entry : mappings.entrySet()) {
							map.put(entry.getKey(), SparkUtils.getValueFromResult(result, entry.getValue()));
						}
						String patient_identifier = getOneValueFromResult(result,
								getPatientIdentifierFields("PATIENT_IDENTIFIER"));
						converts.add(new Tuple2<String, Map<String, String>>(patient_identifier, map));
						return converts;
					}
				});
	}

	private static Map<String, String> idFirstTwoAddressMapping = null;

	public static Map<String, String> getAddressNoNamesMap() {
		if (idFirstTwoAddressMapping == null) {
			idFirstTwoAddressMapping = new LinkedHashMap<String, String>();
			idFirstTwoAddressMapping.put("11", "北京");
			idFirstTwoAddressMapping.put("12", "天津");
			idFirstTwoAddressMapping.put("13", "河北");
			idFirstTwoAddressMapping.put("14", "山西");
			idFirstTwoAddressMapping.put("15", "内蒙古");
			idFirstTwoAddressMapping.put("21", "辽宁");
			idFirstTwoAddressMapping.put("22", "吉林");
			idFirstTwoAddressMapping.put("23", "黑龙江");
			idFirstTwoAddressMapping.put("31", "上海");
			idFirstTwoAddressMapping.put("32", "江苏");
			idFirstTwoAddressMapping.put("33", "浙江");
			idFirstTwoAddressMapping.put("34", "安徽");
			idFirstTwoAddressMapping.put("35", "福建");
			idFirstTwoAddressMapping.put("36", "江西");
			idFirstTwoAddressMapping.put("37", "山东");
			idFirstTwoAddressMapping.put("41", "河南");
			idFirstTwoAddressMapping.put("42", "湖北");
			idFirstTwoAddressMapping.put("43", "湖南");
			idFirstTwoAddressMapping.put("44", "广东");
			idFirstTwoAddressMapping.put("45", "广西");
			idFirstTwoAddressMapping.put("46", "海南");
			idFirstTwoAddressMapping.put("50", "重庆");
			idFirstTwoAddressMapping.put("51", "四川");
			idFirstTwoAddressMapping.put("52", "贵州");
			idFirstTwoAddressMapping.put("53", "云南");
			idFirstTwoAddressMapping.put("54", "西藏");
			idFirstTwoAddressMapping.put("61", "陕西");
			idFirstTwoAddressMapping.put("62", "甘肃");
			idFirstTwoAddressMapping.put("63", "青海");
			idFirstTwoAddressMapping.put("64", "宁夏");
			idFirstTwoAddressMapping.put("65", "新疆");
			//香港 澳门 台湾
		}
		return idFirstTwoAddressMapping;
	}

	/** 
	* 计算量日期之间的分钟数
	* xiehongwei 2017-01-06 增加try-catch处理异常
	*/
	public static Long minutesBetween(String startTime, String endTime) throws ParseException {
		Long between_minutes = null;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			between_minutes = (sdf.parse(endTime).getTime() - sdf.parse(startTime).getTime()) / (1000 * 60);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return between_minutes;
	}

	/** 
	* 计算量日期之间的年数
	*/
	public static Long yearBetween(String startTime, String endTime) throws ParseException {
		if (StringUtils.isBlank(startTime) || StringUtils.isBlank(endTime)) {
			return null;
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long between_days = (sdf.parse(endTime).getTime() - sdf.parse(startTime).getTime())
				/ (1000 * 60 * 60 * 24 * 365);
		return between_days;
	}

	/** 
	* 计算量日期之间的日数
	*/
	public static Long dayBetween(String startTime, String endTime) throws ParseException {
		if (StringUtils.isNotBlank(startTime) && startTime.length() >= 10) {
			startTime = startTime.substring(0, 10) + " 00:00:00";
		} else {
			return null;
		}
		if (StringUtils.isNotBlank(endTime) && endTime.length() >= 10) {
			endTime = endTime.substring(0, 10) + " 00:00:00";
		} else {
			return null;
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long between_days = (sdf.parse(endTime).getTime() - sdf.parse(startTime).getTime()) / (1000 * 60 * 60 * 24);
		return between_days;
	}

	/** 
	* 计算量两个时间字段的小时间隔
	*/
	public static Long hourBetween(String startTime, String endTime) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long between_hours = (sdf.parse(endTime).getTime() - sdf.parse(startTime).getTime()) / (1000 * 60 * 60);
		return between_hours;
	}

	/** 
	* 计算量两个时间字段的分钟间隔
	*/
	public static Long minuteBetween(String startTime, String endTime) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long between_minutes = (sdf.parse(endTime).getTime() - sdf.parse(startTime).getTime()) / (1000 * 60 * 60);
		return between_minutes;
	}

	/**
	 * 根据用户生日计算年龄段
	 * @throws ParseException 
	 */
	public static String getAgeByBirthday(String birthday, String targetDay) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar cal = Calendar.getInstance();
		String result = null;
		try {
			cal.setTime(sdf.parse(targetDay));
			if (cal.before(birthday)) {
				throw new IllegalArgumentException("The birthDay is before Now.It's unbelievable!");
			}

			int yearNow = cal.get(Calendar.YEAR);
			int monthNow = cal.get(Calendar.MONTH) + 1;
			int dayOfMonthNow = cal.get(Calendar.DAY_OF_MONTH);

			cal.setTime(sdf.parse(birthday));
			int yearBirth = cal.get(Calendar.YEAR);
			int monthBirth = cal.get(Calendar.MONTH) + 1;
			int dayOfMonthBirth = cal.get(Calendar.DAY_OF_MONTH);

			int age = yearNow - yearBirth;

			if (monthNow <= monthBirth) {
				if (monthNow == monthBirth) {
					if (dayOfMonthNow < dayOfMonthBirth) {
						age--;
					}
				} else {
					age--;
				}
			}
			if (age <= 6) {
				result = "0_6";
			} else if (age >= 7 && age <= 17) {
				result = "7_17";
			} else if (age >= 18 && age <= 28) {
				result = "18_28";
			} else if (age >= 29 && age <= 40) {
				result = "29_40";
			} else if (age >= 41 && age <= 65) {
				result = "41_65";
			} else if (age >= 66) {
				result = "66_";
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * if (ageNewInt > 0 && ageNewInt <= 6) {
										list.add(new Tuple2<String, Long>("0_6", 1l));
									} else if (ageNewInt >= 7 && ageNewInt <= 17) {
										list.add(new Tuple2<String, Long>("7_17", 1l));
									} else if (ageNewInt >= 18 && ageNewInt <= 28) {
										list.add(new Tuple2<String, Long>("18_28", 1l));
									} else if (ageNewInt >= 29 && ageNewInt <= 40) {
										list.add(new Tuple2<String, Long>("29_40", 1l));
									} else if (ageNewInt >= 41 && ageNewInt <= 65) {
										list.add(new Tuple2<String, Long>("41_65", 1l));
									} else if (ageNewInt >= 66) {
										list.add(new Tuple2<String, Long>("66_", 1l));
									}
	 * @Description
	 * 方法描述:
	 * @return 返回类型： void
	 * @param args
	 * @throws ParseException
	 */
	public static void main(String[] args) throws ParseException {
		System.out.println(dayBetween("2016-04-13 00:00:00", "2016-04-13 01:03:01") == 0);
	}
}
