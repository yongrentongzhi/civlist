package com.hdr.sync;

import com.goodwill.core.orm.MatchType;
import com.goodwill.core.orm.PropertyFilter;
import com.goodwill.hdr.cdssutil.solrsync.civ.CIVPatListUtil;
import com.goodwill.hdr.cdssutil.utils.HbaseConnectUtils;
import com.goodwill.hdr.cdssutil.utils.SolrCloudUtils;
import com.goodwill.hdr.cdssutil.utils.SparkUtils;
import com.goodwill.hdr.cdssutil.utils.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * @author peijiping
 * @Description 类描述：患者列表solr索引同步类，spark并行处理实现。 spark-submit --class
 *              com.goodwill.hdr.cdssutil.solrsync.civ.v2.CIVPatListIndexV2
 *              --master yarn --deploy-mode cluster --executor-memory 3g
 *              --driver-memory 2g --num-executors 4 --executor-cores 2
 *              ./patListIndexV2.jar
 * @Date 2020年3月9日
 * @modify 修改记录：
 */
public class CIVPatListIndex {


	private static String runType = "";// 0正式环境、1测试环境，本地环境
	private static String pid = "";// 调试患者id

	public static void main(String[] args) {

		if (args.length > 0) {
			if (args.length == 1) {
				if ("1".equals(args[0])) {
					runType = args[0];
				} else {
					pid = args[0];
				}
			} else if (args.length == 2) {
				runType = args[0];
				pid = args[1];
			}

		} else {
			// 如果不传参数，默认为正式运行环境
			runType = "0";
		}

		// 实例化spark的上下文实例
		JavaSparkContext jsc = null;

		if (runType.equals("0")) {
			// 正式环境使用
			jsc = SparkUtils.getSparkJavaContext(
					"CIVPatListIndexV3-" + System.currentTimeMillis(), true);
		} else {
			// 本地测试，3个线程，2g内存。
			jsc = SparkUtils.getSparkJavaContextLocal("CIVPatListIndexV3-"
					+ System.currentTimeMillis(), 4, 2);
		}


		try {
			List<PropertyFilter> inpFilters = new ArrayList<PropertyFilter>();
			if (StringUtils.isNotBlank(pid)) {
				PropertyFilter inpFilter = new PropertyFilter();
				inpFilter.setMatchType(MatchType.EQ.getOperation());
				inpFilter.setPropertyName("IN_PATIENT_ID");
				inpFilter.setPropertyValue(pid);
				inpFilters.add(inpFilter);
			}
			// 读取hbase表，特别注意：spark并行读取hbase的前提是表中的数据存在于多个regionserver中。
			JavaPairRDD<ImmutableBytesWritable, Result> civPatListResult = SparkUtils
					.findFilterRDDFromHbase(jsc, "TMP_CIV_PATLIST", inpFilters,
							new String[] { "OID","GLOBLE_ID","IN_PATIENT_ID", "VISIT_ID",
									"VISIT_TYPE_CODE","EID" });

			JavaPairRDD<String, SolrInputDocument> solrDocs = civPatListResult
					.flatMapToPair(new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String, SolrInputDocument>() {

						private static final long serialVersionUID = 1L;

						int count = 1;

						@Override
						public Iterable<Tuple2<String, SolrInputDocument>> call(
								Tuple2<ImmutableBytesWritable, Result> t)
								throws Exception {

							// 定义返回值
							List<Tuple2<String, SolrInputDocument>> list = new ArrayList<Tuple2<String, SolrInputDocument>>();
							String pid = "";
							String vid = "";
							try {
								Result result = t._2;
								 pid = SparkUtils.getValueFromResult(
										result, "IN_PATIENT_ID");
							     vid = SparkUtils.getValueFromResult(
										result, "VISIT_ID");
								String eid = SparkUtils.getValueFromResult(
										result, "EID");
								String visitTypeCode = SparkUtils
										.getValueFromResult(result,
												"VISIT_TYPE_CODE");
								SolrInputDocument doc = new SolrInputDocument();
								if (visitTypeCode.equals("02")) {
									// 住院
									doc = procINP(eid,pid, vid);
								} else if (visitTypeCode.equals("01")) {
									// 门诊
									doc = procOUTP(eid,pid, vid);
								}
								if(StringUtils.isBlank((String)doc.getFieldValue("id"))){
									 doc = new SolrInputDocument();
								}
								if (doc.size() > 0) {
									list.add(new Tuple2<String, SolrInputDocument>(eid, doc));
									count++;
								}
								if (count % 10000 == 0) {
									System.out.println("已读取数据： " + count);
								}
							} catch (Exception e) {
								e.printStackTrace();
							}
							return list;
						}
					});

			// 向solr插入数据 foreachPartion
			solrDocs.foreachPartition(new VoidFunction<Iterator<Tuple2<String, SolrInputDocument>>>() {

				List<SolrInputDocument> solrDocs = new ArrayList<SolrInputDocument>();
				int count = 1;

				@Override
				public void call(Iterator<Tuple2<String, SolrInputDocument>> t)
						throws Exception {
					try {
						// 循环List<SolrInputDocument>
						while (t.hasNext()) {
							Tuple2<String, SolrInputDocument> docList = t
									.next();
							SolrInputDocument doc = docList._2;
							System.out.println("*******doc:" + doc);
							solrDocs.add(doc);

							// System.out.println("foreach: SolrInputDocument:"
							// + doc);

							if (count % 1000 == 0) {
								System.out.println("已添加"
										+ (count)
										+ "个文档索引 "
										+ Utils.dateToStr(new Date(),
												"yyyy-MM-dd hh:mm:ss"));
								SolrCloudUtils.addDocs(solrDocs);
								solrDocs = new ArrayList<SolrInputDocument>();
								System.out.println("文档已提交！ "
										+ Utils.dateToStr(new Date(),
												"yyyy-MM-dd hh:mm:ss"));
								logger.info("文档已提交！ "
										+ Utils.dateToStr(new Date(),
												"yyyy-MM-dd hh:mm:ss"));
							}

							count++;

						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					// 提交剩余项
					if (solrDocs.size() > 0) {
						SolrCloudUtils.addDocs(solrDocs);
						System.out.println("最后提交"
								+ (solrDocs.size())
								+ "个文档索引 "
								+ Utils.dateToStr(new Date(),
										"yyyy-MM-dd hh:mm:ss"));
						logger.info("最后提交"
								+ (solrDocs.size())
								+ "个文档索引 "
								+ Utils.dateToStr(new Date(),
										"yyyy-MM-dd hh:mm:ss"));
						solrDocs = new ArrayList<SolrInputDocument>();
					}

				}

			});

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			e.printStackTrace();
		}
		logger.info("数据读取完毕");

	}

	/**
	 * 处理住院部分数据
	 * 
	 * @param id
	 * @param vid
	 * @throws IOException
	 */
	public static SolrInputDocument procINP(String eid,String id, String vid) {
		SolrInputDocument doc = new SolrInputDocument();
		String rowkeypre = HbaseConnectUtils.getRowkeyPrefix(id) + "|";

		List<PropertyFilter> filters = new ArrayList<PropertyFilter>();
		PropertyFilter filter1 = new PropertyFilter();
		filter1.setMatchType(MatchType.EQ.getOperation());
		filter1.setPropertyName("VISIT_TYPE_CODE");
		filter1.setPropertyValue("02");
		filters.add(filter1);
		
//		PropertyFilter filter2 = new PropertyFilter();
//		filter2.setMatchType(MatchType.EQ.getOperation());
//		filter2.setPropertyName("EID");
//		filter2.setPropertyValue(eid);
//		filters.add(filter2);
		
		List<Map<String, String>> list = HbaseConnectUtils.findByCondition(
				"HDR_PATIENT", rowkeypre, filters, new String[] { "EID",
						"IN_PATIENT_ID", "OUT_PATIENT_ID", "INP_NO", "OUTP_NO",
						"PERSON_NAME", "SEX_CODE", "SEX_NAME", "ID_CARD_NO",
						"HOME_PHONE", "VISIT_TYPE_CODE", "VISIT_TYPE_NAME",
						"DATE_OF_BIRTH", "BRID" });
		logger.info("查询到的hdr_patient患者"+pid+"信息为：" + list.size());
		for (Map<String, String> map : list) {
			// 获取最后住院次
			String pid = map.get("IN_PATIENT_ID");
			eid = map.get("EID");

			String solrid =map.get("IN_PATIENT_ID") + "|" + map.get("VISIT_TYPE_CODE");
			doc.addField("id", solrid);
			doc.addField("EID", map.get("EID"));
			doc.addField("IN_PATIENT_ID", map.get("IN_PATIENT_ID"));
			doc.addField("OUT_PATIENT_ID", map.get("OUT_PATIENT_ID"));
			doc.addField("VISIT_ID", vid);
			doc.addField("INP_NO", map.get("INP_NO"));
			// doc.addField("IN_OUT_NOS", map.get("INP_NO"));
			doc.addField("OUT_NO", map.get("OUTP_NO"));
			doc.addField("PERSON_NAME", map.get("PERSON_NAME"));
			doc.addField("SEX_CODE", map.get("SEX_CODE"));
			doc.addField("SEX_NAME", map.get("SEX_NAME"));
			doc.addField("ID_CARD_NO", map.get("ID_CARD_NO"));
			doc.addField("HOME_PHONE", map.get("HOME_PHONE"));
			doc.addField("VISIT_TYPE_CODE", map.get("VISIT_TYPE_CODE"));
			doc.addField("VISIT_TYPE_NAME", map.get("VISIT_TYPE_NAME"));
			doc.addField("DATE_OF_BIRTH", map.get("DATE_OF_BIRTH"));
			// doc.addField("BRID",map.get("BRID"));

			CIVPatListUtil.setInpData(pid, doc, vid);
			// //获取患者此次住院的医生信息
			// getPatientsDoctorInfo(pid, vid, doc);
		}
		// 新增in_out_nos字段  先注释掉 
		CIVPatListUtil.setInOutNos(eid, doc);
		return doc;
	}

	/**
	 * 处理门诊部分数据
	 * 
	 * @param pid
	 * @param vid
	 */
	public static SolrInputDocument procOUTP(String eid,String pid, String vid) {
		SolrInputDocument doc = new SolrInputDocument();

		String rowkeypre = HbaseConnectUtils.getRowkeyPrefix(pid) + "|";
		List<PropertyFilter> filters = new ArrayList<PropertyFilter>();
		PropertyFilter filter1 = new PropertyFilter();
		filter1.setMatchType(MatchType.EQ.getOperation());
		filter1.setPropertyName("VISIT_TYPE_CODE");
		filter1.setPropertyValue("01");
		filters.add(filter1);
		
//		PropertyFilter filter2 = new PropertyFilter();
//		filter2.setMatchType(MatchType.EQ.getOperation());
//		filter2.setPropertyName("EID");
//		filter2.setPropertyValue(eid);
//		filters.add(filter2);
		
		List<Map<String, String>> list = HbaseConnectUtils.findByCondition(
				"HDR_PATIENT", rowkeypre, filters, new String[] { "EID",
						"IN_PATIENT_ID", "OUT_PATIENT_ID", "INP_NO", "OUTP_NO",
						"PERSON_NAME", "SEX_CODE", "SEX_NAME", "ID_CARD_NO",
						"HOME_PHONE", "VISIT_TYPE_CODE", "VISIT_TYPE_NAME",
						"DATE_OF_BIRTH", "BRID" });
		logger.info("查询到的hdr_patient患者"+pid+"信息为：" + list.size());
		String birthday = "";
		for (Map<String, String> map : list) {
			eid = map.get("EID");
			// 将rowkey添加为id
			String solrid = map.get("OUT_PATIENT_ID") + "|" + map.get("VISIT_TYPE_CODE");
			doc.addField("id", solrid);
			doc.addField("EID", map.get("EID"));
			doc.addField("IN_PATIENT_ID", map.get("IN_PATIENT_ID"));
			doc.addField("OUT_PATIENT_ID", map.get("OUT_PATIENT_ID"));
			doc.addField("VISIT_ID", vid);
			doc.addField("INP_NO", map.get("INP_NO"));
			doc.addField("OUT_NO", map.get("OUTP_NO"));
			doc.addField("PERSON_NAME", map.get("PERSON_NAME"));
			doc.addField("SEX_CODE", map.get("SEX_CODE"));
			doc.addField("SEX_NAME", map.get("SEX_NAME"));
			doc.addField("ID_CARD_NO", map.get("ID_CARD_NO"));
			doc.addField("HOME_PHONE", map.get("HOME_PHONE"));
			doc.addField("DATE_OF_BIRTH", map.get("DATE_OF_BIRTH"));
			// doc.addField("BRID", map.get("BRID") );//map.get("BRID")
			birthday = map.get("DATE_OF_BIRTH");
		}
		Map<String, String> outVisitMap = CIVPatListUtil.getPatOutVisit(pid,
				vid);
		CIVPatListUtil.setOutData("",pid, doc, vid, birthday, outVisitMap);
		CIVPatListUtil.setInOutNos(eid, doc);
		return doc;
	}

	/**
	 * 获取当前患者的三级经治医师
	 * 
	 * @return
	 */
	public static void getPatientsDoctorInfo(String patId, String vid,
			SolrInputDocument doc) {
		String tableName = "HDR_MR_ON_LINE";
		// 拼接rowkey前缀
		String preRowkey = HbaseConnectUtils.getRowkeyPrefix(patId) + "|";
		// 页面查询条件
		List<PropertyFilter> filters = new ArrayList<PropertyFilter>();
		filters.add(new PropertyFilter("VISIT_ID", "STRING", "=", vid));
		List<Map<String, String>> list = HbaseConnectUtils.findByCondition(
				tableName, preRowkey, filters,
				new String[] { "REQUEST_DOCTOR", "PARENT_DOCTOR",
						"SUPER_DOCTOR", "TMP_HOUSEMAN", "DEPT" });
		if (null != list && list.size() > 0) {
			Map<String, String> map = list.get(0);
			doc.addField("REQUEST_DOCTOR", map.get("REQUEST_DOCTOR"));
			doc.addField("PARENT_DOCTOR", map.get("PARENT_DOCTOR"));
			doc.addField("SUPER_DOCTOR", map.get("SUPER_DOCTOR"));
			doc.addField("TMP_HOUSEMAN", map.get("TMP_HOUSEMAN"));
			doc.addField("DEPT", map.get("DEPT"));
		}
	}

}
