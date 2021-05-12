package com.hdr.utils;

import com.goodwill.core.utils.PropertiesUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class SolrCloudUtils {

	protected static Logger logger = LoggerFactory.getLogger(SolrCloudUtils.class);
	private static final String CONFIG_FILE_NAME = "hadoop.properties";
	private static final String cloudSolrServerZK = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME, "zkHost");
	private static final String DefaultCollection = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME,
			"civListCollection");
	private static final String patInfoCollection = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME,
			"patInfoCollection");
	private static final String ZkClientTimeout = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME, "zkClientTimeout");
	private static final String zkConnectTimeout = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME,
			"zkConnectTimeout");

	/**
	 * 病历文书表
	 */
	public static final String EMRCONTENTTABLE = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME, "emrContentTable");
	/**
	 * 病历文书增量表
	 */
	public static final String EMRCONTENTTABLEINC = PropertiesUtils.getPropertyValue(CONFIG_FILE_NAME,
			"emrContentIncTable");

	private static CloudSolrServer cloudSolrServer;
	private static CloudSolrServer cloudSolrServerPatInfo;

	//获得solrcloud连接
	public static synchronized CloudSolrServer getCloudSolrServer() {
		if (cloudSolrServer == null) {
			try {
				cloudSolrServer = new CloudSolrServer(cloudSolrServerZK);
				cloudSolrServer.setDefaultCollection(DefaultCollection);
				cloudSolrServer.setZkClientTimeout(Integer.valueOf(ZkClientTimeout));
				cloudSolrServer.setZkConnectTimeout(Integer.valueOf(zkConnectTimeout));
				cloudSolrServer.connect();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return cloudSolrServer;
	}
	
	//获得solrcloud连接
	public static synchronized CloudSolrServer getPatInfoCloudSolrServer() {
		if (cloudSolrServerPatInfo == null) {
			try {
				cloudSolrServerPatInfo = new CloudSolrServer(cloudSolrServerZK);
				cloudSolrServerPatInfo.setDefaultCollection(patInfoCollection);
				cloudSolrServerPatInfo.setZkClientTimeout(Integer.valueOf(ZkClientTimeout));
				cloudSolrServerPatInfo.setZkConnectTimeout(Integer.valueOf(zkConnectTimeout));
				cloudSolrServerPatInfo.connect();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return cloudSolrServerPatInfo;
	}

	public static void addDocs(Collection<SolrInputDocument> docs) {
		try {
			cloudSolrServer = getCloudSolrServer();
			cloudSolrServer.add(docs);
			cloudSolrServer.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @Description
	 * 方法描述:优化索引，建议晚上执行
	 * @return 返回类型： void
	 */
	public static void optimes() {

		try {
			cloudSolrServer = getCloudSolrServer();
			cloudSolrServer.optimize();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 查询数据
	public static void searchTest(String String) {
		SolrQuery query = new SolrQuery();
		query.setQuery(String);
		QueryResponse response;
		try {
			cloudSolrServer = getCloudSolrServer();
			response = cloudSolrServer.query(query);
			SolrDocumentList docs = response.getResults();
			for (SolrDocument doc : docs) {
				String content_type = doc.getFieldValue("content_type").toString();
				String id = doc.getFieldValue("id").toString();
				System.out.println("id: " + id);
				System.out.println("content_type: " + content_type);
				System.out.println();
			}
		} catch (SolrServerException e) {
			System.out.println("连接solr服务器出错！");
			e.printStackTrace();
		}

	}

	/**
	 * @Description
	 * 方法描述:solr查询，不包含高亮部分数据
	 * @param queryParams
	 * @param params
	 * @return SolrDocumentList
	 */
	public static SolrDocumentList querySolrDocList(List<String> queryParams, Map<String, String> params) {
		CloudSolrServer solrServer = getCloudSolrServer();
		//		HttpSolrServer solrServer = getSolrServer();
		SolrQuery solrQuery = new SolrQuery();

		//查询参数设置
		for (String qparam : queryParams) {
			solrQuery.setQuery(qparam);
		}

		//参数设置
		for (Map.Entry<String, String> param : params.entrySet()) {
			solrQuery.set(param.getKey(), param.getValue());
		}
		SolrDocumentList solrdDocList = null;
		try {
			QueryResponse response = solrServer.query(solrQuery);
			solrdDocList = response.getResults();

		} catch (Exception e) {
			logger.error("查询全部文档失败", e);
		}

		return solrdDocList;
	}

	
	/**
	 * @Description
	 * 方法描述:solr查询，不包含高亮部分数据
	 * @param queryParams
	 * @param params
	 * @return SolrDocumentList
	 */
	public static SolrDocumentList querySolrPatInfo(List<String> queryParams, Map<String, String> params) {
		CloudSolrServer solrServer = getPatInfoCloudSolrServer();
		//		HttpSolrServer solrServer = getSolrServer();
		SolrQuery solrQuery = new SolrQuery();

		//查询参数设置
		for (String qparam : queryParams) {
			solrQuery.setQuery(qparam);
		}

		//参数设置
		for (Map.Entry<String, String> param : params.entrySet()) {
			solrQuery.set(param.getKey(), param.getValue());
		}
		SolrDocumentList solrdDocList = null;
		try {
			QueryResponse response = solrServer.query(solrQuery);
			solrdDocList = response.getResults();

		} catch (Exception e) {
			logger.error("查询全部文档失败", e);
		}

		return solrdDocList;
	}
	
	//根据ID修改Solr
	public static void updatePatById(String id, Map<String, String> maps) {
		// TODO Auto-generated method stub
		try {
			CloudSolrServer solr = SolrCloudUtils.getCloudSolrServer();
			Set<String> keys = maps.keySet();
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("id", id);
			for (String key : keys) {
				HashMap<String, Object> oper = new HashMap<String, Object>();
				oper.put("set", maps.get(key));
				doc.addField(key, oper);
			}
			UpdateResponse rsp = solr.add(doc);
			System.out.println("update doc id:" + id + " result:" + rsp.getStatus() + " Qtime:" + rsp.getQTime());
			UpdateResponse rspCommit = solr.commit();
			System.out.println(
					"commit doc to index" + " result:" + rspCommit.getStatus() + " Qtime:" + rspCommit.getQTime());
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * @Description
	 * 方法描述:通过EID删除数据
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public static void deleDocByEID(String EID) {
		SolrServer solrServer = getCloudSolrServer();
		;
		try {
			solrServer.deleteByQuery("EID:" + EID + "");
			solrServer.commit();
		} catch (Exception e) {
			logger.error("solr 删除索引失败", e);
		}
	}

	/**
	 * @Description
	 * 方法描述:删除所有数据
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public static void deleDocAll() {
		SolrServer solrServer = getCloudSolrServer();
		;
		try {
			//删除所有数据
			solrServer.deleteByQuery("*:*");
			solrServer.commit();
		} catch (Exception e) {
			logger.error("solr 删除索引失败", e);
		}
	}
}
