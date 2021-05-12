package com.hdr.utils;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * 类描述：Zookeeper工具类
 * @author peijiping
 * @Date 2019年9月27日
 * @modify
 * 修改记录：
 * 
 */
public class ZKCliUtil {

	private static ZooKeeper zk;

	//链接字符串
	private static String zkConnStr = "cul1hadoop01:2181,cul1hadoop02:2181,cul1hadoop03:2181";
	//超时毫秒数
	private static int zkTimeOut = 5000;

	private static Long sessionID;

	//	private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

	/**
	 * @Description
	 * 方法描述:创建链接
	 * @return 返回类型： void
	 */
	public static ZooKeeper getZKConnection(Watcher watch) {

		if (zk == null) {
			try {
				zk = new ZooKeeper(zkConnStr, zkTimeOut, watch);
				zk = new ZooKeeper(zkConnStr, zkTimeOut, watch);
				sessionID = zk.getSessionId();
				//				System.out.println("sessionID:" + sessionID);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return zk;
	}

	/**
	 * @Description
	 * 方法描述: 获取路径下子目录
	 * @return 返回类型： List<String>
	 * @param path
	 * @return
	 */
	public static List<String> getZKChildPath(String path) {

		getZKConnection(null);

		List<String> childList = new ArrayList<String>();
		try {
			childList = zk.getChildren(path, null);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return childList;

	}

	/**
	 * @Description
	 * 方法描述:获取数据
	 * @return 返回类型： Map<String,String>
	 * @param path
	 * @return
	 */
	public static Map<String, Object> getZKData(String path) {
		getZKConnection(null);
		Stat stat = new Stat();
		Map<String, Object> zkDataMap = new HashMap<String, Object>();
		try {
			byte[] zkDataArr = zk.getData(path, null, stat);

			String zkDataStr = new String(zkDataArr);
			zkDataMap.put("zkData", zkDataStr);
			zkDataMap.put("cZkid", stat.getCzxid());
			zkDataMap.put("mZxid", stat.getMzxid());

		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return zkDataMap;
	}

	/**
	 * @Description
	 * 方法描述: Watcher测试
	 * @return 返回类型： void
	 * @throws Exception
	 */
	public static void WatcherTest() throws Exception {
		ZKCliUtil zcu = new ZKCliUtil();
		//实例化Watcher
		DefaultWatcher dw = zcu.new DefaultWatcher();
		DataChangeWatcher dcw = zcu.new DataChangeWatcher();
		ChildrenWatcher cw = zcu.new ChildrenWatcher();

		ZooKeeper zookeeper = getZKConnection(dw);

		try {

			zookeeper.getChildren("/test", cw);
			//			zookeeper.getData("/test/pjp2", dcw, null);
			while (true) {
				System.out.println("------");
				Thread.sleep(1000);
			}

			//			zookeeper.setData("/test/pjp2", "123".getBytes(), -1);

		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}

	}

	public static void SessionTest() throws Exception {

		ZKCliUtil zcu = new ZKCliUtil();

		ZooKeeper zookeeper = getZKConnection(null);
		Thread.sleep(1000);
		//获取sessionID
		Long sessionId = zookeeper.getSessionId();
		byte[] passwd = zookeeper.getSessionPasswd();
		System.out.println("sessionID:" + zookeeper.getSessionId());
		System.out.println("SessionPasswd:" + zookeeper.getSessionPasswd());

		zookeeper.close();
		Thread.sleep(10000);
		//等待会话关闭后，测试原sessionId是否可复用
		System.out.println("创建新的会话连接");
		ZooKeeper zk2 = new ZooKeeper(zkConnStr, zkTimeOut, null, sessionId, passwd);
		System.out.println("zk2 sessionID:" + zk2.getSessionId());
		System.out.println("zk2 SessionPasswd:" + zk2.getSessionPasswd());
	}

	public static void main(String[] args) throws Exception {

		//		List<String> path = getZKChildPath("/test");
		//		System.out.println("path:" + path.toString());
		//		Map<String, Object> zkData = getZKData("/test");
		//		for (Map.Entry<String, Object> entry : zkData.entrySet()) {
		//			System.out.println(entry.getKey() + " ---- " + entry.getValue());
		//		}

		SessionTest();
	}

	//===================================================
	//zookeeper的Watcher实现类
	//===================================================
	public class DefaultWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			System.out.println("==========DefaultWatcher start==============");
			System.out.println("Watcher state: " + event.getState().name());
			System.out.println("Watcher type: " + event.getType().name());
			System.out.println("Watcher path: " + event.getPath());
			System.out.println("==========DefaultWatcher end==============");
		}

	}

	public class DataChangeWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			System.out.println("==========DataChangeWatcher start==============");
			System.out.println("Watcher state: " + event.getState().name());
			System.out.println("Watcher type: " + event.getType().name());
			System.out.println("Watcher path: " + event.getPath());
			System.out.println("==========DataChangeWatcher end==============");
		}

	}

	public class ChildrenWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			System.out.println("==========ChildrenWatcher start==============");
			System.out.println("Watcher state: " + event.getState().name());
			System.out.println("Watcher type: " + event.getType().name());
			System.out.println("Watcher path: " + event.getPath());
			System.out.println("==========ChildrenWatcher end==============");
		}

	}
}
