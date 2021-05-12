package com.hdr.utils;

import com.goodwill.core.enums.EnumType;
import com.goodwill.core.orm.Page;
import com.goodwill.core.orm.Page.Sort;
import com.goodwill.core.utils.CollectionUtils;
import com.goodwill.core.utils.DateUtils;
import com.goodwill.core.utils.EnumUtils;
import com.goodwill.core.utils.json.CustomFieldFilter;
import com.goodwill.core.utils.json.DateValueProcessor;
import com.goodwill.core.utils.json.EnumFieldProcessor;
import com.google.gson.Gson;
import net.sf.json.*;
import net.sf.json.processors.DefaultValueProcessor;
import net.sf.json.util.CycleDetectionStrategy;
import net.sf.json.xml.XMLSerializer;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

	/**
	 * @Description
	 * 方法描述: List<Map<>>多字段排序
	 * @return 返回类型： void
	 * @param list 排序集合
	 * @param columns 排序字段  [visit_time]
	 * @param orders 排序规则     [Sort.DESC]
	 */
	public static void sortListMulti(List<?> list, String[] columns, String[] orders) {
		//排序规则
		List<Page.Sort> sorts = new ArrayList<Page.Sort>();
		//构建排序规则
		for (int i = 0; i < columns.length; i++) {
			Sort sort = new Sort(columns[i], orders[i]);
			sorts.add(sort);
		}
		//排序
		CollectionUtils.sortMuti(list, sorts);
	}

	/**
	 * @Description
	 * 方法描述: json字符串转化为数组
	 * @return 返回类型： List<T>
	 * @param jsonString
	 * @param clazz
	 * @return
	 */
	public static <T> List<T> getDTOList(String jsonString, Class<T> clazz) {
		Type type = new ParameterizedTypeImpl(clazz);
		Gson gson = new Gson();
		List<T> list = gson.fromJson(jsonString, type);
		return list;
	}

	/**
	 * @Description
	 * 方法描述: 获取时间字符串
	 * @return 返回类型： String
	 * @param fmt 时间格式
	 * @param time 完整的时间字符串 yyyy-MM-dd HH:mm:ss
	 * @return
	 */
	public static String getDate(String fmt, String time) {
		Date date = DateUtils.convertStringToDate(time);
		return DateUtils.getDate(fmt, date);
	}

	/**
	 * @Description
	 * 方法描述: 向Map中填充数据，若value为空，则使用指定替换符替换
	 * @return 返回类型： void
	 * @param map Map集合
	 * @param key 键
	 * @param value 值
	 * @param replace 空值替换符
	 * @param update 若key已经存在且对应的value不为空，是否更新已有value
	 */
	public static void checkAndPutToMap(Map<String, String> map, String key, String value, String replace,
			boolean update) {
		boolean put = false; //是否更新
		if (map.containsKey(key)) { //key已存在
			//key存在 对应的value为空
			if (null != replace && StringUtils.equals(map.get(key).toString(), replace)) {
				put = true;
			} else {
				//key存在 对应的value不为空 根据update决定是否更新key对应的value
				if (update) {
					put = true;
				}
			}
		} else { //key不存在 直接更新
			put = true;
		}

		//终止执行
		if (!put) {
			return;
		}

		//更新
		if (StringUtils.isBlank(value)) {
			if (null != replace) {
				map.put(key, replace);
			}
		} else {
			map.put(key, value);
		}
	}

	/**
	 * @Description
	 * 方法描述: 将字符串编码转化为utf8
	 * @return 返回类型： String
	 * @param str 字符串
	 * @return
	 */
	public static String convertToUtf8(String str) {
		if (StringUtils.isNotBlank(str)) {
			try {
				return new String(str.getBytes("iso-8859-1"), "UTF-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		return str;
	}

	/**
	 * 比较两个数值大小
	 * @param val1
	 * @param val2
	 * @param oper
	 * @return
	 */
	public static boolean compareStr(double val1, double val2, String oper) {
		boolean result = false;
		if (StringUtils.isBlank(oper)) {
			result = false;
		}
		switch (oper) {
		case ">":
			result = (val1 > val2);
			break;
		case "≥":
			result = (val1 >= val2);
			break;
		case "=":
			result = (val1 == val2);
			break;
		case "≤":
			result = (val1 <= val2);
			break;
		case "<":
			result = (val1 < val2);
			break;
		case "≠":
			result = (val1 != val2);
			break;
		default:
			break;
		}
		return result;
	}

	/**
	 * 将带分隔符号的字符串转换为List,空字符串或者空格将被忽略掉
	 * @param str
	 * @param splitOper
	 * @return
	 */
	public static List<String> StrSplitToList(String str, String splitOper) {

		List<String> list = new ArrayList<String>();
		if (!str.equals("")) {
			if (str.contains(splitOper)) {
				String[] arr = str.split(splitOper);
				for (String s : arr) {
					if (StringUtils.isNotBlank(s)) {
						list.add(s);
					}
				}
			} else {
				list.add(str);
			}
		}

		return list;
	}

	/**
	 * 将格式化字符串直接转换为Map<String, String>类型数据
	 * @param str 示例 k1:v1,k2:v2,k3:v3
	 * @param splitList 分隔每个list元素符号
	 * @param splitMap 分隔每个Map分隔符
	 * @return 
	 */
	public static Map<String, String> StrSplitToMap(String str, String splitMap, String splitKV) {

		Map<String, String> mapResult = new HashMap<String, String>();

		if (!str.equals("")) {
			if (str.contains(splitMap)) {
				String[] arr = str.split(splitMap);
				for (String s : arr) {
					if (StringUtils.isNotBlank(s)) {
						if (s.contains(splitKV)) {
							String[] arrMap = s.split(splitKV);
							mapResult.put(arrMap[0], arrMap[1]);
						}
					}
				}
			} else {
				if (str.contains(splitKV)) {
					String[] arrMap = str.split(splitKV);
					mapResult.put(arrMap[0], arrMap[1]);
				}
			}
		}

		return mapResult;
	}

	/**
	 * 将带分隔符号的字符串转换为List,空字符串或者空格将被忽略掉
	 * 最后的结果去重
	 * @param str
	 * @param splitOper
	 * @return
	 */
	public static List<String> StrSplitToListDW(String str, String splitOper) {

		List<String> list = new ArrayList<String>();
		if (!str.equals("")) {
			if (str.contains(splitOper)) {
				String[] arr = str.split(splitOper);
				for (String s : arr) {
					if (StringUtils.isNotBlank(s) && (!list.contains(s))) {
						list.add(s);
					}
				}
			} else {
				list.add(str);
			}
		}

		return list;
	}

	/**
	 * 将List中的数据去重
	 * @param list
	 * @return
	 */
	public static <T> List<T> listDW(List<T> list) {
		List<T> dwList = new ArrayList<T>();
		for (int i = 0; i < list.size(); i++) {
			if (!dwList.contains(list.get(i))) {
				dwList.add(list.get(i));
			}
		}
		return dwList;
	}

	/**
	 * 获取当前时间文本
	 * @param formatStr
	 * @return
	 */
	public static String dateToStr(Date date, String formatStr) {
		SimpleDateFormat df = new SimpleDateFormat(formatStr);
		return df.format(date);
	}

	/**
	 * 将字符串转换为日期类型
	 * @param date
	 * @param formatStr
	 * @return
	 */
	public static Date strToDate(String date, String formatStr) {
		SimpleDateFormat sdf = new SimpleDateFormat(formatStr);
		Date cdate = new Date();
		try {
			cdate = sdf.parse(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return cdate;
	}

	/**
	 * 日期减计算，默认为strDate减去diff
	 * @param strDate 日期字符串
	 * @param diff 单位：Y年，M月，D日，H小时，m分钟，s秒
	 * @oper 加 add，减 sub
	 * @return
	 */
	public static Date dateDiff(Date date, String diff, String oper) {
		Calendar rightNow = Calendar.getInstance();
		rightNow.setTime(date);
		int c = -1;
		if (diff.contains("Y")) {
			c = Calendar.YEAR;
		}
		if (diff.contains("M")) {
			c = Calendar.MONTH;
		}
		if (diff.contains("D")) {
			c = Calendar.DAY_OF_YEAR;
		}
		if (diff.contains("H")) {
			c = Calendar.HOUR;
		}
		if (diff.contains("m")) {
			c = Calendar.MINUTE;
		}
		if (diff.contains("S")) {
			c = Calendar.SECOND;
		}
		if (c == -1) {
			return date;
		}
		Integer diffvalue = Integer.parseInt(diff.substring(0, diff.length() - 1));
		if (oper.equals("add")) {
			rightNow.add(c, diffvalue);
		} else {
			rightNow.add(c, -diffvalue);
		}

		return rightNow.getTime();
	}

	/**
	 * 两个时间差
	 * @param begin
	 * @param end
	 * @param diff D H m S
	 * @return
	 */
	public static long dateDiff(Date begin, Date end, String diff) {
		long between = (end.getTime() - begin.getTime()) / 1000;//除以1000是为了转换成秒
		if (diff.contains("D")) {
			return between / (24 * 3600);
		} else if (diff.contains("H")) {
			return between / 3600;
		} else if (diff.contains("m")) {
			return between / 60;
		} else if (diff.contains("S")) {
			//返回秒
			return between;
		} else {
			return 0;
		}
	}

	/**
	 * 比较两个时间大小 date1是否大于date2
	 * @param date1
	 * @param date2
	 * @return
	 */
	public static boolean dateCompare(Date date1, Date date2) {
		long between = (date1.getTime() - date2.getTime());
		if (between >= 0) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 计算当前时间向前或者向后的天数时间
	 * @param preback pre天数向前推,back天数向后推
	 * @param dayCount
	 * @return
	 */
	public static String CurrDateDiff(String preback, int dayCount) {
		String result = "";
		Date d = new Date();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		if (preback.equals("pre")) {
			result = df.format(new Date(d.getTime() - dayCount * 24 * 60 * 60 * 1000));
		} else {
			result = df.format(new Date(d.getTime() + dayCount * 24 * 60 * 60 * 1000));
		}
		return result;
	}

	/**
	 * 由出生日期获得年龄
	 * @param birthDay
	 * @return
	 * @throws Exception
	 */
	public static int getAgebyBirthDay(Date birthDay) {
		Calendar cal = Calendar.getInstance();

		if (cal.before(birthDay)) {
			throw new IllegalArgumentException("The birthDay is before Now.It's unbelievable!");
		}
		int yearNow = cal.get(Calendar.YEAR);
		int monthNow = cal.get(Calendar.MONTH);
		int dayOfMonthNow = cal.get(Calendar.DAY_OF_MONTH);
		cal.setTime(birthDay);

		int yearBirth = cal.get(Calendar.YEAR);
		int monthBirth = cal.get(Calendar.MONTH);
		int dayOfMonthBirth = cal.get(Calendar.DAY_OF_MONTH);

		int age = yearNow - yearBirth;

		if (monthNow <= monthBirth) {
			if (monthNow == monthBirth) {
				if (dayOfMonthNow < dayOfMonthBirth)
					age--;
			} else {
				age--;
			}
		}
		return age;
	}

	/**
	 * 根据code获取枚举label
	 * @return EnumType
	 * @param enumClass
	 * @param code
	 * @return
	 */
	public static String getEnumLabelByCode(Class enumClass, String code) {
		String result = "";
		for (Iterator iter = EnumUtils.iterator(enumClass); iter.hasNext();) {
			Enum enm = (Enum) iter.next();
			EnumType vt = (EnumType) enm;
			if (vt.getCode().equals(code)) {
				result = vt.getLabel();
			}
		}
		return result;
	}

	/**
	 * json 转换为xml
	 * @param json
	 * @return
	 */
	public static synchronized String jsontoXml(String json) {
		try {
			XMLSerializer serializer = new XMLSerializer();
			JSON jsonObject = JSONSerializer.toJSON(json);
			String xmlString = serializer.write(jsonObject);
			return xmlString.replace("<?xml version=\"1.0\" encoding=\"UTF-8\"?>", "");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * map转换为xml
	 * @param map
	 * @return
	 */
	public static synchronized String maptoXml(Map map) {
		Document document = DocumentHelper.createDocument();
		Element nodeElement = document.addElement("node");
		for (Object obj : map.keySet()) {
			Element keyElement = nodeElement.addElement("key");
			keyElement.addAttribute("label", String.valueOf(obj));
			keyElement.setText(String.valueOf(map.get(obj)));
		}
		return doc2String(document);
	}

	/**
	 * 文档转化为输出流
	 * @param document
	 * @return
	 */
	public static String doc2String(Document document) {
		String s = "";
		try {
			// 使用输出流来进行转化  
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			// 使用UTF-8编码  
			OutputFormat format = new OutputFormat("   ", true, "UTF-8");
			format.setSuppressDeclaration(false);
			XMLWriter writer = new XMLWriter(out, format);
			writer.write(document);
			s = out.toString("UTF-8");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return s;
	}

	/**
	 * 将list转换为字符串，并带分隔符
	 * @param list
	 * @param separator
	 * @return
	 */
	public static String listToString(List list, char separator) {
		return org.apache.commons.lang.StringUtils.join(list.toArray(), separator);
	}

	/**
	 * 字符串数组转换为字符串
	 * @param arr
	 * @return
	 */
	public static String arrToString(String[] arr, String separator) {
		StringBuffer sb = new StringBuffer();
		int i = 0;
		for (String str : arr) {
			if (i == arr.length - 1) {
				sb.append(str);
			} else {
				sb.append(str + separator);
			}
			i++;
		}
		return sb.toString();
	}

	/**
	 * 字符串是否是数字
	 * @param value
	 * @return
	 */
	public static boolean isNumber(String value) {
		try {
			Double.parseDouble(value);
			//			Integer.parseInt(value);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	/**
	 * @Description
	 * 方法描述:对一系列Map按照TIME进行排序
	 * @return 返回类型： List<Map<String,String>>
	 * @param list
	 * @return
	 */
	public static List<Map<String, String>> sortListDescByDate(List<Map<String, String>> list, final String sortBy) {
		List<Map<String, String>> result = new LinkedList<Map<String, String>>(list);
		/*for (Map<String, String> map : list) {
			result.add(map);
		}*/
		if (list.size() > 1) {
			Collections.sort(result, new Comparator<Map<String, String>>() {
				@Override
				public int compare(Map<String, String> o1, Map<String, String> o2) {
					String s1 = o2.get(sortBy);
					String s2 = o1.get(sortBy);
					if (StringUtils.isBlank(s1)) {
						s1 = "1900-01-01";
					}
					if (StringUtils.isBlank(s2)) {
						s2 = "1900-01-01";
					}
					return (s1).compareTo(s2);
				}
			});
		}
		return result;
	}

	/**
	 * 对list中的Map进行排序
	 * @param list
	 * @param sortColumn 指定排序列
	 * @param sortBy asc，desc
	 * @return
	 */
	public static List<Map<String, String>> sortList(List<Map<String, String>> list, final String sortColumn,
			final String sortBy) {
		List<Map<String, String>> result = new LinkedList<Map<String, String>>(list);
		/*for (Map<String, String> map : list) {
			result.add(map);
		}*/
		if (list.size() > 1) {
			Collections.sort(result, new Comparator<Map<String, String>>() {
				@Override
				public int compare(Map<String, String> o1, Map<String, String> o2) {
					String s1 = o2.get(sortColumn);
					String s2 = o1.get(sortColumn);
					if (StringUtils.isBlank(s1)) {
						s1 = "";
					}
					if (StringUtils.isBlank(s2)) {
						s2 = "";
					}
					if (StringUtils.isBlank(sortBy) || sortBy.equals("asc")) {
						return (s2).compareTo(s1);
					} else {
						return (s1).compareTo(s2);
					}
				}
			});
		}
		return result;
	}

	/**
	 * 对list中的Map进行排序
	 * @param list
	 * @param sortColumn 指定排序列
	 * @param sortBy asc，desc
	 * @return
	 */
	public static List<Integer> sortList(List<Integer> list, final String sortBy) {
		List<Integer> result = new LinkedList<Integer>(list);
		/*for (Map<String, String> map : list) {
			result.add(map);
		}*/
		if (list.size() > 1) {
			Collections.sort(result, new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					if (StringUtils.isBlank(sortBy) || sortBy.equals("asc")) {
						return (o1).compareTo(o2);
					} else {
						return (o2).compareTo(o1);
					}
				}
			});
		}
		return result;
	}

	/**
	 * 把数据对象转换成json字符串 
	 * 此方法默认集成了项目中常用的一些jsconfig设置,如日期处理、hibernate设置、空值转换等
	 * 此方法默认只是过滤集合字段,不过滤其他任何字段
	 * @param object
	 * @return
	*/
	public static String getJSONString(Object object) {
		// 日期值处理器
		JsonConfig jsonConfig = new JsonConfig();
		jsonConfig.registerJsonValueProcessor(Date.class, new DateValueProcessor());

		//malb 解决hibernate延迟加载导致解析代理类
		jsonConfig.setExcludes(new String[] { "handler", "hibernateLazyInitializer" });

		//malb 解决hibernate双向关联引起嵌套问题
		jsonConfig.setIgnoreDefaultExcludes(false);
		jsonConfig.setCycleDetectionStrategy(CycleDetectionStrategy.LENIENT);

		//jsonConfig.setJsonPropertyFilter(new IgnoreFieldFilter(true)); // 忽略掉集合对象

		//malb 枚举值自动转换为名称
		EnumFieldProcessor enumFieldProcessor = new EnumFieldProcessor();
		jsonConfig.setJsonPropertyFilter(new CustomFieldFilter(enumFieldProcessor));
		jsonConfig.registerJsonValueProcessor(String.class, enumFieldProcessor);

		//测试数据隐藏真实姓名
		//		HideNameProcessor hideNameProcessor = new HideNameProcessor();
		//		jsonConfig.registerJsonValueProcessor(String.class, hideNameProcessor);

		//malb json转换空值设置,常用类型如integer,double之类为null时json默认转换为0,设置使之转换为空
		Class<?>[] classType = { Integer.class, Double.class, Short.class, Boolean.class, Float.class, Long.class,
				BigDecimal.class };
		for (int i = 0; i < classType.length; i++) {
			jsonConfig.registerDefaultValueProcessor(classType[i], new DefaultValueProcessor() {
				@Override
				public Object getDefaultValue(Class type) {
					return JSONNull.getInstance();
				}
			});
		}
		return getJSONString(object, jsonConfig);

	}

	/**
	 * 把数据对象根据传入的jsonConfig转换成json字符串 
	 * DTO对象形如：{"id" : idValue, "name" : nameValue, ...}
	 * 数组对象形如：[{}, {}, {}, ...] 
	 * map对象形如：{key1 : {"id" : idValue, "name" :nameValue, ...}, key2 : {}, ...}
	 * @return String
	 * @param object
	 * @param jsonConfig
	 * @return
	 */
	public static String getJSONString(Object object, JsonConfig jsonConfig) {
		String jsonString = null;
		//malb 判断对象类型,解决List和数组传入报错
		if (object != null) {
			if (object instanceof Collection<?> || object instanceof Object[]) {
				jsonString = JSONArray.fromObject(object, jsonConfig) != null
						? JSONArray.fromObject(object, jsonConfig).toString()
						: null;
			} else {
				jsonString = JSONObject.fromObject(object, jsonConfig) != null
						? JSONObject.fromObject(object, jsonConfig).toString()
						: null;
			}
		}
		return jsonString == null ? "{}" : jsonString;
	}

	/**
	 * 将对象转换为字符串，如果为NULL则转换为空字符串
	 * @param obj
	 * @return
	 */
	public static String objToStr(Object obj) {
		return obj == null ? "" : obj.toString();
	}

	/**
	 * 将对象转换为字符串，如果为NULL则转换为默认值
	 * @param obj 对象
	 * @param def 默认值
	 * @return
	 */
	public static String objToStr(Object obj, String def) {
		return obj == null ? def : obj.toString();
	}

	public static String objToStr(Object obj, String def, String replace) {
		return ((obj == null) || (obj.equals(def))) ? replace : obj.toString();
	}

	/**
	 *  年龄转换为天
	 * @param age X年X月X天
	 * @return  XX天
	 */
	public static double ageToDay(String age) {
		double result = -1;
		if (StringUtils.isNotBlank(age)) {
			age = age.replace(" ", "");
			age = age.replace("年", "岁");
			age = age.replace("日", "天");
			Pattern patternYear = Pattern
					.compile("(\\d*[.{1}\\d]*岁)*(\\d*[.{1}\\d]*月)*(\\d*[.{1}\\d]*天)*(\\d*[.{1}\\d]小时)*");
			Matcher matcher = patternYear.matcher(age);
			while (matcher.find()) {
				String yearStr = Utils.objToStr(matcher.group(1), "0").replace("岁", "");
				String monthStr = Utils.objToStr(matcher.group(2), "0").replace("月", "");
				String dayStr = Utils.objToStr(matcher.group(3), "0").replace("天", "");

				double year = Double.valueOf(yearStr);
				double month = Double.valueOf(monthStr);
				double day = Double.valueOf(dayStr);
				result = (year * 365) + (month * 30) + day;
				break;
			}
		}
		return result;
	}

	/**
	 * 将年龄转换为岁，不慢一岁设置为一岁
	 * @param patInfoMap
	 * @return
	 */
	public static double procAgeYear(String patAge) {
		double patAgeValue = -1;
		//处理年龄统一以岁为单位
		if (StringUtils.isNotBlank(patAge)) {
			//包含岁的数据为正确，不包含的数据直接表示0岁
			if (patAge.contains("岁")) {
				//如果有X岁Y月的数据，直接取岁，忽略月
				String value = patAge.split("岁")[0];
				if (Utils.isNumber(value)) {
					patAgeValue = Double.valueOf(value);
				}
			} else if (!patAge.contains("岁")
					&& (patAge.contains("月") || patAge.contains("天") || patAge.contains("小时"))) {
				//一岁以内的年龄默认为1岁
				patAgeValue = 1;
			} else if (Utils.isNumber(patAge)) {
				//没有单位的默认为岁
				patAgeValue = Double.valueOf(patAge);
			}
		}
		return patAgeValue;
	}

	/**
	 * @Description
	 * 类描述：解决JSON解析不支持泛型解析，特定义此内部类
	 * @author zhaowenkai
	 * @Date 2018年11月13日
	 * @modify
	 * 修改记录：
	 *
	 */
	@SuppressWarnings({ "rawtypes" })
	private static class ParameterizedTypeImpl implements ParameterizedType {
		Class clazz;

		public ParameterizedTypeImpl(Class clz) {
			clazz = clz;
		}

		@Override
		public Type[] getActualTypeArguments() {
			return new Type[] { clazz };
		}

		@Override
		public Type getRawType() {
			return List.class;
		}

		@Override
		public Type getOwnerType() {
			return null;
		}
	}

	public static void main(String[] args) {
		System.out.println(0.0 < 10);
	}
}
