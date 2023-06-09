package cn.tenmg.flink.jobs.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期工具类。已废弃，请使用 {@code cn.tenmg.dsl.utils.DateUtils} 替换
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
@Deprecated
public abstract class DateUtils {
	/**
	 * 根据模板将制定对象格式化为日期字符串
	 * 
	 * @param obj
	 *            指定对象
	 * @param pattern
	 *            模板
	 * @return 日期字符串
	 */
	public static String format(Object obj, String pattern) {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		return sdf.format(obj);
	}

	/**
	 * 根据模板将指定对象转换为日期对象
	 * 
	 * @param obj
	 *            指定对象
	 * @param pattern
	 *            模板
	 * @return 日期对象
	 * @throws ParseException
	 *             如果无法将对象转换，将抛出此异常
	 */
	public static Date parse(Object obj, String pattern) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		if (obj instanceof String) {
			return sdf.parse((String) obj);
		}
		return sdf.parse(sdf.format(obj));
	}
}