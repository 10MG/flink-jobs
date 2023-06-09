package cn.tenmg.flink.jobs.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.tenmg.dsl.NamedScript;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.MapUtils;
import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.exception.IllegalConfigurationException;
import cn.tenmg.flink.jobs.metadata.MetaDataGetter;
import cn.tenmg.flink.jobs.metadata.MetaDataGetter.TableMetaData;
import cn.tenmg.flink.jobs.metadata.MetaDataGetterFactory;
import cn.tenmg.flink.jobs.model.DataSync;
import cn.tenmg.flink.jobs.model.data.sync.Column;
import cn.tenmg.flink.jobs.parser.FlinkSQLParamsParser;
import cn.tenmg.flink.jobs.utils.ConfigurationUtils;
import cn.tenmg.flink.jobs.utils.DataSourceFilterUtils;
import cn.tenmg.flink.jobs.utils.SQLUtils;
import cn.tenmg.flink.jobs.utils.StreamTableEnvironmentUtils;

/**
 * 数据同步操作执行器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public class DataSyncOperator extends AbstractOperator<DataSync> {

	private static Logger log = LoggerFactory.getLogger(DataSyncOperator.class);

	private static final String TOPIC_KEY = "topic", GROUP_ID_KEY = "properties.group.id",
			TIMESTAMP_COLUMNS = "data.sync.timestamp.columns", TIMESTAMP_COLUMNS_SPLIT = ",",
			TYPE_KEY_PREFIX = "data.sync" + FlinkJobsContext.CONFIG_SPLITER,
			SCRIPT_KEY_SUFFIX = FlinkJobsContext.CONFIG_SPLITER + "script",
			STRATEGY_KEY_SUFFIX = FlinkJobsContext.CONFIG_SPLITER + "strategy", COLUMN_NAME = "columnName";

	private static final boolean TO_LOWERCASE = !Boolean.valueOf(FlinkJobsContext.getProperty(
			Arrays.asList("data.sync.timestamp.case_sensitive", "data.sync.timestamp.case-sensitive"), "true"));// 不区分大小写，统一转为小写

	private static final Map<String, ColumnConvertArgs> columnConvertArgsMap = new HashMap<String, ColumnConvertArgs>();

	static {
		String convert = FlinkJobsContext.getProperty("data.sync.columns.convert");
		if (convert != null) {
			String argsArr[] = convert.split(";"), args[], argsStr, fromType = null, toType, script;
			StringBuilder typeBuilder = new StringBuilder();
			for (int i = 0; i < argsArr.length; i++) {
				argsStr = argsArr[i];
				int j = 0, len = argsStr.length();
				boolean sameType = false;
				while (j < len) {
					char c = argsStr.charAt(j++);
					if (c == ',') {
						fromType = typeBuilder.toString().trim();
						break;
					} else if (c == ':') {
						sameType = true;
						break;
					} else {
						typeBuilder.append(c);
					}
				}
				typeBuilder.setLength(0);

				if (sameType) {
					toType = fromType;
					script = argsStr.substring(j);
					if (StringUtils.isBlank(script)) {
						throw new IllegalConfigurationException(
								"Each item of the configuration for the key 'data.sync.columns.convert' must be in the form of '{type}:{script}' or '{fromtype},{totype}:{script}'");
					}
				} else {
					args = argsStr.substring(j).split(":", 2);
					if (args.length < 2) {
						throw new IllegalConfigurationException(
								"Each item of the configuration for the key 'data.sync.columns.convert' must be in the form of '{type}:{script}' or '{fromtype},{totype}:{script}'");
					}
					toType = args[0];
					script = args[1];
				}
				columnConvertArgsMap.put(toType.toUpperCase(), new ColumnConvertArgs(fromType, script));
			}
		}
	}

	@Override
	public Object execute(StreamExecutionEnvironment env, DataSync dataSync, Map<String, Object> params)
			throws Exception {
		String from = dataSync.getFrom(), to = dataSync.getTo(), table = dataSync.getTable();
		if (StringUtils.isBlank(from) || StringUtils.isBlank(to) || StringUtils.isBlank(table)) {
			throw new IllegalArgumentException("The property 'from', 'to' or 'table' cannot be blank.");
		}
		StreamTableEnvironment tableEnv = FlinkJobsContext.getOrCreateStreamTableEnvironment(env);
		String fromTable = FlinkJobsContext
				.getProperty(Arrays.asList("data.sync.from_table_prefix", "data.sync.from-table-prefix")) + table, // 兼容老的配置
				fromConfig = dataSync.getFromConfig();
		StreamTableEnvironmentUtils.useCatalogOrDefault(tableEnv, null);

		TableConfig tableConfig = tableEnv.getConfig();
		if (tableConfig != null) {
			Configuration configuration = tableConfig.getConfiguration();
			String pipelineName = configuration.get(PipelineOptions.NAME);
			if (StringUtils.isBlank(pipelineName)) {
				configuration.set(PipelineOptions.NAME, "data-sync" + FlinkJobsContext.CONFIG_SPLITER
						+ String.join(FlinkJobsContext.CONFIG_SPLITER, String.join("-", from, "to", to), table));
			}
		}

		Map<String, String> fromDataSource = DataSourceFilterUtils.filter("source",
				FlinkJobsContext.getDatasource(from)),
				toDataSource = DataSourceFilterUtils.filter("sink", FlinkJobsContext.getDatasource(to));

		Set<String> primaryKeys = collation(dataSync, toDataSource, params);
		List<Column> columns = dataSync.getColumns();

		String sql = fromCreateTableSQL(fromDataSource, dataSync.getTopic(), table, fromTable, columns, primaryKeys,
				fromConfig, params);
		if (log.isInfoEnabled()) {
			log.info("Create source table by Flink SQL: " + SQLUtils.hiddePassword(sql));
			tableEnv.executeSql(sql);

			sql = toCreateTableSQL(toDataSource, table, columns, primaryKeys, dataSync.getToConfig(), params);
			log.info("Create sink table by Flink SQL: " + SQLUtils.hiddePassword(sql));
			tableEnv.executeSql(sql);

			sql = insertSQL(table, fromTable, columns, params);
			log.info("Execute Flink SQL: " + SQLUtils.hiddePassword(sql));
		} else {
			tableEnv.executeSql(sql);

			sql = toCreateTableSQL(toDataSource, table, columns, primaryKeys, dataSync.getToConfig(), params);
			tableEnv.executeSql(sql);

			sql = insertSQL(table, fromTable, columns, params);
		}
		return tableEnv.executeSql(sql);
	}

	/**
	 * 校对和整理列配置并返回主键列（多个列之间使用“,”分隔）
	 * 
	 * @param dataSync
	 *            数据同步配置对象
	 * @param toDataSource
	 *            目标数据源
	 * @param params
	 *            参数查找表
	 * @return 返回主键
	 * @throws Exception
	 *             发生异常
	 */
	private static Set<String> collation(DataSync dataSync, Map<String, String> toDataSource,
			Map<String, Object> params) throws Exception {
		List<Column> columns = dataSync.getColumns();
		if (columns == null) {
			dataSync.setColumns(columns = new ArrayList<Column>());
		}
		Boolean smart = dataSync.getSmart();
		if (smart == null) {
			smart = Boolean.valueOf(FlinkJobsContext.getProperty(FlinkJobsContext.SMART_MODE_CONFIG_KEY));
		}
		Set<String> primaryKeys = null;
		String primaryKey = dataSync.getPrimaryKey(), timestamp = dataSync.getTimestamp();
		if (StringUtils.isNotBlank(primaryKey)) {
			primaryKeys = new HashSet<String>();
			String[] columnNames = primaryKey.split(",");
			for (int i = 0; i < columnNames.length; i++) {
				primaryKeys.add(columnNames[i].trim());
			}
		}

		boolean customTimestampBlank = StringUtils.isBlank(timestamp);
		if (customTimestampBlank) {// 没有指定时间戳列名，使用配置的全局默认值，并根据目标表的实际情况确定是否添加时间戳列
			timestamp = getDefaultTimestamp();
		}
		Map<String, String> timestampMap = StringUtils.isBlank(timestamp) ? Collections.emptyMap()
				: toMap(TO_LOWERCASE, timestamp.split(TIMESTAMP_COLUMNS_SPLIT));// 不区分大小写，统一转为小写
		if (Boolean.TRUE.equals(smart)) {// 智能模式，自动查询列名、数据类型
			MetaDataGetter metaDataGetter = MetaDataGetterFactory.getMetaDataGetter(toDataSource);
			TableMetaData tableMetaData = metaDataGetter.getTableMetaData(toDataSource, dataSync.getTable());
			if (primaryKey == null) {
				primaryKeys = tableMetaData.getPrimaryKeys();
			}
			Map<String, String> columnsMap = tableMetaData.getColumns();
			if (columns.isEmpty()) {// 没有用户自定义列
				addSmartLoadColumns(columns, columnsMap, params, timestampMap);
			} else {// 有用户自定义列
				collationPartlyCustom(columns, params, columnsMap, timestampMap);
			}
		} else if (columns.isEmpty()) {// 没有用户自定义列
			throw new IllegalArgumentException(
					"At least one column must be configured in manual mode, or set the configuration '"
							+ FlinkJobsContext.SMART_MODE_CONFIG_KEY
							+ "=true' to enable automatic column acquisition in smart mode");
		} else {// 全部是用户自定义列
			collationCustom(columns, params, timestampMap);
		}
		if (!customTimestampBlank) {// 配置了时间戳列名
			String columnName;
			for (Iterator<String> it = timestampMap.values().iterator(); it.hasNext();) {// 如果没有时间戳列，但是配置了该列名，依然增加该列，这是用户的错误配置。运行时，可能会由于列不存在会报错
				columnName = it.next();
				Column column = new Column();
				column.setFromName(columnName);
				column.setToName(columnName);// 目标列名和来源列名相同
				columnName = TO_LOWERCASE ? columnName.toLowerCase() : columnName;// 不区分大小写，统一转为小写
				column.setFromType(getDefaultTimestampFromType(columnName));
				column.setToType(getDefaultTimestampToType(columnName));
				columns.add(column);
			}
		}
		return primaryKeys;
	}

	private static void collationPartlyCustom(List<Column> columns, Map<String, Object> params,
			Map<String, String> columnsMap, Map<String, String> timestampMap) {
		String strategy;
		for (int i = 0, size = columns.size(); i < size; i++) {
			Column column = columns.get(i);
			strategy = column.getStrategy();
			if ("from".equals(strategy)) {// 仅创建来源列
				collationPartlyCustomFromStrategy(column, i, params, columnsMap, timestampMap);
			} else if ("to".equals(strategy)) {// 仅创建目标列
				collationPartlyCustomToStratagy(column, i, params, columnsMap, timestampMap);
			} else {
				collationPartlyCustomBothStratagy(column, i, params, columnsMap, timestampMap);
			}
			wrapColumnName(column);// SQL保留关键字包装
		}
		addSmartLoadColumns(columns, columnsMap, params, timestampMap);
	}

	private static void collationPartlyCustomFromStrategy(Column column, int index, Map<String, Object> params,
			Map<String, String> columnsMap, Map<String, String> timestampMap) {
		String fromName = column.getFromName();
		if (StringUtils.isBlank(fromName)) {
			throw new IllegalArgumentException("The property 'fromName' cannot be blank, column index: " + index);
		}
		String fromType = column.getFromType(), columnName = TO_LOWERCASE ? fromName.toLowerCase() : fromName;// 不区分大小写，统一转为小写
		if (timestampMap.containsKey(columnName)) {// 时间戳列
			if (StringUtils.isBlank(fromType)) {
				column.setFromType(getDefaultTimestampFromType(columnName));// 更新时间戳列来源类型
			}
			timestampMap.remove(columnName);
		} else if (StringUtils.isBlank(fromType)) {
			throw new IllegalArgumentException("The property 'fromType' cannot be blank, column index: " + index);
		}
		columnsMap.remove(fromName);
	}

	private static void collationPartlyCustomToStratagy(Column column, int index, Map<String, Object> params,
			Map<String, String> columnsMap, Map<String, String> timestampMap) {
		String toName = column.getToName();
		if (StringUtils.isBlank(toName)) {
			throw new IllegalArgumentException("The property 'toName' cannot be blank, column index: " + index);
		}
		String toType = columnsMap.get(toName), columnName = TO_LOWERCASE ? toName.toLowerCase() : toName;// 不区分大小写，统一转为小写
		if (timestampMap.containsKey(columnName)) {// 时间戳列
			if (StringUtils.isBlank(column.getToType())) {
				column.setToType(toType == null ? getDefaultTimestampToType(columnName) : toType);
			}
			if (StringUtils.isBlank(column.getScript())) {
				column.setScript(getDefaultTimestampScript(columnName));
			}
			timestampMap.remove(columnName);
		} else {
			if (toType == null && StringUtils.isBlank(column.getToType())) {
				throw new IllegalArgumentException("The property 'toType' cannot be blank, column index: " + index);
			} else {// 使用用户自定义列覆盖智能获取的列
				if (StringUtils.isBlank(column.getToType())) {
					column.setToType(toType);
				}
			}
		}
		columnsMap.remove(toName);
	}

	private static void collationPartlyCustomBothStratagy(Column column, int index, Map<String, Object> params,
			Map<String, String> columnsMap, Map<String, String> timestampMap) {
		String fromName = column.getFromName(), toName = column.getToName();
		if (StringUtils.isBlank(fromName)) {
			if (StringUtils.isBlank(toName)) {
				throw new IllegalArgumentException(
						"One of the properties 'fromName' or 'toName' cannot be blank, column index: " + index);
			} else {
				column.setFromName(toName);
			}
		} else if (StringUtils.isBlank(toName)) {
			column.setToName(fromName);
		}
		String columnName = TO_LOWERCASE ? column.getToName().toLowerCase() : column.getToName();// 不区分大小写，统一转为小写
		String fromType, toType = columnsMap.get(column.getToName());
		if (timestampMap.containsKey(columnName)) {// 时间戳列
			if (StringUtils.isBlank(column.getFromType())) {
				column.setFromType(getDefaultTimestampFromType(columnName));
			}
			if (StringUtils.isBlank(column.getToType())) {
				column.setToType(toType == null ? getDefaultTimestampToType(columnName) : toType);
			}
			if (StringUtils.isBlank(column.getScript())) {
				column.setScript(getDefaultTimestampScript(columnName));
			}
			timestampMap.remove(columnName);
		} else {
			if (toType == null) {// 类型补全
				fromType = column.getFromType();
				toType = column.getToType();
				if (StringUtils.isBlank(fromType)) {
					if (StringUtils.isBlank(toType)) {
						throw new IllegalArgumentException(
								"One of the properties 'fromType' or 'toType' cannot be blank, column index: " + index);
					} else {
						column.setFromType(toType);
					}
				} else if (StringUtils.isBlank(toType)) {
					column.setToType(fromType);
				}
			} else {// 使用用户自定义列覆盖智能获取的列
				if (StringUtils.isBlank(column.getToType())) {
					column.setToType(toType);
				}
				ColumnConvertArgs columnConvertArgs = columnConvertArgsMap.get(getDataType(toType).toUpperCase());

				fromType = column.getFromType();
				if (columnConvertArgs == null) {// 无类型转换配置
					if (StringUtils.isBlank(fromType)) {
						column.setFromType(toType);
					}
				} else {// 有类型转换配置
					if (StringUtils.isBlank(fromType)) {
						column.setFromType(columnConvertArgs.fromType);
						if (StringUtils.isBlank(column.getScript())) {
							column.setScript(columnConvertArgs.script);
						}
					} else {
						if (columnConvertArgs.fromType.equalsIgnoreCase(getDataType(fromType))) {
							if (StringUtils.isBlank(column.getScript())) {
								column.setScript(columnConvertArgs.script);
							}
						}
					}
				}
				columnsMap.remove(column.getToName());
			}
		}
	}

	private static void collationCustom(List<Column> columns, Map<String, Object> params,
			Map<String, String> timestampMap) {
		String strategy;
		for (int i = 0, size = columns.size(); i < size; i++) {
			Column column = columns.get(i);
			strategy = column.getStrategy();
			if ("from".equals(strategy)) {// 仅创建来源列
				collationCustomFromStrategy(column, i, params, timestampMap);
			} else if ("to".equals(strategy)) {// 仅创建目标列
				collationCustomToStrategy(column, i, params, timestampMap);
			} else {
				collationCustomBothStrategy(column, i, params, timestampMap);
			}
			wrapColumnName(column);// SQL保留关键字包装
		}
	}

	private static void collationCustomFromStrategy(Column column, int index, Map<String, Object> params,
			Map<String, String> timestampMap) {
		String fromName = column.getFromName();
		if (StringUtils.isBlank(fromName)) {
			throw new IllegalArgumentException("The property 'fromName' cannot be blank, column index: " + index);
		}
		String fromType = column.getFromType(), columnName = TO_LOWERCASE ? fromName.toLowerCase() : fromName;// 不区分大小写，统一转为小写
		if (timestampMap.containsKey(columnName)) {// 时间戳列
			if (StringUtils.isBlank(fromType)) {
				column.setFromType(getDefaultTimestampFromType(columnName));// 更新时间戳列来源类型
			}
			timestampMap.remove(columnName);
		} else if (StringUtils.isBlank(fromType)) {
			throw new IllegalArgumentException("The property 'fromType' cannot be blank, column index: " + index);
		}
	}

	private static void collationCustomToStrategy(Column column, int index, Map<String, Object> params,
			Map<String, String> timestampMap) {
		String toName = column.getToName();
		if (StringUtils.isBlank(toName)) {
			throw new IllegalArgumentException("The property 'toName' cannot be blank, column index: " + index);
		}
		String columnName = TO_LOWERCASE ? toName.toLowerCase() : toName;// 不区分大小写，统一转为小写
		if (timestampMap.containsKey(columnName)) {// 时间戳列
			if (StringUtils.isBlank(column.getToType())) {
				column.setToType(getDefaultTimestampToType(columnName));
			}
			if (StringUtils.isBlank(column.getScript())) {
				column.setScript(getDefaultTimestampScript(columnName));
			}
			timestampMap.remove(columnName);
		} else if (StringUtils.isBlank(column.getToType())) {
			throw new IllegalArgumentException("The property 'toType' cannot be blank, column index: " + index);
		}
	}

	private static void collationCustomBothStrategy(Column column, int index, Map<String, Object> params,
			Map<String, String> timestampMap) {
		String fromName = column.getFromName(), toName = column.getToName();
		if (StringUtils.isBlank(fromName)) {
			if (StringUtils.isBlank(toName)) {
				throw new IllegalArgumentException(
						"One of the properties 'fromName' or 'toName' cannot be blank, column index: " + index);
			} else {
				column.setFromName(toName);
			}
		} else if (StringUtils.isBlank(toName)) {
			column.setToName(fromName);
		}

		String columnName = TO_LOWERCASE ? column.getToName().toLowerCase() : column.getToName();// 不区分大小写，统一转为小写
		if (timestampMap.containsKey(columnName)) {// 时间戳列
			if (StringUtils.isBlank(column.getFromType())) {
				column.setFromType(getDefaultTimestampFromType(columnName));
			}
			if (StringUtils.isBlank(column.getToType())) {
				column.setToType(getDefaultTimestampToType(columnName));
			}
			if (StringUtils.isBlank(column.getScript())) {
				column.setScript(getDefaultTimestampScript(columnName));
			}
			timestampMap.remove(columnName);
		} else {
			String fromType = column.getFromType(), toType = column.getToType();
			if (StringUtils.isBlank(fromType)) {
				if (StringUtils.isBlank(toType)) {
					throw new IllegalArgumentException(
							"One of the properties 'fromType' or 'toType' cannot be blank, column index: " + index);
				} else {
					column.setFromType(toType);
				}
			} else if (StringUtils.isBlank(toType)) {
				column.setToType(fromType);
			}

			ColumnConvertArgs columnConvertArgs = columnConvertArgsMap
					.get(getDataType(column.getToType()).toUpperCase());
			if (columnConvertArgs != null
					&& columnConvertArgs.fromType.equalsIgnoreCase(getDataType(column.getFromType()))) {// 有类型转换配置
				column.setFromType(columnConvertArgs.fromType);
				if (StringUtils.isBlank(column.getScript())) {
					column.setScript(columnConvertArgs.script);
				}
			}
		}
	}

	private static void addSmartLoadColumns(List<Column> columns, Map<String, String> columnsMap,
			Map<String, Object> params, Map<String, String> timestampMap) {
		String toName, toType, columnName, strategy;
		for (Iterator<Entry<String, String>> it = columnsMap.entrySet().iterator(); it.hasNext();) {
			Entry<String, String> entry = it.next();
			toName = entry.getKey();
			toType = entry.getValue();

			Column column = new Column();
			column.setToName(toName);
			column.setToType(toType);
			columnName = TO_LOWERCASE ? toName.toLowerCase() : toName;// 不区分大小写，统一转为小写
			if (timestampMap.containsKey(columnName)) {// 时间戳列
				strategy = getDefaultColumnStrategy(columnName);
				column.setStrategy(strategy);// 设置时间戳列的同步策略
				if (!"to".equals(strategy)) {// 非仅创建目标列
					column.setFromName(toName);// 来源列名和目标列名相同
					column.setFromType(getDefaultTimestampFromType(columnName));
				}
				if (!"from".equals(strategy) && StringUtils.isBlank(column.getScript())) {
					column.setScript(getDefaultTimestampScript(columnName));
				}
				timestampMap.remove(columnName);
			} else {
				column.setFromName(toName);// 来源列名和目标列名相同
				ColumnConvertArgs columnConvertArgs = columnConvertArgsMap.get(getDataType(toType).toUpperCase());
				if (columnConvertArgs == null) {// 无类型转换配置
					column.setFromType(toType);
				} else {// 有类型转换配置
					column.setFromType(columnConvertArgs.fromType);
					column.setScript(columnConvertArgs.script);
				}
			}
			wrapColumnName(column);// SQL保留关键字包装
			columns.add(column);
		}
	}

	private static String getDefaultTimestamp() {
		return FlinkJobsContext.getProperty(TIMESTAMP_COLUMNS);
	}

	private static String getDefaultColumnStrategy(String columnName) {
		return FlinkJobsContext.getProperty(TYPE_KEY_PREFIX + columnName + STRATEGY_KEY_SUFFIX);
	}

	private static String getDefaultTimestampFromType(String columnName) {
		String prefix = TYPE_KEY_PREFIX + columnName + FlinkJobsContext.CONFIG_SPLITER,
				fromType = FlinkJobsContext.getProperty(Arrays.asList(prefix + "from_type", prefix + "from-type"));// 兼容老的配置
		if (fromType == null) {
			return FlinkJobsContext
					.getProperty(Arrays.asList("data.sync.timestamp.from_type", "data.sync.timestamp.from-type"));// 兼容老的配置
		}
		return fromType;
	}

	private static String getDefaultTimestampToType(String columnName) {
		String prefix = TYPE_KEY_PREFIX + columnName + FlinkJobsContext.CONFIG_SPLITER,
				toType = FlinkJobsContext.getProperty(Arrays.asList(prefix + "to_type", prefix + "to-type"));// 兼容老的配置
		if (toType == null) {
			return FlinkJobsContext
					.getProperty(Arrays.asList("data.sync.timestamp.to_type", "data.sync.timestamp.to-type"));// 兼容老的配置
		}
		return toType;
	}

	private static String getDefaultTimestampScript(String columnName) {
		return FlinkJobsContext.getProperty(TYPE_KEY_PREFIX + columnName + SCRIPT_KEY_SUFFIX);
	}

	private static String fromCreateTableSQL(Map<String, String> dataSource, String topic, String table,
			String fromTable, List<Column> columns, Set<String> primaryKeys, String fromConfig,
			Map<String, Object> params) throws IOException {
		Set<String> actualPrimaryKeys = newSet(primaryKeys);
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("CREATE TABLE ").append(SQLUtils.wrapIfReservedKeywords(fromTable)).append("(");
		Column column;
		String toName;
		int i = 0, size = columns.size();
		while (i < size) {
			column = columns.get(i++);
			if ("to".equals(column.getStrategy())) {
				toName = column.getToName();
				actualPrimaryKeys.remove(toName == null ? column.getFromName() : toName);
			} else {
				sqlBuffer.append(column.getFromName()).append(DSLUtils.BLANK_SPACE).append(column.getFromType());
				break;
			}
		}
		while (i < size) {
			column = columns.get(i++);
			if ("to".equals(column.getStrategy())) {
				toName = column.getToName();
				actualPrimaryKeys.remove(toName == null ? column.getFromName() : toName);
			} else {
				sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append(column.getFromName())
						.append(DSLUtils.BLANK_SPACE).append(column.getFromType());
			}
		}
		if (!actualPrimaryKeys.isEmpty()) {
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append("PRIMARY KEY (")
					.append(String.join(", ", actualPrimaryKeys)).append(") NOT ENFORCED");
		}
		sqlBuffer.append(") ").append("WITH (");
		if (StringUtils.isNotBlank(fromConfig)) {
			dataSource.putAll(ConfigurationUtils.load(SQLUtils.toSQL(DSLUtils.parse(fromConfig, params))));
		}
		if (ConfigurationUtils.isKafka(dataSource)) {
			if (!dataSource.containsKey(GROUP_ID_KEY)) {
				dataSource.put(GROUP_ID_KEY,
						FlinkJobsContext
								.getProperty(Arrays.asList("data.sync.group_id_prefix", "data.sync.group-id-prefix"))// 兼容老的配置
								+ table);// 设置properties.group.id
			}
			if (topic != null) {
				dataSource.put(TOPIC_KEY, topic);
			}
		}
		SQLUtils.appendDataSource(sqlBuffer, dataSource, table);
		sqlBuffer.append(")");
		return sqlBuffer.toString();
	}

	private static String toCreateTableSQL(Map<String, String> dataSource, String table, List<Column> columns,
			Set<String> primaryKeys, String toConfig, Map<String, Object> params) throws IOException {
		Set<String> actualPrimaryKeys = newSet(primaryKeys);
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("CREATE TABLE ").append(SQLUtils.wrapIfReservedKeywords(table)).append("(");
		Column column;
		String toName, columnName;
		int i = 0, size = columns.size();
		while (i < size) {
			column = columns.get(i++);
			toName = column.getToName();
			columnName = toName == null ? column.getFromName() : toName;
			if ("from".equals(column.getStrategy())) {
				actualPrimaryKeys.remove(columnName);
			} else {
				sqlBuffer.append(columnName).append(DSLUtils.BLANK_SPACE).append(column.getToType());
				break;
			}
		}
		while (i < size) {
			column = columns.get(i++);
			toName = column.getToName();
			columnName = toName == null ? column.getFromName() : toName;
			if ("from".equals(column.getStrategy())) {
				actualPrimaryKeys.remove(columnName);
			} else {
				sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append(columnName)
						.append(DSLUtils.BLANK_SPACE).append(column.getToType());
			}
		}
		if (!actualPrimaryKeys.isEmpty()) {
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append("PRIMARY KEY (")
					.append(String.join(", ", actualPrimaryKeys)).append(") NOT ENFORCED");
		}
		sqlBuffer.append(") ").append("WITH (");
		if (StringUtils.isBlank(toConfig)) {
			SQLUtils.appendDataSource(sqlBuffer, dataSource, table);
		} else {
			SQLUtils.appendDataSource(sqlBuffer, MapUtils.toHashMapBuilder(dataSource)
					.build(ConfigurationUtils.load(SQLUtils.toSQL(DSLUtils.parse(toConfig, params)))), table);
		}
		sqlBuffer.append(")");
		return sqlBuffer.toString();
	}

	private static String insertSQL(String table, String fromTable, List<Column> columns, Map<String, Object> params) {
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("INSERT INTO ").append(SQLUtils.wrapIfReservedKeywords(table)).append(DSLUtils.BLANK_SPACE)
				.append("(");

		boolean needComma = false;
		Column column;
		String toName;
		for (int i = 0, size = columns.size(); i < size; i++) {
			column = columns.get(i);
			toName = column.getToName();
			if (!"from".equals(column.getStrategy())) {
				if (needComma) {
					sqlBuffer.append(DSLUtils.COMMA);
				} else {
					needComma = true;
				}
				sqlBuffer.append(DSLUtils.BLANK_SPACE).append(toName == null ? column.getFromName() : toName);
			}
		}

		sqlBuffer.append(") SELECT ");
		needComma = false;
		String script;
		for (int i = 0, size = columns.size(); i < size; i++) {
			column = columns.get(i);
			script = column.getScript();
			if (!"from".equals(column.getStrategy())) {
				if (needComma) {
					sqlBuffer.append(DSLUtils.COMMA);
				} else {
					needComma = true;
				}
				sqlBuffer.append(DSLUtils.BLANK_SPACE).append(StringUtils.isBlank(script) ? column.getFromName()
						: toScript(script, column.getFromName(), params));
			}
		}

		sqlBuffer.append(" FROM ").append(SQLUtils.wrapIfReservedKeywords(fromTable));
		return sqlBuffer.toString();

	}

	// 将同步的列转换为SELECT语句的其中一个片段
	private static String toScript(String dsl, String columnName, Map<String, Object> params) {
		NamedScript namedScript = DSLUtils.parse(dsl, MapUtils.toHashMapBuilder(params).build(COLUMN_NAME, columnName));
		return DSLUtils.toScript(namedScript.getScript(), namedScript.getParams(), FlinkSQLParamsParser.getInstance())
				.getValue();
	}

	private static final Map<String, String> toMap(boolean toLowercase, String... strings) {
		Map<String, String> map = new HashMap<String, String>();
		String string;
		if (toLowercase) {
			for (int i = 0; i < strings.length; i++) {
				string = strings[i].trim();
				map.put(string.toLowerCase(), string);
			}
		} else {
			for (int i = 0; i < strings.length; i++) {
				string = strings[i].trim();
				map.put(string, string);
			}
		}
		return map;
	}

	private static String getDataType(String type) {
		return type.split("\\s", 2)[0];
	}

	/**
	 * SQL保留关键字包装
	 * 
	 * @param column
	 *            列
	 */
	private static void wrapColumnName(Column column) {
		column.setFromName(SQLUtils.wrapIfReservedKeywords(column.getFromName()));
		column.setToName(SQLUtils.wrapIfReservedKeywords(column.getToName()));
	}

	private static Set<String> newSet(Set<String> set) {
		Set<String> newSet = new HashSet<String>();
		if (set != null) {
			newSet.addAll(set);
		}
		return newSet;
	}

	/**
	 * 列转换配置参数
	 * 
	 * @author June wjzhao@aliyun.com
	 * 
	 * @since 1.1.3
	 *
	 */
	private static class ColumnConvertArgs {

		/**
		 * 来源类型
		 */
		private String fromType;

		/**
		 * 转换的SQL脚本片段
		 */
		private String script;

		public ColumnConvertArgs(String fromType, String script) {
			super();
			this.fromType = fromType;
			this.script = script;
		}

	}

}