package cn.tenmg.flink.jobs.metadata.getter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.utils.JDBCUtils;

/**
 * StarRocks元数据获取器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.3
 */
public class StarrocksMetaDataGetter extends AbstractJDBCMetaDataGetter {

	private static final boolean UK_AS_PK = Boolean
			.valueOf(FlinkJobsContext.getProperty("metadata.starrocks.unique_key_as_primary_key")),
			CAL_AS_SCM = Boolean.valueOf(FlinkJobsContext.getProperty("metadata.starrocks.catalog_as_schema"));

	@Override
	Connection getConnection(Map<String, String> dataSource) throws Exception {
		String driver = dataSource.get("driver"), url = dataSource.get("jdbc-url"),
				database = dataSource.get("database-name");
		if (StringUtils.isBlank(driver)) {
			driver = FlinkJobsContext.getDefaultJDBCDriver(JDBCUtils.getProduct(url));
		}
		if (StringUtils.isNotBlank(database)) {
			url += "/" + database;
		}
		Class.forName(driver);
		return DriverManager.getConnection(url, dataSource.get("username"), dataSource.get("password"));
	}

	@Override
	protected Set<String> getPrimaryKeys(Connection con, String catalog, String schema, String tableName)
			throws SQLException {
		StringBuilder sqlBuilder = new StringBuilder(
				"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_KEY "
						+ (UK_AS_PK ? "IN ('PRI','UNI')" : "= 'PRI'"));
		// StarRocks JDBC适配有问题，catalog和schema对调了（catalog本应为null，但实际上却是schema的值）
		// 因此这里允许用户选择是否将catalog作为schema
		if (schema != null || (CAL_AS_SCM && catalog != null)) {
			sqlBuilder.append(" AND TABLE_SCHEMA = ?");
		}
		sqlBuilder.append(" AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION");
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = con.prepareStatement(sqlBuilder.toString());
			int nextId = 1;
			if (schema != null) {
				ps.setString(nextId++, schema);
			} else if (CAL_AS_SCM && catalog != null) {
				ps.setString(nextId++, catalog);
			}
			ps.setString(nextId, tableName);
			rs = ps.executeQuery();
			Set<String> primaryKeys = new HashSet<String>();
			while (rs.next()) {
				primaryKeys.add(rs.getString(COLUMN_NAME));
			}
			return primaryKeys;
		} finally {
			JDBCUtils.close(rs);
			JDBCUtils.close(ps);
		}
	}

}