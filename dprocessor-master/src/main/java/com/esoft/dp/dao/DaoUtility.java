package com.esoft.dp.dao;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import org.hibernate.SessionFactory;
import org.hibernate.jdbc.Work;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DaoUtility {
	
	private static Logger logger = LoggerFactory.getLogger(DaoUtility.class);

	// 数据库会话工厂
	private SessionFactory sessionFactory;

	public SessionFactory getSessionFactory() {
		return sessionFactory;
	}

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	/**
	 * 保存记录
	 * 
	 * @param object
	 *            对象实例
	 * @return 新对象实例
	 */
	public void save(Object object) {
		sessionFactory.getCurrentSession().saveOrUpdate(object);
	}
	/**
	 * 更新记录
	 * 
	 * @param object
	 *            对象实例
	 * @return 新对象实例
	 */
	public void merge(Object object) {
		sessionFactory.getCurrentSession().merge(object);
	}
	
	
	/**
	 * @author taoshi
	 * 生成Insert语句
	 */
//	public String initInsertSQL(final String tableName, 
//				final Set<String> columnNames, JSONObject obj,String attID) {
//		// 获取表字段名字和类型
//		this.getSessionFactory().getCurrentSession().doWork(new Work() {
//			@Override
//			public void execute(Connection connection) throws SQLException {
//				DatabaseMetaData meta = connection.getMetaData();
//				ResultSet rs = meta.getColumns(connection.getCatalog(), null,
//						tableName, null);
//				while (rs.next())
//					columnNames.add(rs.getString("COLUMN_NAME"));
//				}
//		});
//		// 创建 INSERT SQL 语句
//		StringBuffer names = new StringBuffer();
//		StringBuffer paras = new StringBuffer();
//		String columnValue = null;
//		for (String columnName : columnNames) {
//			try {
//				columnValue = obj.getString(columnName);
//			} catch (Exception e) {
//				columnValue = null;
//			}
//			if("ID".equals(columnName)){
//				names.append(",").append(columnName);
//				paras.append(",").append("'"+CommandUtils.generUUID()+"'");
//			} else if ("ATTACH_ID".equals(columnName)){
//				names.append(",").append(columnName);
//				paras.append(",").append("'"+attID+"'");
//			} else {
//				if(StringUtils.isNotBlank(columnValue)){
//					names.append(",").append(columnName);
//					paras.append(",").append("'"+columnValue+"'");
//				}
//			}
//			
//		}
//		return new StringBuffer("INSERT INTO ").append(tableName)
//				.append(" (").append(names.substring(1)).append(") VALUES (")
//				.append(paras.substring(1)).append(")").toString();
//	}
	
	/**
	 * @author taoshi
	 * 生成要保存的对象
	 */
//	public Object createObject(final String tableName, 
//				final HashMap<String, Integer> columnType, JSONObject obj, Class<?> clas) {
//		Object clazz = null;
//		// 获取表字段名字和类型
//		this.getSessionFactory().getCurrentSession().doWork(new Work() {
//			@Override
//			public void execute(Connection connection) throws SQLException {
//				DatabaseMetaData meta = connection.getMetaData();
//				ResultSet rs = meta.getColumns(connection.getCatalog(), null,
//						tableName, null);
//				while (rs.next())
//					columnType.put(rs.getString("COLUMN_NAME"),
//							rs.getInt("DATA_TYPE"));
//				}
//		});
//		Object columnValue = null;
//		try {
//			clazz = clas.newInstance();
//		} catch (InstantiationException e1) {
//			// TODO Auto-generated catch block
//			logger.error("createObject |InstantiationException", e1);
//		} catch (IllegalAccessException e1) {
//			// TODO Auto-generated catch block
//			logger.error("createObject |IllegalAccessException", e1);
//		}
//		Set<String> keys = columnType.keySet();
//		for (String columnName : keys) {
//			try {
//				columnValue = obj.getString(columnName);
//				switch (columnType.get(columnName)) {
//				case 3:
//					columnValue = Double.parseDouble(obj.getString(columnName));
//					break;
//				case 12:
//					columnValue = (String)columnValue;
//					break;
//
//				default:
//					break;
//				}
//			} catch (Exception e) {
//				columnValue = null;
//			}
//			//通过反射 实例化对象
//			String setMethod = "set"+ StringHelper.columnNameToProperty(columnName);
//			ReflectUtils.invokeMethod(clazz, setMethod, new Object[] { columnValue });	
//			
//		}
//		return clazz;
//	}
	
	/**
	 * 获取记录
	 * 
	 * @param clazz
	 *            记录类
	 * @param id
	 *            标识
	 * @return 记录实例
	 */
	public Object get(Class<?> clazz, Serializable id) {
		return sessionFactory.getCurrentSession().get(clazz, id);
	}

	public int deleteWithSql(String sql) {
		return sessionFactory.getCurrentSession().createSQLQuery(sql)
				.executeUpdate();
	}
	public void deleteWithTruncate(final String sql) {
		sessionFactory.getCurrentSession().doWork(new Work() {
			@Override
			public void execute(Connection connection) throws SQLException {
				PreparedStatement stat = connection.prepareStatement(sql);
				stat.execute();
			}
		});
	}
	public List<?> queryWithSQL(String sql) {
		return sessionFactory.getCurrentSession().createSQLQuery(sql).list();
	}

	public int[] insertWithSqlBatch(final String sql,
			final String[] columnNames, final List<Object[]> list,
			final HashMap<String, Integer> columnType) {
		final int[] result = new int[list.size()];
		// 执行
		sessionFactory.getCurrentSession().doWork(new Work() {
			@Override
			public void execute(Connection connection) throws SQLException {
				PreparedStatement stat = connection.prepareStatement(sql);
				for (Object[] values : list) {
					// 设置参数
					for (int i = 0; i < values.length; i++)
						stat.setObject(i + 1, values[i],
								columnType.get(columnNames[i]));
					// 加入批量
					stat.addBatch();
				}
				// 批量执行
				int[] batch = stat.executeBatch();
				for (int i = 0, size = batch.length; i < size; i++)
					result[i] = batch[i];
			}
		});
		// 返回
		return result;
	}
	/**
	 * @author taoshi
	 * 通过JDBC 执行语句
	 * @param sql
	 * @return
	 */
	public int[] query4JDBC(final String sql) {
		final int[] result = new int[1];
		// 执行
		sessionFactory.getCurrentSession().doWork(new Work() {
			@Override
			public void execute(Connection connection) throws SQLException {
				PreparedStatement stat = connection.prepareStatement(sql);
				ResultSet rs = stat.executeQuery();
				if (rs.next()) {
					result[0] = rs.getInt(1);
				}
			}
		});
		// 返回
		return result;
	}
	
	
	/**
	 * 查询全部数据 HQL
	 * 返回List 
	 * */
	public List<?> findListHql(String Hql)
	{
		return (List<?>) sessionFactory.getCurrentSession().createQuery(Hql).list();
	}
	
	/**
	 * 查询全部数据 HQL
	 * 返回List 
	 * */
	public List<?> findListHqlPage(String Hql,Integer firstResult,Integer max)
	{
		return (List<?>) sessionFactory.getCurrentSession()
						.createQuery(Hql).setFirstResult(firstResult).setMaxResults(max).list();
	}
	/**
	 * 查询全部数据 SQL
	 * 返回List 
	 * */
	/*
	public List<?> findList(String sql)
	{
		return (List<?>) sessionFactory.getCurrentSession().createSQLQuery(sql).list();
	}*/
	public List<?> findList(String sql,Class<?> entity)
	{
		return (List<?>) sessionFactory.getCurrentSession().createSQLQuery(sql).addEntity(entity).list();
	}

	
	
	/**
	 * 查询全部数据 SQL
	 * 返回List 
	 * */
	public Object findListSql(String Sql)
	{
		return  (Object) sessionFactory.getCurrentSession().createSQLQuery(Sql).list().get(0);
	}
	
	public void callProdure(final String seqName){
		sessionFactory.getCurrentSession().doWork(new Work() {
			@Override
			public void execute(Connection connection) throws SQLException {
				PreparedStatement stat = connection.prepareStatement("{call seq_reset(?)}");
				stat.setString(1, seqName);
				stat.execute();
			}
		});
	}
}
