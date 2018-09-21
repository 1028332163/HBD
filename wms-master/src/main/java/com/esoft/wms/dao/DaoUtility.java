package com.esoft.wms.dao;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.QueryTimeoutException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.jdbc.Work;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.wms.entity.Job;
import com.esoft.wms.entity.TaskInfo;
import com.esoft.wms.entity.TaskInstance;
import com.esoft.wms.utils.CommonUtils;
import com.esoft.wms.utils.ConstanceInfo;

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
	public Serializable save(Object object) {
		return sessionFactory.getCurrentSession().save(object);
	}
	
	/**
	 * 保存记录
	 * 
	 * @param object
	 *            对象实例
	 * @return 新对象实例
	 */
	public void saveTaskIns(TaskInstance taskInstance,TaskInfo info) {
		// 将taskIns持久化到数据库
		sessionFactory.getCurrentSession().save(taskInstance);
		taskInstance.setSparkCmdContent(CommonUtils.initSparkSubmitCmd(
				info, taskInstance));
		taskInstance.setInsStatus(ConstanceInfo.TASK_INSTANCE_STATUS_NEW);
		
	}
	/**
	 * 保存记录
	 * 
	 * @param object
	 *            对象实例
	 * @return 新对象实例
	 */
	public void saveJob(Job job) {
		sessionFactory.getCurrentSession().save(job);
	}
	
	/**
	 * 保存记录
	 * 
	 * @param object
	 *            对象实例
	 * @return 新对象实例
	 */
	public Object merge(Object object) {
		return sessionFactory.getCurrentSession().merge(object);
	}
	
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
	public int updateWithSql(String sql) {
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
	 * 查询多条数据库记录
	 * */	
	public List<?> findList(String sql) {
		return sessionFactory.getCurrentSession().createSQLQuery(sql).list();
	}
	/**
	 * 查询一条数据库记录
	 * */	
	public Object[] findRecord(String sql) {
		return (Object[])sessionFactory.getCurrentSession().createSQLQuery(sql).list().get(0);
	}
	/**
	 * 查询一条数据库记录的一个字段
	 * */
	public Object queryValue(String Sql)
	{	//如果数据库的查询语句只select一个字段的话，查询结果就是一维的是List[object] 不是 List[object[]]
		return  sessionFactory.getCurrentSession().createSQLQuery(Sql).list().get(0);
	}

	/**
	 * 查询全部数据 SQL
	 * 返回List 
	 * */
	public List<?> findList(String sql,Class<?> entity)
	{
		return (List<?>) sessionFactory.getCurrentSession().createSQLQuery(sql).addEntity(entity).list();
	}
	

	/**
	 * 查询全部数据 HQL
	 * 返回List 
	 * */
	public List<?> findListHql(String Hql)
	{
		return (List<?>) sessionFactory.getCurrentSession().createQuery(Hql).list();
	}


	

	public List<?> findListParamStr(String Sql, String param)
	{
		return sessionFactory.getCurrentSession()
				.createSQLQuery(Sql).setString(0, param).list();
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
	
	public void querySeq(String seqName){
		Session session = this.getSessionFactory().getCurrentSession();
		try {
			StringBuilder sb = new StringBuilder("select ");
			sb.append(seqName);
			sb.append(".NEXTVAL FROM DUAL");
			int[] result = this.query4JDBC(sb.toString());
			int seqNum = result[0];
			String frontStr = null;
			StringBuilder sbAlter = new StringBuilder("alter sequence ");
			sbAlter.append(seqName);
			sbAlter.append(" increment by ");
			frontStr = sbAlter.toString();//这部分字符串可以重复使用
			sbAlter.append("-"+seqNum); 
			frontStr = frontStr + "1";
			Query queryAlterBack = session.createSQLQuery(sbAlter.toString());
			queryAlterBack.executeUpdate();
			this.query4JDBC(sb.toString());
			Query queryAlter = session.createSQLQuery(frontStr);
			queryAlter.executeUpdate();
		} catch (Exception e) {
			logger.info("catch sql exp");
			if(e instanceof QueryTimeoutException){
				String str = "alter sequence "+seqName+ " increment by 1";
				Query q = session.createSQLQuery(str);
				q.executeUpdate();
			}
		}
	}
	
//	/**
//	 * 查询全部数据 SQL
//	 * 返回List 
//	 * */
//	public List<?> findBrocastList(Class<?> entity)
//	{
//		String sql = SqlTemplate.FIND_TASK_BY_HQL 
//				+ ConstanceInfo.TASK_INSTANCE_STATUS_HIVED
//				+ "','" + ConstanceInfo.TASK_INSTANCE_STATUS_SPARKFAILED
//				+ "','" + ConstanceInfo.TASK_INSTANCE_STATUS_HIVEFAL
//				+ "','" + ConstanceInfo.TASK_INSTANCE_STATUS_SPARK
//				+ "') "
//				+ " ORDER BY t.ID desc";
//		return sessionFactory.getCurrentSession().createSQLQuery(sql).list();
//	}
	
//	//更新推送附属表
//	public int updateInsSlave(int pushTimes, String taskInstanceID) {
//		if(pushTimes < 5){
//			pushTimes = pushTimes + 1;
//			String sql = SqlTemplate.UPDATE_INS_SLAVE_SQL + pushTimes
//						+" WHERE TASK_INSTANCE_ID = " 
//						+ taskInstanceID;
//			return sessionFactory.getCurrentSession().createSQLQuery(sql)
//					.executeUpdate();
//		}
//		return 0;
//	}
	
	
//	/**
//	 * 查询全部数据 SQL
//	 * 返回List 
//	 * */
//	public int updateByParentID(String parentID)
//	{
//		Session session = sessionFactory.getCurrentSession();
//		//更新为end 状态
//		String sql = SqlTemplate.UPDATE_TASK_BY_PARENTID 
//				+ ConstanceInfo.TASK_INSTANCE_STATUS_END
//				+ "',TASK_END_TIME = current_timestamp"
//				+ " WHERE INS_STATUS = '" + ConstanceInfo.TASK_INSTANCE_STATUS_HIVED
//				+ "' AND id in(" + parentID
//				+ ") ";
//		//更新从表
//		String sqlSlave = SqlTemplate.UPDATE_INS_SLAVE_STATUS_SQL + parentID + ")";
//		session.createSQLQuery(sqlSlave).executeUpdate();
//		return session.createSQLQuery(sql).executeUpdate();
//	}
	/**
	 * 查询全部数据 SQL
	 * 返回List 
	 * */
	public Object[] findObj(String Sql, String param){
		List<?> result = sessionFactory.getCurrentSession()
				.createSQLQuery(Sql).setString(0, param).list();
		if(null != result && result.size() > 0){
			return (Object[])result.get(0);
		}
		return null;
	}	

	
//	/**
//	 * 查询全部数据 SQL
//	 * 返回List 
//	 * */
//	public int updateProjectStatus(Integer projectID, String status)
//	{
//		Session session = sessionFactory.getCurrentSession();
//		//更新为end 状态
//		String sql = SqlTemplate.UPDATE_PROJECT_STATUS_SQL 
//				+ status
//				+ "' "
//				+ " WHERE ID = " + projectID;
//		return session.createSQLQuery(sql).executeUpdate();
//	}
	
}
