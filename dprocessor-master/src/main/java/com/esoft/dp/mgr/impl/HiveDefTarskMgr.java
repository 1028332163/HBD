package com.esoft.dp.mgr.impl;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.codehaus.plexus.util.StringUtils;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.dp.dao.HiveDaoUtility;
import com.esoft.dp.entity.Task;
import com.esoft.dp.entity.TaskInstanceData;
import com.esoft.dp.utils.DateUtil;
import com.esoft.dp.utils.HDFSUtil;
import com.esoft.dp.utils.InterpreterResult;
import com.esoft.dp.utils.InterpreterResult.Code;
import com.esoft.dp.vo.ConstanceInfo;

public class HiveDefTarskMgr extends HiveTaskMgr {

	private static Logger logger = LoggerFactory
			.getLogger(HiveDefTarskMgr.class);
	private HiveDaoUtility hiveDaoUtility;
	
	private HDFSUtil hdfs;
	
	private String checkPrefix;

	// 查询要执行的任务
	private final String sql = "from Task t join fetch t.taskInfo ti  "
			+ "  where t.insStatus = '"
			+ ConstanceInfo.TASK_INSTANCE_STATUS_SPARKED + "'";
	private final String sqlData = "from TaskInstanceData t  "
			+ " where t.taskID = '";

	@Override
	public void invoke(JobExecutionContext context) {
		try {
			logger.info("check data dir...");
			checkRequired();
			logger.info("check data dir..end.");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("hdfsUtil.changePermission",e);
		}
		// 查询taskInstance 表中符合条件的记录
		logger.debug("HiveDefTarskMgr begin .......");
		List<?> taskList = daoUtility.findListHql(sql);
		logger.debug("taskList size:{}",taskList.size());
		for (Object obj : taskList) {
			// 循环获取符合条件的任务，批量执行
			Task t = (Task) obj;
			// t.getId();
			add(t);
		}
	}
	/**
	 * 检测HDFS 路径是否存在
	 * @throws IOException 
	 */
	private void checkRequired()throws IOException{
		//获取今天文件夹
		String today = DateUtil.toString(new Date(), DateUtil.DATE_FORMAT_4YMD);
		String target = checkPrefix + today;
		if(!hdfs.check(target)) {
			//创建自己的文件并给予777
			hdfs.mkdir(target);
			Path hdfsFile = new Path(target);
			hdfs.changePermission(hdfsFile,"777");
		}
	}
	/**
	 * @author taoshi
	 * 使用impala 查看数据，建立hive 表时，
	 * 需要增加hive表的库名 “this.hiveDbName”
	 */
	@Override
	public void add(Task instance) {
		update(instance, ConstanceInfo.TASK_INSTANCE_STATUS_HIVING,ConstanceInfo.TASK_INSTANCE_STATUS_SPARKED, null);
		//查询task instance 的附属信息
		List<?> taskList = daoUtility.findListHql(sqlData + instance.getId() + "'");
		//创建hive 表
		try {
			if(prepare(taskList, instance)){
				logger.info("titleContent:{}",instance.getTitleContent());
				//判断为nohead，则直接更新为hived
				if(checkNoHead(instance.getTitleContent())) {
					update(instance,ConstanceInfo.TASK_INSTANCE_STATUS_HIVED,ConstanceInfo.TASK_INSTANCE_STATUS_HIVING, null);
					return;
				}
				//执行建表
				InterpreterResult result = exec(instance.getTableName(), 
						instance.getTitleContent(), instance.getSparkResult());
				if(result.code() == InterpreterResult.Code.ERROR){
					update(instance,ConstanceInfo.TASK_INSTANCE_STATUS_HIVEFAL,ConstanceInfo.TASK_INSTANCE_STATUS_HIVING, result.message());
					logger.info("instance data create hive table has error!");
				} 
			} else {
				update(instance,ConstanceInfo.TASK_INSTANCE_STATUS_HIVEFAL,ConstanceInfo.TASK_INSTANCE_STATUS_HIVING, null);
				logger.info("instance data create hive table has error!");
			}
		} catch (Exception e) {
			logger.error("HiveDefTarskMgr->add",e);
			update(instance,ConstanceInfo.TASK_INSTANCE_STATUS_HIVEFAL,ConstanceInfo.TASK_INSTANCE_STATUS_HIVING, e.getMessage());
		}
		//更新为成功
		update(instance,ConstanceInfo.TASK_INSTANCE_STATUS_HIVED,ConstanceInfo.TASK_INSTANCE_STATUS_HIVING, null);
	}


	@Override
	public void update(Object t,String beginStatus,String endStatus, String errorHint) {
		if(StringUtils.isNotBlank(errorHint)){
			errorHint = errorHint.replaceAll(":", " ");
		}
		
		StringBuilder sb = new StringBuilder("update");
		Integer targetID = null;
		if(t instanceof Task){
			targetID = ((Task) t).getId();
			sb.append(" TB_DAE_TARSK_INSTANCE t set t.INS_STATUS = '");
			sb.append(beginStatus);
			sb.append("',t.HIVE_CREATE_TIME = current_timestamp");
			if(StringUtils.isNotBlank(errorHint)){
				sb.append(",t.ERROR_HINT=\"" + errorHint + "\"");
			}
			sb.append(" where t.INS_STATUS = '");
		} else {
			targetID = ((TaskInstanceData) t).getId();
			sb.append(" TB_DAE_TARSK_INSTANCE_DATA t set t.HIVE_STATUS = '");
			sb.append(beginStatus);
			sb.append("' ");
			sb.append(" where t.HIVE_STATUS = '");
		}
		sb.append(endStatus);
		sb.append("' and t.id =");
		sb.append(targetID);
		logger.info("exec after sql:,{}",sb.toString());
		//更新 记录状态
		super.daoUtility.deleteWithSql(sb.toString());
	}

	/**
	 * 将taskinstanceData 中的数据全部更新
	 */
	@Override
	public boolean prepare(Object obj,Task instance) {
		List<?> set = (List<?> ) obj;
		if(set == null || set.size() == 0) return true;
		for (Object object : set) {
		   TaskInstanceData tid = (TaskInstanceData)object;
		   try {
			    //判断为nohead 则，本条记录略过，直接更新为hived
			    if(!checkNoJSON(tid.getSummaryJsonContent())) {

			    	update(tid,ConstanceInfo.TASK_INSTANCE_DATA_STATUS_OK,
							ConstanceInfo.TASK_INSTANCE_DATA_STATUS_NEW, null);
			    	continue;
			    	// 等于no json 则创建hive 表
			    } else {
			    	update(tid, ConstanceInfo.TASK_INSTANCE_DATA_STATUS_HIVING,ConstanceInfo.TASK_INSTANCE_DATA_STATUS_NEW, null);
					String tableName = "s" + instance.getProjectID() + "_" + System.currentTimeMillis();
					//执行 创建hive表
					InterpreterResult result = exec(tableName, 
							tid.getTitleContent(), tid.getSparkResult());
					tid.setTableName(tableName);
					//判断结果如果是失败，则更新状态
					if(result.code() == InterpreterResult.Code.ERROR){
						update(tid,ConstanceInfo.TASK_INSTANCE_DATA_STATUS_FAIL,
									ConstanceInfo.TASK_INSTANCE_DATA_STATUS_HIVING, result.message());
						//提示 外层记录，记为失败
						return false;
					} else { //将缺失字段更新到DB
						mergeDataIntoDB(tid, instance);
					}
			    }
			} catch (Exception e) {
				logger.error("HiveDefTarskMgr->add",e);
				update(tid,ConstanceInfo.TASK_INSTANCE_DATA_STATUS_FAIL,ConstanceInfo.TASK_INSTANCE_DATA_STATUS_HIVING,null);
				return false;
			}
		}
		return true;
	}
	
	private void mergeDataIntoDB(TaskInstanceData td,Task instance){
		//查找projectID 和 projectName
		String queryProjectSQL = "SELECT NAME FROM TB_DAE_PROJECT_INFO P WHERE P.ID = '"
		+ instance.getProjectID() + "'";
		String projectName = (String)
				super.daoUtility.findListSql(queryProjectSQL);
		StringBuilder sb = new StringBuilder("update");
		//更新为hived
		sb.append(" TB_DAE_TARSK_INSTANCE_DATA t set t.HIVE_STATUS = '");
		sb.append(ConstanceInfo.TASK_INSTANCE_DATA_STATUS_OK);
		sb.append("',t.HIVE_CREATE_TIME = current_timestamp ");
		sb.append(",t.NAME = \"");
		//TASK_INSTANCE NAME
		sb.append(instance.getName());
		sb.append("\",t.PROJECT_NAME = '");
		sb.append(projectName);
		sb.append("',t.TABLE_NAME = '");
		sb.append(td.getTableName());
		sb.append("',t.PROJECT_ID = '");
		sb.append(instance.getProjectID());
		sb.append("'");
		sb.append(" where t.HIVE_STATUS = '");
		sb.append(ConstanceInfo.TASK_INSTANCE_DATA_STATUS_HIVING);
		sb.append("' and t.id =");
		sb.append(td.getId());
		logger.info("exec after sql:,{}",sb.toString());
		//更新 记录状态
		super.daoUtility.deleteWithSql(sb.toString());
	}
	/**
	 * @author:taoshi
	 * 拼接sql 并创建hive 表
	 * @param tableName
	 * @param titleContent
	 * @param sparkResult
	 */
	private InterpreterResult exec(String tableName, 
			String titleContent,String sparkResult){
		//扩大文件权限
		Path hdfsFile = new Path(sparkResult);
		logger.info("file begin to change permission:{}", sparkResult);
		try {
			hdfs.changePermission(hdfsFile,"777");
		} catch (IOException e) {
			logger.error("change file op permission error", e);
			 InterpreterResult rett = new InterpreterResult(Code.ERROR, e.toString());
			 return rett;
		}
		logger
		.info("tableName:{},titleContent:{},sparkResult:{}",tableName,titleContent,sparkResult);
		String sql = "create external table " +this.hiveDbName+"."+tableName + "  ("
				+ titleContent
				+ ")   row format delimited fields terminated by ','"
				+ "stored as parquet "
				+ "location '" + sparkResult + "'";
		logger.info(sql);
		logger.info("create==========================================================");
		return hiveDaoUtility.execute(sql);
	}
	
	public void setHiveDaoUtility(HiveDaoUtility hiveDaoUtility) {
		this.hiveDaoUtility = hiveDaoUtility;
	}
	public static final String nohead = "`no` head";
	
	public static final String nojson = "no json";
	
	private static boolean checkNoHead(String target){
		if(nohead.equals(target)) return true;
		return false;
	}
	private static boolean checkNoJSON(String target){
		if(nojson.equals(target)) return true;
		return false;
	}
	public void setHdfs(HDFSUtil hdfs) {
		this.hdfs = hdfs;
	}
	public void setCheckPrefix(String checkPrefix) {
		this.checkPrefix = checkPrefix;
	}

}
