package com.esoft.dp.mgr.impl;

import java.util.List;

import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.dp.dao.DaoUtility;
import com.esoft.dp.entity.Ancestor;
import com.esoft.dp.entity.Task;
import com.esoft.dp.mgr.CommandMgr;
import com.esoft.dp.vo.ConstanceInfo;
/**
 * @author taoshi
 * 定期执行bat文件，进行数据load
 *
 */
public abstract class CommandExecMgrImpl implements CommandMgr{
	
	protected static Logger logger = LoggerFactory.getLogger(CommandExecMgrImpl.class);
	//command 要执行的命令绝对地址
	protected String cmdDirectory;
	//时间太长 - lzw
	protected int commandTimeOut = 60000000;
	
	protected DaoUtility daoUtility;
	
	public int prepare(Ancestor t) {
		Task sparkTask = (Task) t;
		String sql = "update TB_DAE_TARSK_INSTANCE t set t.INS_STATUS = '"
				+ ConstanceInfo.TASK_INSTANCE_STATUS_SPARK
				+ "' where t.INS_STATUS = '"
				+ ConstanceInfo.TASK_INSTANCE_STATUS_NEW + "' and t.id = "
				+ sparkTask.getId();
		logger.info("exec update sql:,{}", sql);
		sparkTask.setInsStatus(ConstanceInfo.TASK_INSTANCE_STATUS_SPARK);
		// 更新 记录状态
		return daoUtility.deleteWithSql(sql);
	}
	
	public abstract void generateCommand(Ancestor t);
	//查询要执行的任务
	private final String  sql = "from Task t join fetch t.taskInfo "
							+ "  i where t.insStatus = '" 
							+ ConstanceInfo.TASK_INSTANCE_STATUS_NEW + "'"
							//jobServer 任务堆满
							+ " and t.jobSendResult = '" + ConstanceInfo.JOB_SEND_FULL + "'";
//	private final String  sql = "from Task t join fetch t.taskInfo "
//			+ "  i where t.insStatus = '" 
//			+ ConstanceInfo.TASK_INSTANCE_STATUS_NEW + "'";
	@Override
	public void invoke(JobExecutionContext context) {
		//查询taskInstance 表中符合条件的记录
		List<?> taskList = daoUtility.findListHql(sql);
		logger.info(sql);
		logger.info("taskList size:{}",taskList.size());
		for (Object obj : taskList) {
			//循环获取符合条件的任务，批量执行
			Task t = (Task)obj;
			int updateCount = prepare(t);
			logger.info("updateCount:{}",updateCount);
			//调用spark 任务
			if(updateCount > 0) generateCommand(t);
		}
	}
	
	public void after(Ancestor t) {
		//没必要传task，只需要传taskId即可
		Task sparkTask = (Task) t;
		String sql = "update TB_DAE_TARSK_INSTANCE t set t.INS_STATUS = '" 
				+ ConstanceInfo.TASK_INSTANCE_STATUS_SPARKFAILED
			    + "' where t.INS_STATUS = '" 
				+ ConstanceInfo.TASK_INSTANCE_STATUS_SPARK
			    + "' and t.id = " + sparkTask.getId();
		logger.info("exec after sql:,{}",sql);
		//更新 记录状态
		daoUtility.deleteWithSql(sql);
	}

	public DaoUtility getDaoUtility() {
		return daoUtility;
	}

	public void setDaoUtility(DaoUtility daoUtility) {
		this.daoUtility = daoUtility;
	}

	public String getCmdDirectory() {
		return cmdDirectory;
	}

	public void setCmdDirectory(String cmdDirectory) {
		this.cmdDirectory = cmdDirectory;
	}

	
}
