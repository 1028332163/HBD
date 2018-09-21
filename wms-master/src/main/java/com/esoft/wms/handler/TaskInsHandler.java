package com.esoft.wms.handler;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.wms.dao.DaoUtility;
import com.esoft.wms.entity.TaskInfo;
import com.esoft.wms.entity.TaskInstance;
import com.esoft.wms.jobServer.JobSubmitter;
import com.esoft.wms.utils.HandlerUtil;
import com.esoft.wms.utils.JsonIoConvertor;
import com.esoft.wms.utils.SqlTemplate;
import com.esoft.wms.vo.TurningResult;

public class TaskInsHandler {
	private final Logger logger = LoggerFactory.getLogger(TaskInsHandler.class);

	private DaoUtility daoUtility;

	private JobSubmitter jobSubmitter;

	public void setDaoUtility(DaoUtility daoUtility) {
		this.daoUtility = daoUtility;
	}

	public void setJobSubmitter(JobSubmitter jobSubmitter) {
		this.jobSubmitter = jobSubmitter;
	}

	public void handleTaskIns(String taskInsJson) {
		try {
			// 初始化任务对象并加载任务的执行类包等信息
			TaskInstance taskInstance = (TaskInstance) new JsonIoConvertor()
					.read(taskInsJson, TaskInstance.class);
			TaskInfo info = (TaskInfo) daoUtility.findListHql(
					SqlTemplate.FIND_TASKINFO_PARAM_BY_SQL
							+ taskInstance.getTaskId()).get(0);
			// // 告诉页面此item已经开始，item的图标开始转圈
			// ReturnVO returnVO = new ReturnVO();
			// returnVO.setItemUuid(taskInstance.getItemUuid());
			// returnVO.setStatus(ConstanceInfo.TASK_INSTANCE_STATUS_NEW);
			// HandlerUtil.returnJson(taskInstance.getProjectId(), returnVO);
			// 将taskInstance 添加到数据库
			daoUtility.saveTaskIns(taskInstance, info);
			// 如果调优则启动新的context
			TurningResult rt = hasTurning(info, taskInstance);
			if (null != rt)
				logger.info("turningResult:{}", rt.toString());
			// 提交任务
			jobSubmitter.submit(info, taskInstance,
					rt == null ? null : rt.getContextName());
		} catch (Throwable e) {
			logger.error("error when resolve taskInsJson ", e);
		}

	}

	// 判断该任务是否进行了调优
	private TurningResult hasTurning(TaskInfo info, TaskInstance taskInstance) {
		TurningResult turnResponse = new TurningResult();
		// 查询记录的contextName 是否为空
		List<?> contextName = daoUtility
				.findList(String.format(SqlTemplate.QUERY_PROJECT_CONTEXT,
						taskInstance.getProjectId()));
		logger.info("contextName.size:{}", contextName.size());
		// 查询出context，直接运行
		if (null != contextName && contextName.size() > 0) {
			for (Object context : contextName) {
				if (null != context) {
					logger.info(context.toString());
					turnResponse.setContextName(String.valueOf(context));
					return turnResponse;
				}
			}
		}
		// String userId = HandlerUtil.getUserId(taskInstance);
		// logger.info(userId);
		// 根据projectId查询userid，根据userId查询用户标配资源
		logger.info(String.format(SqlTemplate.QUERY_USER_RES, taskInstance.getProjectId()));
		Object[] userResource = daoUtility.findRecord(String.format(
				SqlTemplate.QUERY_USER_RES, taskInstance.getProjectId()));

		// 判断 当前资源申请是否超过限制
		if (HandlerUtil.resComparator(userResource, taskInstance)) {
			// 发启调优任务调度 创建调优资源的context
			String cName = jobSubmitter.createContext(info, taskInstance);
			logger.info("cName:{}", cName);
			if (null != cName) {
				turnResponse.setSucc(true);
				turnResponse.setContextName(cName);
				return turnResponse;
			}
		}
		return null;
	}

	// // 根据parentID 更新 父记录状态
	// private void updateParentID(String id) {
	// logger.debug("task.getParentID() ===>{}", id);
	// if (null != id) {
	// daoUtility.updateByParentID(id);
	// }
	// }
}
