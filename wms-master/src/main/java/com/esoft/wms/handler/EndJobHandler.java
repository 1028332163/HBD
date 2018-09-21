package com.esoft.wms.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.wms.WMSMain;
import com.esoft.wms.dao.DaoUtility;
import com.esoft.wms.entity.EndJob;
import com.esoft.wms.entity.JobNode;
import com.esoft.wms.entity.ReturnItem;
import com.esoft.wms.jobServer.JobSubmitter;
import com.esoft.wms.utils.ConstanceInfo;
import com.esoft.wms.utils.HandlerUtil;
import com.esoft.wms.utils.SqlTemplate;
import com.mysql.jdbc.StringUtils;

/**
 * @author asus
 * 
 */
public class EndJobHandler extends TimerTask {
	private final Logger logger = LoggerFactory.getLogger(EndJobHandler.class);

	private DaoUtility daoUtility;
	private TaskInsHandler taskInsHandler;
	private JobSubmitter jobSubmitter;

	public void setJobSubmitter(JobSubmitter jobSubmitter) {
		this.jobSubmitter = jobSubmitter;
	}

	public void setDaoUtility(DaoUtility daoUtility) {
		this.daoUtility = daoUtility;
	}

	public void setTaskInsHandler(TaskInsHandler taskInsHandler) {
		this.taskInsHandler = taskInsHandler;
	}

	@Override
	public void run() {
		try {

			Map<String, EndJob> broadList = this.getReturnList();
			logger.debug("timer startting:" + "push size:" + broadList.size()
					+ "  resource size:" + WMSMain.linkResources.size());
			for (Entry<String, EndJob> entry : broadList.entrySet()) {
				// 处理与job相关的工程和子job
				dealEndJob(entry.getKey(), entry.getValue());
			}
		} catch (Throwable e) {
			logger.error("timer exe error ", e);
		}
	}

	private void dealEndJob(String projectId, EndJob endJob) {
		// 标识job已经被处理，不要再被处理
		this.daoUtility.updateWithSql(String.format(
				SqlTemplate.UPDATE_JOB_SCAN_NUM, endJob.getJobUuid()));
		// 该job的子节点
		String children = (String) this.daoUtility.queryValue(String.format(
				SqlTemplate.QUERY_CHILDREN, endJob.getJobUuid()));
		List<ReturnItem> results = new ArrayList<ReturnItem>();
		results.add(endJob.getReturnVO());
		
		if (ConstanceInfo.TASK_INSTANCE_STATUS_HIVED.equals(endJob
				.getReturnVO().getStatus())) {// 任务执行成功 hived
			//执行子节点
			if (!StringUtils.isNullOrEmpty(children)) {
				StringTokenizer stk = new StringTokenizer(children, ",");
				while (stk.hasMoreTokens()) {
					String sonItemUuid = stk.nextToken();
					if (dealChild(endJob.getSerialId(), sonItemUuid)) {
						ReturnItem startJob = new ReturnItem();
						startJob.setItemUuid(sonItemUuid);
						startJob.setStatus(ConstanceInfo.TASK_INSTANCE_STATUS_NEW);
						results.add(startJob);
					}
				}
			}
			HandlerUtil.returnJson(projectId, results);
			dealProject(true, projectId, 1);
		} else {
			// 任务执行失败引发的工程执行失败
			JobNode rootJob = new JobNode();
			rootJob.setItemUuid(endJob.getItemUuid());
			rootJob.setSonItemUuid(children);
			rootJob.setHasScan(true);
			int treeNum = getTreeNum(endJob.getSerialId(), rootJob);
			logger.info("handle fail job,this job has " + treeNum + " children");
			HandlerUtil.returnJson(projectId, results);// 失败的job退给前台
			dealProject(true, projectId, treeNum);
		}

	}

	/**
	 * @param serialId
	 * @param sonItemUuid
	 * @return 此child是否可以执行
	 */
	private boolean dealChild(String serialId, String sonItemUuid) {
		Object[] condition = this.daoUtility.findRecord(String.format(
				SqlTemplate.QUERY_EXE_CONDITION, serialId, sonItemUuid));
		Short parentNum = (Short) condition[0];
		Short endParentNum = (Short) condition[1];
		if (parentNum - 1 == endParentNum) {// 子任务的执行就差当前这个父任务
			String taskInsJson = (String) condition[2];
			logger.info("to exe taskInstance json" + taskInsJson);
			taskInsHandler.handleTaskIns(taskInsJson);
			return true;
		} else {// 子节点的endParent+1
			String sonJobUuid = (String) condition[3];
			this.daoUtility.updateWithSql(String.format(
					SqlTemplate.UPDATE_END_PARENT_NUM, sonJobUuid));
			return false;
		}
	}

	/**
	 * 处理与job相关的project
	 * 
	 * @param isFailJob
	 * @param projectId
	 * @param decNum
	 *            :完成几个job
	 */
	private void dealProject(boolean isFailJob, String projectId, int decNum) {

		Object[] condition = this.daoUtility.findRecord(String.format(
				SqlTemplate.QUERY_PROJECT_CONDITION, projectId));
		Integer remainJob = (Integer) condition[0];
		int newRemainNum = remainJob - decNum;
		if (newRemainNum == 0) {// 工程执行完毕
			Integer hasFailJob = (Integer) condition[1];
			if (hasFailJob == 1 || isFailJob)
				dealEndProject(projectId, ConstanceInfo.PROJECT_STATUS_END);
			else
				dealEndProject(projectId, ConstanceInfo.PROJECT_STATUS_FAIL);
		}
		if (isFailJob) {// job执行失败
			// 工程的remainJob减去子树的数量,并标识工程hasFailJob执行失败;
			this.daoUtility.updateWithSql(String.format(
					SqlTemplate.FLAG_PROJECT_FAIL, newRemainNum, projectId));
		} else {// job执行成功
			// 工程的remainJob-1 ;
			this.daoUtility.updateWithSql(String.format(
					SqlTemplate.UPDATE_PROJECT_REMAIN_JOB, newRemainNum,
					projectId));
		}

	}

	private void dealEndProject(String projectId, String status) {
		logger.info("project:{};status:{}", projectId, status);
		// 更新工程状态
		this.daoUtility.updateWithSql(String.format(
				SqlTemplate.UPDATE_PROJECT_STATUS, status, projectId));
		// 如果是调优的工程则杀死调优时启动的context
		String result = jobSubmitter.killContext(projectId);
		logger.info("kill context result:{}", result);
		// 通知前台
		ReturnItem returnVO = new ReturnItem();
		returnVO.setTaskId(projectId);
		returnVO.setItemUuid("Project");
		returnVO.setStatus(ConstanceInfo.PROJECT_STATUS4PUSH_END);
		HandlerUtil.returnJson(projectId, new ReturnItem[] { returnVO });
	}

	/**
	 * 获取执行完毕，可以向前台推送的job
	 * 
	 * @return
	 */
	private Map<String, EndJob> getReturnList() {
		List<?> temp = this.daoUtility.findList(SqlTemplate.FIND_TASK_RETURN);
		Iterator<?> iter = temp.iterator();
		Map<String, EndJob> result = new HashMap<String, EndJob>();
		while (iter.hasNext()) {
			EndJob endJob = new EndJob();
			ReturnItem br = new ReturnItem();
			Object[] object = (Object[]) iter.next();
			br.setTaskId(object[1].toString());
			br.setItemUuid(object[2].toString());
			br.setStatus(object[3].toString());
			endJob.setReturnVO(br);
			endJob.setJobUuid(object[4].toString());
			endJob.setSerialId(object[5].toString());
			endJob.setItemUuid(object[2].toString());
			result.put(object[0].toString(), endJob);
		}
		return result;
	}

	private int getTreeNum(String serialId, JobNode rootJob) {
		int num = 1;
		List<JobNode> remainJobs = this.getRemainJobs(serialId);
		Queue<JobNode> tmpStack = new LinkedList<JobNode>();
		tmpStack.add(rootJob);
		while (tmpStack.size()>0) {
			JobNode fathJob = tmpStack.poll();
			String childrenStr = fathJob.getSonItemUuid();
			if (!StringUtils.isNullOrEmpty(childrenStr)) {
				String[] childrenItems = childrenStr.split(",");
				for (String childItem : childrenItems) {
					JobNode sonJob = null;
					for (JobNode job : remainJobs) {
						if (!job.isHasScan()
								&& job.getItemUuid().equals(childItem)) {// 该子节点还未被处理过
							sonJob = job;
							break;
						}
					}
					if (sonJob != null) {
						sonJob.setHasScan(true);
						this.daoUtility.updateWithSql(String.format(
								SqlTemplate.UPDATE_JOB_SCAN_NUM,
								sonJob.getJobUuid()));
						num++;
						tmpStack.add(sonJob);
					} else
						logger.debug(serialId
								+ "-"
								+ childItem
								+ " sonJob : scanNum has changed by other father");
				}
			}
		}
		return num;
	}

	/**
	 * 根据serialId查询此流水号中的所有job
	 * 
	 * @param serialId
	 * @return
	 */
	private List<JobNode> getRemainJobs(String serialId) {
		List<?> temp = this.daoUtility.findList(String.format(
				SqlTemplate.FIND_REMAIN_JOBS, serialId));
		Iterator<?> iter = temp.iterator();
		List<JobNode> result = new ArrayList<JobNode>();
		while (iter.hasNext()) {
			Object[] object = (Object[]) iter.next();
			JobNode jobNode = new JobNode();
			jobNode.setItemUuid(object[0].toString());
			jobNode.setSonItemUuid(object[1].toString());
			jobNode.setJobUuid(object[2].toString());
			jobNode.setHasScan(false);
			result.add(jobNode);
		}
		return result;
	}

}
