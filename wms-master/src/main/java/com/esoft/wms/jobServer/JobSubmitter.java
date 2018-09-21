package com.esoft.wms.jobServer;

import java.util.List;

import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.wms.dao.DaoUtility;
import com.esoft.wms.entity.JobServerMsg;
import com.esoft.wms.entity.TaskInfo;
import com.esoft.wms.entity.TaskInstance;
import com.esoft.wms.utils.CommonUtils;
import com.esoft.wms.utils.ConstanceInfo;
import com.esoft.wms.utils.JsonIoConvertor;
import com.esoft.wms.utils.SocketClient;
import com.esoft.wms.utils.SqlTemplate;
import com.esoft.wms.vo.SchedulerVO;

/**
 * 
 * @author taos jobServer 提交
 * 
 */
public class JobSubmitter {
	private final Logger logger = LoggerFactory.getLogger(JobSubmitter.class);

	private SocketClient client;
	private DaoUtility daoUtility;

	/**
	 * @modify by taos
	 * @param info
	 *            : 任务信息
	 * @param taskInstance
	 *            : 任务实例
	 * @param withParent
	 *            : 是否继承对象
	 * @param withContext
	 *            : 调优任务不使用context
	 */
	public void submit(TaskInfo info, TaskInstance taskInstance,
			String contextName) {
		// 将任务添加到jobserver的context
		String reqURL = JobServerUtil.generateReqURL(info.getTaskClass(),
				contextName);
		logger.info("reqURL:" + reqURL);
		String reqBody = "input.string = "
				+ CommonUtils.appendQuote(JobServerUtil
						.generateReqBody(taskInstance));
		logger.info("reqBody:" + reqBody);

		String msg = JobServerUtil.sendPostReq(reqURL, reqBody);
		logger.info("msg from jobserver:" + msg);
		// 分析结果
		if (!ConstanceInfo.JOB_SERVER_NO_RESPONSE.equals(msg)
				&& !ConstanceInfo.JOB_SERVER_NO_START.equals(msg)) {
			JobServerMsg serverResponse = null;
			try {
				serverResponse = ((JobServerMsg) new JsonIoConvertor().read(
						msg, JobServerMsg.class));
			} catch (Throwable e) {//
				this.recFull(taskInstance.getId());// 让job可以通过exec上传
				logger.error(
						"error stringToVO after getting msg from jobserver:", e);
			}
			logger.info("status from jobserver:{}", serverResponse.getStatus());

			if (ConstanceInfo.STARTED_JOBSERVER.equals(serverResponse
					.getStatus())
					|| ConstanceInfo.INIT_SUCC_JOBSERVER.equals(serverResponse
							.getStatus())) {
				// 成功将任务提交到jobserver
				this.recSuccess(taskInstance.getId());
				String jobId = serverResponse.getJobId();
				logger.info("jobId:{}", jobId);
				if (StringUtils.isNotBlank(contextName)) {
					int returnExecSQL = daoUtility.updateWithSql(String.format(
							SqlTemplate.UPDATE_PROJECT_CONTEXTNAME,
							contextName, taskInstance.getProjectId()));
					logger.info("returnExecSQL:{}", returnExecSQL);
				}
				// 更新jobId 到DB
				int returnCode = daoUtility.updateWithSql(String.format(
						SqlTemplate.UPDATE_JOBID, jobId, taskInstance.getId()));
				logger.info("returnCode:{}", returnCode);
			} else if (ConstanceInfo.ERROR_JOBSERVER.equals(serverResponse
					.getStatus())
					|| ConstanceInfo.TOO_MANY_JOBD.equals(serverResponse
							.getStatus())) {
				// 将任务提交到jobserve失败，需要触发quartz的trigger
				recFull(taskInstance.getId());
			} else if (ConstanceInfo.LOADING_FAILED_JOBSERVER
					.equals(serverResponse.getStatus())
					|| ConstanceInfo.FAIL_JOBSERVER.equals(serverResponse
							.getStatus())) {
				// 返回结果为fail
				this.recFail(taskInstance.getId());
			} else {
				logger.error("receive unexpected msg from job server:" + msg);
			}
		} else {
			logger.warn("jobserver service missing! ");
			this.recFull(taskInstance.getId());
		}
	}

	/**
	 * @author taosh 创建新Context
	 * @param info
	 *            :任务
	 * @param task
	 *            :任务实例
	 */
	public String createContext(TaskInfo info, TaskInstance taskInstance) {
		logger.info("createContext");
		String contextName = "context" + System.currentTimeMillis();
		// 拼串
		String reqURL = JobServerUtil.generateContextURL(info.getTaskClass(),
				taskInstance, contextName);
		logger.info(reqURL);
		// 创建jobServer context
		String msg = JobServerUtil.sendPostReq(reqURL, null);
		logger.info("msg from jobserver:" + msg);
		if (ConstanceInfo.JOB_SERVER_NO_RESPONSE.equals(msg)) {
			return null;
		}
		// 解析调用结果
		if (analyzeResult(msg))
			return contextName;
		else
			return null;
	}

	/**
	 * @author taos 杀掉进程
	 * @param task
	 *            :taskIns
	 * @return
	 */
	public String killContext(String projectId) {
		logger.info(projectId);
		List<?> result = daoUtility.findList(String.format(
				SqlTemplate.QUERY_PROJECT_CONTEXT, projectId));
		if(null != result && result.size() > 0) {
			logger.info("result size:{}", result.size());
			Object context = result.get(0);
			if (null != context) {
				String contextName = context.toString();
				String reqURL = JobServerUtil.generateKillContextURL(contextName);
				logger.info("reqURL:{}", reqURL);
				String msg = JobServerUtil.sendDelReq(reqURL, null);
				if (analyzeResult(msg))
					return ConstanceInfo.KILL_CONTEXT_SUCC;
				else
					return ConstanceInfo.KILL_CONTEXT_RES_FAIL;
			}
		}
		return ConstanceInfo.KILL_CONTEXT_RES_NO_CONTEXT;
	}

	/**
	 * @author:taosh 分析调用接口结果
	 * @param msg
	 *            ：调用接口返回json
	 * @param task
	 *            ：任务
	 * @param contextName
	 *            ：context名称，用于启停context
	 */
	private boolean analyzeResult(String msg) {
		logger.info("msg from jobserver:" + msg);
		if (!ConstanceInfo.JOB_SERVER_NO_RESPONSE.equals(msg)
				&& !ConstanceInfo.JOB_SERVER_NO_START.equals(msg)) {
			JobServerMsg serverResponse = null;
			try {
				serverResponse = ((JobServerMsg) new JsonIoConvertor().read(
						msg, JobServerMsg.class));
				if (ConstanceInfo.STARTED_JOBSERVER.equals(serverResponse
						.getStatus())
						|| ConstanceInfo.INIT_SUCC_JOBSERVER
								.equals(serverResponse.getStatus())) {
					// 成功的状态
					return true;
				} else {
					// 失败的状态
					return false;
				}
			} catch (Throwable e) {
				// json解析失败
				logger.error(
						"error stringToVO after getting msg from jobserver:", e);
				return false;
			}
		} else {
			// jobserver 无反应
			return false;
		}

	}

	private String generateReqContent() throws Throwable {
		SchedulerVO cmd = new SchedulerVO();
		cmd.setOperation("exeJob");
		cmd.setJobDetailGroup("DEFAULT");
		cmd.setJobDetailName("featureExpclusterJobDetail");
		return new JsonIoConvertor().write(cmd);
	}

	/**
	 * @author lzw job server 队列已满
	 * @param jobId
	 */
	private void recFull(Integer jobId) {
		String result;
		try {
			// 将task的JOB_SEND_RESULT置为full
			String sql = "update TB_DAE_TARSK_INSTANCE t set t.JOB_SEND_RESULT = '"
					+ ConstanceInfo.JOB_SEND_FULL + "' where t.id = " + jobId;
			logger.info("update job state after recFull:,{}", sql);
			daoUtility.updateWithSql(sql);// 更新操作
			// 让提交spark任务的trigger被触发
			result = client.send(generateReqContent());
			logger.debug("call just now:{}", result);
		} catch (Throwable e) {
			logger.error("call quartz api fail after jobserver:", e);
		}
	}

	private void recSuccess(Integer jobId) {
		// do nothing
	}

	/**
	 * @author lzw 处理提交job server 出错的情况
	 * @param jobId
	 */
	private void recFail(Integer jobId) {
		// // 将taskId的数据库记录置为sparkal
		String sql = "update TB_DAE_TARSK_INSTANCE t set t.INS_STATUS = '"
				+ ConstanceInfo.TASK_INSTANCE_STATUS_SPARKFAILED
				+ "' where t.id = " + jobId;
		logger.info("update job state after recFail:,{}", sql);
		daoUtility.updateWithSql(sql);// 更新操作
	}

	// // 将任务放到未成功的队列里面等待
	// String sql = String
	// .format("INSERT INTO TB_DAE_HTTP_FAIL_TASK  VALUES(%s,\"%s\",\"%s\",current_timestamp)",
	// jobId, reqURL, reqBody);
	// daoUtility.deleteWithSql(sql);
	public void setClient(SocketClient client) {
		this.client = client;
	}

	public void setDaoUtility(DaoUtility daoUtility) {
		this.daoUtility = daoUtility;
	}
}
