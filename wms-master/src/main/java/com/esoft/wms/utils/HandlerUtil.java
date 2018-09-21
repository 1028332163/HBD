package com.esoft.wms.utils;

import java.util.Comparator;

import javax.servlet.http.HttpSession;

import org.atmosphere.cpr.AtmosphereResource;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.esoft.wms.WMSMain;
import com.esoft.wms.entity.TaskInstance;

/**
 * 
 * @author taosh
 * 
 */
public class HandlerUtil {

	private HandlerUtil() {
	}

	private final static Logger logger = LoggerFactory
			.getLogger(HandlerUtil.class);

	public static boolean resComparator(Object[] userResource,
			TaskInstance taskInstance) {

		if (null == userResource) {
			logger.info("userResource is null, userid can not find");
			return false;
		}
		// 额定资源，大于这个资源，需要调优
		TaskInstance standerdTask = new TaskInstance();
		standerdTask.setDriverMem(String.valueOf(Config.props
				.get("jobServer.DriverMem")));
		standerdTask.setExecutorMem(String.valueOf(Config.props
				.get("jobServer.ExecutorMem")));
		standerdTask.setNumExecutors(String.valueOf(Config.props
				.get("jobServer.DriverMem")));

		// 额定资源,机构限制用户的资源
		TaskInstance userTask = new TaskInstance();
		if (null != userResource) {
			userTask.setDriverMem(userResource[0].toString());
			userTask.setExecutorMem(userResource[1].toString());
			userTask.setNumExecutors(userResource[2].toString());
		}
		// 判断用户页面设置资源超出额定资源
		int userResult = HandlerUtil.taskComparator.compare(taskInstance,
				userTask);
		logger.info("self set is larger than Rated(1:larger ;0:less):{}",
				userResult);

		if (userResult == 0) {
			// 小于限制资源 判断是否超过额定资源
			logger.debug("standerdTask{},{},{}", standerdTask.getDriverMem(),
					standerdTask.getExecutorMem(),
					standerdTask.getNumExecutors());
			int standerResult = HandlerUtil.taskComparator.compare(
					taskInstance, standerdTask);
			if (standerResult == 1) {
				// 发启调优任务调度 创建调优资源的context
				return true;
			}
		}
		return false;
	}

	// 定义资源比较器 1大于 0 小于
	public static Comparator<TaskInstance> taskComparator = new Comparator<TaskInstance>() {
		@Override
		public int compare(TaskInstance o1, TaskInstance o2) {
			if (Integer.parseInt(o1.getDriverMem().replace("g", "")) >= Integer
					.parseInt(o2.getDriverMem().replace("g", ""))
					&& Integer.parseInt(o1.getExecutorMem().replace("g", "")) >= Integer
							.parseInt(o2.getExecutorMem().replace("g", ""))
					&& Integer.parseInt(o1.getNumExecutors()) >= Integer
							.parseInt(o2.getNumExecutors())) {
				return 1;
			} else {
				return 0;
			}
		}
	};

	// session 中获取lastPoint，用于完成流程
	public static String checkLastPoint(AtmosphereResource resource) {
		if (resource == null)
			return null;

		HttpSession s = resource.session(true);
		if (s != null) {
			return (String) s.getAttribute("lastPoint");
		}
		return null;
	}

//	// 从session 中取出 用户ID
//	public static String getUserId(TaskInstance taskInstance) {
//
//		return null;
//	}

	// 向project的前台页面
	public static void returnJson(String projectId, Object returnVO) {
		AtmosphereResource resource = WMSMain.linkResources.get(projectId);
		if (resource != null)
			try {
				resource.getResponse().write(
						new JsonIoConvertor().write(returnVO));
				logger.info("send start");
			} catch (Throwable e) {
				logger.error("write broadResult to json error", e);
			}
	}
	/**
	 * @author taoshi
	 * 返回需要执行的job数量
	 * @param exeJobs
	 * @param waitJobs
	 * @return
	 */
//	public static int countNExecNum(List<Job> exeJobs, List<Job> waitJobs){
//		int totalCount = 0;
//		if(null != exeJobs && null != waitJobs){
//			for(Job job: exeJobs){
//				if(job.getItemUuid().contains(ConstanceInfo.DATASOUCE_TASK_NAME))
//					return (exeJobs.size() + waitJobs.size()) -1;
//				else totalCount = getChildrenNum(waitJobs, job.getSonItemUuid(), 0);
//			}
//		}
//		return totalCount;
//	}
	

	
	public static void main(String args[]) throws Throwable{
//		Scanner s = new Scanner(System.in);
//		String message = s.nextLine();
////		String message = "{'exeJobs':[{'jobUuid':'ece424a5-7f2b-46ec-ab8d-c60a22151fcd','itemUuid':'DataBase-1','sonItemUuid':'Histogram-1,AttributeSelectionPrincipalComponent-1','taskInsJson':'{\"driverMem\":\"3g\",\"executorMem\":\"3g\",\"numExecutors\":\"3\",\"name\":\"zxxxxxx\",\"taskId\":\"9\",\"projectId\":\"675\",\"itemUuid\":\"DataBase-1\",\"jobUuid\":\"ece424a5-7f2b-46ec-ab8d-c60a22151fcd\",\"alArg\":\"{\"head\":\"`numpregnancies` double,`pg2` double,`dbp` double,`tsft` double,`si2` double,`bmi` double,`dpf` double,`age` double,`class` double,`age1` double\",\"minNonNullPro\":\"0.5\",\"sep\":\",\",\"whetherHeader\":\"1\",\"inputPath\":\"/out/20170414/correlations_1492155633135.csv\",\"rightInputPath\":\"\",\"outputPath\":\"out/20170414/p675_1492158031989.parquet\",\"rightOutputPath\":\"\",\"alPath\":\"\"}\"}','parentNum':'0'}],'waitJobs':[{'jobUuid':'efed872b-3bad-4457-ac9e-1d0898f52910','itemUuid':'Histogram-1','sonItemUuid':'','taskInsJson':'{\"driverMem\":\"3g\",\"executorMem\":\"3g\",\"numExecutors\":\"3\",\"name\":\"????-1\",\"taskId\":\"76\",\"projectId\":\"675\",\"itemUuid\":\"Histogram-1\",\"jobUuid\":\"efed872b-3bad-4457-ac9e-1d0898f52910\",\"alArg\":\"{\"featuresCol\":\"numpregnancies\",\"inputPath\":\"out/20170414/p675_1492158031989.parquet\",\"outputPath\":\"out/20170414/p675_1492158038040.parquet\"}\"}','parentNum':'1'},{'jobUuid':'1d1e3199-5805-49ac-b155-64b22499fcd4','itemUuid':'AttributeSelectionPrincipalComponent-1','sonItemUuid':'','taskInsJson':'{\"driverMem\":\"3\",\"executorMem\":\"3\",\"numExecutors\":\"3\",\"name\":\"???????-1\",\"taskId\":\"44\",\"projectId\":\"675\",\"itemUuid\":\"AttributeSelectionPrincipalComponent-1\",\"jobUuid\":\"1d1e3199-5805-49ac-b155-64b22499fcd4\",\"alArg\":\"{\"colNamesPara\":\"numpregnancies,pg2,dbp,tsft,si2,bmi,dpf,age,class,age1\",\"kPara\":\"5\",\"inputPath\":\"out/20170414/p675_1492158031989.parquet\",\"rightInputPath\":\"\",\"outputPath\":\"out/20170415/p675_1492240298500.parquet\",\"rightOutputPath\":\"\",\"alPath\":\"\"}\"}','parentNum':'1'}]}";
//		ReceiveVO receiveInfo = (ReceiveVO) new JsonIoConvertor().read(message,
//				ReceiveVO.class);
//		List<Job> exeJobs = receiveInfo.getExeJobs();
//		List<Job> waitJobs = receiveInfo.getWaitJobs();
//		List<Job> childrenJobs = new ArrayList<Job>();
//		List<Job> cc = getChildrenNum(waitJobs,"ScatterDiagram-2,Histogram-1", childrenJobs);
//		
//		for (Job job : cc) {
//			System.out.println(job.getItemUuid());
//		}
		
//		System.out.println(countNExecNum(exeJobs, waitJobs));
		
//		List list = new ArrayList();
//		
//		list.add(1+"");
//		list.add(2+"");
//		list.add(3+"");
//		list.add(4+"");
//		list.remove(2+"");
//		for (Object object : list) {
//			System.out.println(object);
//		}
		
	}

}
