package com.esoft.dp.mgr.impl;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esoft.dp.dao.DaoUtility;
import com.esoft.dp.mgr.PushMgr;
import com.esoft.dp.utils.DateUtil;

public class PushMgrImpl implements PushMgr{
	
	private static Logger logger = LoggerFactory.getLogger(PushMgrImpl.class);
	
	private Integer threshold;
	
	private ArrayList list;
	//推 次数
	private Integer times;
	
	private DaoUtility daoUtility;
	
	private static final String sql = " SELECT t.ID, s.PUSH_TIME, s.PUSH_TIMES, s.STATUS, t.INS_STATUS "
									+"  FROM TB_DAE_TARSK_INSTANCE t JOIN TB_DAE_TASK_INS_SLAVE s"
									+ " WHERE t.ID = s.TASK_INSTANCE_ID AND s.`STATUS` = 1 "
									+ " AND t.INS_STATUS IN ('hived','sparkfal','hivefal')"
									+ " AND s.PUSH_TIMES < ";
	
	private static final String UPDATE_TASK_BY_PARENTID = "UPDATE TB_DAE_TASK_INS_SLAVE "
										+ " SET `STATUS` = 0 "
										+ " WHERE `STATUS` = 1 AND TASK_INSTANCE_ID =";
	
	@Override
	public void invoke(JobExecutionContext context) {
		// TODO Auto-generated method stub
		 logger.info("begin PushMgr......");
		 List<?> result = prepare();
		 logger.info("size is {}", result.size());
		 Iterator<?> iter = result.iterator();
		 while (iter.hasNext()) {
			Object[] object = (Object[]) iter.next();
			int pushTimes = Integer.parseInt(object[2].toString());
			logger.info("pushTimes:{},ID:{}", pushTimes,object[0].toString());
			checkTimeOut(object);
		}
	}
	//检查时间超时，设置重推
	private void checkTimeOut(Object[] object){
		try {
			Date pushTime = DateUtil.toDate(object[1].toString(), 
					DateUtil.DATE_TIME_FORMAT_4SS);
			Date current = new Date();
			logger.info("current:{}", DateUtil.toString(current, DateUtil.DATE_TIME_FORMAT_4SS));
			long timeInteval = new Long(current.getTime() - pushTime.getTime());
			logger.info("timeInteval:{}", timeInteval);
			//推送时间指此时大于阀值
			if(timeInteval > threshold){
				//推送次数加1 状态置为未推送
				int re = daoUtility
						.deleteWithSql(UPDATE_TASK_BY_PARENTID + object[0].toString());
				logger.info("re:{}",re);
			}
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			logger.error("parse push Time error", e);
		}
	}
	@Override
	public List<?> prepare() {
		return daoUtility.queryWithSQL(sql + times);
	}
	
	public void setThreshold(Integer threshold) {
		this.threshold = threshold;
	}
	public void setDaoUtility(DaoUtility daoUtility) {
		this.daoUtility = daoUtility;
	}
	public void setTimes(Integer times) {
		this.times = times;
	}

}
