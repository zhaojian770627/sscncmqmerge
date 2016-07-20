package ssc.util.mq;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import nc.bs.dao.BaseDAO;
import nc.bs.dao.DAOException;
import nc.jdbc.framework.SQLParameter;
import nc.jdbc.framework.processor.ResultSetProcessor;
import nc.vo.sscmq.MqtaskVO;

/**
 * 
 * @author zhaojianc
 * 
 */
public class MQTaskManagerImpl implements IMQTaskManager {

	@Override
	public String insert(MqtaskVO taskVO) throws DAOException {
		// N 表示空
		taskVO.setTaskid("N");
		// R 表示待发送
		taskVO.setMsgstatus("R");
		taskVO.setStatus(2);
		String id = new BaseDAO().insertVOWithPK(taskVO);
		return id;
	}

	/**
	 * 执行步骤
	 * 
	 * 1.执行数据库锁 2.从数据库中获取还没有设置taskID的任务，并设置taskID字段为taskID，暂时设定每次取100条
	 * 3.将这些任务返回，用于发消息
	 * 
	 * 此操作是幂等的，可以考虑在调用前后，不用加数据库锁，因为消息同时只能被同一个消费者消费、
	 * 先针对消息标志查询是否有已经发配的任务，如果有的话直接返回，否则，重新分配一批进行发送。
	 * 
	 * @throws DAOException
	 */
	@Override
	public MqtaskVO[] getPendingMqTask(String taskID) throws DAOException {
		BaseDAO dao = new BaseDAO();
		// 在tasklock中表中预置LOCK字段，进行数据库锁，防止同时取任务
		String lockSql = "update mq_tasklock set taskstatus='0' where pk ='MQTASK_getPendingMqTask'";
		// 锁定本行
		int c = dao.executeUpdate(lockSql);

		// 先进行查询，如果存在则发送，下面的代码段可以去掉,程序再次执行，表明消息没发送成功，先从数据库查询本消息
		// 附带的任务，如果没有在取一批，否则只发送本批次
		String select = "select msgid,msgcontent from mq_task where taskid='"
				+ taskID + "'";

		ResultSetProcessor processor = new ResultSetProcessor() {

			@Override
			public Object handleResultSet(ResultSet rs) throws SQLException {
				List<MqtaskVO> lstTask = new ArrayList<MqtaskVO>();
				while (rs.next()) {
					String msgid = rs.getString(1);
					Object msgContent = rs.getObject(2);
					MqtaskVO tvo = new MqtaskVO();
					tvo.setMsgid(msgid);
					tvo.setMsgcontent(msgContent);
					lstTask.add(tvo);
				}
				MqtaskVO[] aryTaskVOS = lstTask.toArray(new MqtaskVO[0]);
				return aryTaskVOS;
			}
		};

		MqtaskVO[] aryTaskVOS = (MqtaskVO[]) dao
				.executeQuery(select, processor);

		if (aryTaskVOS != null && aryTaskVOS.length > 0)
			return aryTaskVOS;

		// ---------------------------------

		// 设置指定数量MqtaskVO数据TaskID
		String setsql = "update mq_task set taskid='" + taskID
				+ "' where  taskid='N' and rownum<=100";
		int count = dao.executeUpdate(setsql);
		if (count == 0)
			return null;

		aryTaskVOS = (MqtaskVO[]) dao.executeQuery(select, processor);

		return aryTaskVOS;
	}

	@Override
	public void updateMqTask(String msgId, String status, String desc)
			throws DAOException {
		String upSql = "update mq_task set replystatus=?,replaycontent=? where msgid=?";
		SQLParameter parm = new SQLParameter();
		parm.addParam(status);
		parm.addParam(desc);
		parm.addParam(msgId);
		new BaseDAO().executeUpdate(upSql, parm);
	}

	@Override
	public void delMqTask(String msgId) throws DAOException {
		String delSql = "delete from mq_task where taskid=?";
		SQLParameter parm = new SQLParameter();
		parm.addParam(msgId);
		new BaseDAO().executeUpdate(delSql, parm);
	}

	@Override
	public void delMqTask_RequiresNew(String msgId) throws DAOException {
		delMqTask(msgId);
	}
}
