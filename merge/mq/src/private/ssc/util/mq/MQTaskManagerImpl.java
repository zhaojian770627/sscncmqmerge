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
		// N ��ʾ��
		taskVO.setTaskid("N");
		// R ��ʾ������
		taskVO.setMsgstatus("R");
		taskVO.setStatus(2);
		String id = new BaseDAO().insertVOWithPK(taskVO);
		return id;
	}

	/**
	 * ִ�в���
	 * 
	 * 1.ִ�����ݿ��� 2.�����ݿ��л�ȡ��û������taskID�����񣬲�����taskID�ֶ�ΪtaskID����ʱ�趨ÿ��ȡ100��
	 * 3.����Щ���񷵻أ����ڷ���Ϣ
	 * 
	 * �˲������ݵȵģ����Կ����ڵ���ǰ�󣬲��ü����ݿ�������Ϊ��Ϣͬʱֻ�ܱ�ͬһ�����������ѡ�
	 * �������Ϣ��־��ѯ�Ƿ����Ѿ��������������еĻ�ֱ�ӷ��أ��������·���һ�����з��͡�
	 * 
	 * @throws DAOException
	 */
	@Override
	public MqtaskVO[] getPendingMqTask(String taskID) throws DAOException {
		BaseDAO dao = new BaseDAO();
		// ��tasklock�б���Ԥ��LOCK�ֶΣ��������ݿ�������ֹͬʱȡ����
		String lockSql = "update mq_tasklock set taskstatus='0' where pk ='MQTASK_getPendingMqTask'";
		// ��������
		int c = dao.executeUpdate(lockSql);

		// �Ƚ��в�ѯ������������ͣ�����Ĵ���ο���ȥ��,�����ٴ�ִ�У�������Ϣû���ͳɹ����ȴ����ݿ��ѯ����Ϣ
		// �������������û����ȡһ��������ֻ���ͱ�����
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

		// ����ָ������MqtaskVO����TaskID
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
