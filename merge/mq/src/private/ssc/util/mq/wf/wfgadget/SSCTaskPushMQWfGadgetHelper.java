package ssc.util.mq.wf.wfgadget;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.net.URL;

import nc.bs.dao.BaseDAO;
import nc.bs.framework.common.InvocationInfoProxy;
import nc.bs.logging.Log;
import nc.bs.logging.Logger;
import nc.itf.ssc.remote.ISSCMQWrapProcessor;
import nc.itf.uap.pf.metadata.IFlowBizItf;
import nc.md.data.access.NCObject;
import nc.pub.ssc.jedis.tool.RedisProxy;
import nc.pub.ssc.mq.vo.SSCEjbMessage;
import nc.pub.ssc.mq.vo.SSCEjbNetMessage;
import nc.pub.ssc.pr.bill.BillEditListener;
import nc.pubitf.para.SysInitQuery;
import nc.vo.pub.AggregatedValueObject;
import nc.vo.pub.BusinessException;
import nc.vo.pub.CircularlyAccessibleValueObject;
import nc.vo.pub.SuperVO;
import nc.vo.wfengine.core.application.WfGadgetContext;

import org.codehaus.xfire.client.Client;

import ssc.util.mq.TaskMQUtil;

/**
 * ���� WebService
 * 
 * @author zhaojianc
 * 
 */
public class SSCTaskPushMQWfGadgetHelper {
	private String callWS(String xmlInfo) throws BusinessException {
		Client client = null;
		// ��ַ����ʱд��
		String wsip = SysInitQuery.getParaString("GLOBLE00000000000000",
				"SSC013");
		String wsdl = "http://" + wsip
				+ "/uapws/service/nc.itf.ssc.remote.ISSCPubWS?wsdl"; // TaskUtil.getInstance().getSysWebSvrIp();
		Object[] result;
		try {
			client = new Client(new URL(wsdl));
			result = client.invoke("process", new Object[] { "addToSSCTask",
					xmlInfo });
			if (result != null)
				return result[0].toString();

		} catch (MalformedURLException e) {
			Logger.error("������񴫵�������񣬵���WebService�ӿڳ��ִ���", e);
			throw new BusinessException(e.getMessage());
		} catch (Exception e) {
			Logger.error("������񴫵�������񣬵���WebService�ӿڳ��ִ���", e);
			throw new BusinessException(e.getMessage());
		}
		return null;
	}

	/**
	 * ��Ϣ��ʽ����
	 * 
	 * @return
	 * @throws BusinessException
	 */
	public void pushToSSCByMsg(WfGadgetContext gc) throws BusinessException {
		// String wfpk = gc.getPfParameterVO().m_workFlow.getPk_checkflow();
		// �ı䵥������״̬
		AggregatedValueObject billVO = (AggregatedValueObject) gc
				.getBillEntity();
		NCObject ncObj = NCObject.newInstance(billVO);
		IFlowBizItf itf = (IFlowBizItf) ncObj
				.getBizInterface(IFlowBizItf.class);
		String approvecol = itf.getColumnName("approvestatus");
		itf.setApproveStatus(Integer.valueOf(1));
		CircularlyAccessibleValueObject parent = billVO.getParentVO();
		BaseDAO dao = new BaseDAO();
		dao.setAddTimeStamp(false);
		dao.updateVO((SuperVO) parent, new String[] { approvecol });

		// �����Ϣ���������ѹ��Ϣ
		// ��װ��Ϣ
		SSCEjbMessage mqmes = new SSCEjbMessage();
		mqmes.setGroupid(InvocationInfoProxy.getInstance().getGroupId());
		mqmes.setUserid(InvocationInfoProxy.getInstance().getUserId());
		mqmes.setServiceName("nc.itf.ssc.remote.ISSCMQWrapProcessor");
		mqmes.setMethodName("process");
		mqmes.setParamtypeClass(new Class[] { String.class, Object[].class });
		mqmes.setRealparams(new Object[] {
				ISSCMQWrapProcessor.Oper_addToSSCTask,
				new Object[] { InvocationInfoProxy.getInstance().getGroupId(),
						gc.getBillEntity(), "" } });

		// sSSCEjbMessage successMsg= new SSCEjbMessage();
		SSCEjbNetMessage netMsg = SSCEjbNetMessage.toByEjbMsg(mqmes);
		TaskMQUtil.getInstance().saveMQTaskToDB(netMsg);
	}

	/**
	 * ����xml�ļ�
	 * 
	 * @param pk_group
	 * @param billtype
	 * @param transtype
	 * @param billId
	 * @return
	 */
	private String generateXml(String pk_group, String billtype,
			String transtype, String billId) {
		StringBuilder xmlBuilder = new StringBuilder();
		xmlBuilder.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
				.append("<params>").append("<operate>addToSSCTask</operate>")
				.append("<pk_group>").append(pk_group).append("</pk_group>")
				.append("<billtype>").append(billtype).append("</billtype>")
				.append("<transtype>").append(transtype).append("</transtype>")
				.append("<billid>").append(billId).append("</billid>")
				.append("</params>");
		return xmlBuilder.toString();
	}

	/**
	 * ��WebService��ʽѹ����
	 * 
	 * @param gc
	 */
	public void pushToSSCByWS(WfGadgetContext gc) throws BusinessException {
		AggregatedValueObject billVO = (AggregatedValueObject) gc
				.getBillEntity();
		
		//aggvo����redis
		save2Redis(billVO);
		
		NCObject ncObj = NCObject.newInstance(billVO);
		IFlowBizItf itf = (IFlowBizItf) ncObj
				.getBizInterface(IFlowBizItf.class);
		String approvecol = itf.getColumnName("approvestatus");
		itf.setApproveStatus(Integer.valueOf(1));
		CircularlyAccessibleValueObject parent = billVO.getParentVO();
		BaseDAO dao = new BaseDAO();
		dao.setAddTimeStamp(false);
		dao.updateVO((SuperVO) parent, new String[] { approvecol });

		String pk_group = InvocationInfoProxy.getInstance().getGroupId();
		String billtype = itf.getBilltype();
		String trantype = itf.getTranstype();
		String billId = itf.getBillId();

		// ����redis zhaojianc 2016-7-12
		try {
			new BillEditListener().updateBillInfoIfExists(billtype, billVO);
		} catch (Exception e) {
			Log.getInstance(this.getClass()).error(e.getMessage(), e);
		}
		// ����redis����
		
		String xmlInfo = generateXml(pk_group, billtype, trantype, billId);
		Logger.error("pushToSSCByWS:" + xmlInfo);
		String result = callWS(xmlInfo);
		if (!"success".equals(result))
			throw new BusinessException("������񴫵�����ʧ��,���ݺ�:" + billId);
	}
	
	/**
	 * ���� aggvo ��redis��
	 * @throws BusinessException 
	 */
	private void save2Redis(AggregatedValueObject billVO) throws BusinessException {
		ByteArrayOutputStream bos =  new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(bos);
			oos.writeObject(billVO);
			byte[] key = ("pushToSSCByWS_" + billVO.getParentVO().getPrimaryKey()).getBytes();
			RedisProxy.getJedisOne().set(key, bos.toByteArray());
		} catch (Exception e) {
			Logger.error(e);
			throw new BusinessException(e);
		}
		finally{
			try {
				oos.close();
			} catch (IOException e) {}
		}
	}
}
