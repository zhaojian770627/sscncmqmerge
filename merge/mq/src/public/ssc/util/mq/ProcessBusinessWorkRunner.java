package ssc.util.mq;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import nc.bs.dao.DAOException;
import nc.bs.framework.common.InvocationInfoProxy;
import nc.bs.framework.common.NCLocator;
import nc.bs.framework.comn.NetStreamContext;
import nc.bs.framework.mx.thread.ThreadTracer;
import nc.bs.logging.Log;
import nc.md.persist.framework.IMDPersistenceQueryService;
import nc.pub.ssc.mq.vo.SSCEjbMessage;
import nc.pub.ssc.mq.vo.SSCEjbNetMessage;
import nc.pub.ssc.mq.vo.SSCMessage;
import nc.vo.arap.basebill.BaseBillVO;
import nc.vo.cmp.applaybill.AggApplyBillVO;
import nc.vo.cmp.applaybill.ApplyBillVO;
import nc.vo.ep.bx.JKBXVO;
import nc.vo.erm.accruedexpense.AggAccruedBillVO;
import nc.vo.erm.lendext.AggLendExtVO;
import nc.vo.erm.matterapp.AggMatterAppVO;
import nc.vo.jcom.lang.StringUtil;
import nc.vo.pu.m25.entity.InvoiceHeaderVO;
import nc.vo.pu.m25.entity.InvoiceVO;
import nc.vo.pub.AggregatedValueObject;
import nc.vo.pub.BusinessException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

public class ProcessBusinessWorkRunner implements Runnable {
	MQApp parentApp;
	Channel channelRev;
	Envelope envelope;

	// �߳��ڱ�������token
	byte[] token;
	// ����Դ
	String dataSource;

	byte[] bodys;

	IRecvHandler recvHandlerImp;

	public byte[] getBodys() {
		return bodys;
	}

	public void setBodys(byte[] bodys) {
		this.bodys = bodys;
	}

	public Envelope getEnvelope() {
		return envelope;
	}

	public void setEnvelope(Envelope envelope) {
		this.envelope = envelope;
	}

	public byte[] getToken() {
		return token;
	}

	public void setToken(byte[] token) {
		this.token = token;
	}

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public ProcessBusinessWorkRunner(MQApp parentApp, Channel channelRev,
			IRecvHandler recvHandlerImp) {
		this.parentApp = parentApp;
		this.channelRev = channelRev;
		this.recvHandlerImp = recvHandlerImp;
	}

	@Override
	public void run() {
		try {
			ThreadTracer.getInstance().startThreadMonitor("ProcessBusinessWorkRunner", String.valueOf(Thread.currentThread().getId()), "$$$$$$$");
			NetStreamContext.setToken(token);
			InvocationInfoProxy.getInstance().setUserDataSource(dataSource);
			processBusinessWork(channelRev, envelope, bodys, recvHandlerImp);
		} catch (Exception e) {
			Log.getInstance(TaskMQUtil.MODULENAME).error(e);
			throw new RuntimeException("����ʱ�쳣",e);
		}
	}

	/**
	 * ���ڴ���ҵ�����
	 * 
	 * @param channelRev
	 * @param envelope
	 * @param bodys
	 * @param recvHandlerImp
	 * @return
	 * @throws IOException
	 */
	private boolean processBusinessWork(Channel channelRev, Envelope envelope,
			byte[] bodys, IRecvHandler recvHandlerImp) throws IOException {
		IMQLockManager lockManager = NCLocator.getInstance().lookup(
				IMQLockManager.class);
		SSCMessage sscVO = null;
		boolean isDelLock=true;
		try {
			Log.getInstance(TaskMQUtil.MODULENAME).error("1.processBusinessWork��ʼ����:"+System.currentTimeMillis());
			sscVO = MessageSerialUtil.deSerialize(bodys);
			Log.getInstance(TaskMQUtil.MODULENAME).error("1.1.processBusinessWork�ѻ��ԭʼ��Ϣ"+sscVO.getId()+":"+System.currentTimeMillis());
			ProcessResult pr = new ProcessResult();
			
			if(sscVO instanceof SSCEjbNetMessage)
			{
				SSCEjbNetMessage netMsg=(SSCEjbNetMessage) sscVO;
				SSCEjbMessage ejbMsg=netMsg.getEjgmsg();
				
				if(netMsg.getStatus()==1)
				{
					if(ejbMsg==null)
					{
						SSCEjbNetMessage retNetMsg=SSCEjbNetMessage.toAskErrorMsg(netMsg, "���л����ִ���,�޷���ò�������", "");
						// ��ӡ�����͵���Ϣ���Ա�У��
						Log.getInstance(TaskMQUtil.MODULENAME).error("�ط���Ϣ��FAILED��:"+retNetMsg.getId());
						byte[] bytes = MessageSerialUtil.serialize(retNetMsg);
						channelRev.basicPublish(parentApp.getExchangename(),
								this.parentApp.getSndQueueRoutingKey(),
								MessageProperties.PERSISTENT_TEXT_PLAIN, bytes);
					}
					else
					{
						Log.getInstance(TaskMQUtil.MODULENAME).error("2.processBusinessWork��ʼ����EJB��Ϣ"+ejbMsg.getId()+":"+System.currentTimeMillis());
						pr = recvHandlerImp.handle(netMsg, sscVO.getId());
						Log.getInstance(TaskMQUtil.MODULENAME).error("10.processBusinessWork��Ϣ�������"+ejbMsg.getId()+":"+System.currentTimeMillis());
						
						processMsgBeforSend(netMsg);
						
						// ���ͳɹ���Ϣ
						SSCEjbNetMessage retMsg = SSCEjbNetMessage.toAskMsg(
								netMsg, pr.getReturnObj());
						byte[] bytes = MessageSerialUtil.serialize(retMsg);
						channelRev.basicPublish(parentApp.getExchangename(),
								this.parentApp.getSndQueueRoutingKey(),
								MessageProperties.PERSISTENT_TEXT_PLAIN, bytes);

						int i=0;
					}
				}
				else if(netMsg.getStatus()==0)	// ��Ϣ����ֵ�ɹ�
				{
					// �����Ϣ 
					
				}
				else if(netMsg.getStatus()==2)	// ��Ϣ����ʧ��
				{
					Log.getInstance(TaskMQUtil.MODULENAME).error("��Ϣ����ʧ��:"+netMsg.getId()+netMsg.getStackError());
				}
			}
			else{
				throw new BusinessException("��֧�ֵ���Ϣ��ʽ");
			}
			
		} catch (MQLockedExeption e) { // ���������ʧ�ܣ����������������̴߳����У����ǿ���ʱ��Ƚϳ�
			Log.getInstance(TaskMQUtil.MODULENAME).error("processBusinessWork-MQLockedExeption",e);
		}catch (Exception e) { // �����쳣�������ظ�
			if(e instanceof DAOException)
			{
				isDelLock=false;
			}
			backFailedMsg(sscVO,e,channelRev);
		}
		finally {
			try {
				channelRev.basicAck(envelope.getDeliveryTag(), false);
				if(isDelLock)
					lockManager.finishDelLock_RequiresNew(sscVO.getId());
			} catch (Exception e) {
				Log.getInstance(TaskMQUtil.MODULENAME).error("processBusinessWork-finally",e);
			}
			Log.getInstance(TaskMQUtil.MODULENAME).error("11.processBusinessWork�̴߳������"+sscVO.getId()+":"+System.currentTimeMillis());
		}
		return false;
	}

	/**
	 * �ط�������Ϣ
	 * 
	 * @param msg
	 * @param e
	 * @param channelRev
	 * @throws IOException 
	 */
	private void backFailedMsg(SSCMessage msg,Exception e, Channel channelRev) throws IOException{
		if(msg==null)
			Log.getInstance(TaskMQUtil.MODULENAME).error("backFailedMsg���޷���ȡ��Ϣ����");
		
		SSCEjbNetMessage netMsg=(SSCEjbNetMessage) msg;
		String stackInfo= processErrorStackInfo(e);
		String bizInfo=processErrorMessage(e);
		//SSCEjbNetMessage retNetMsg=SSCEjbNetMessage.toByErrorMsg(netMsg, bizInfo, stackInfo);
		SSCEjbNetMessage retNetMsg=SSCEjbNetMessage.toAskErrorMsg(netMsg, bizInfo, stackInfo);
		// ��ӡ�����͵���Ϣ���Ա�У��
		Log.getInstance(TaskMQUtil.MODULENAME).error("�ط���Ϣ��FAILED��:"+retNetMsg.getId());
		byte[] bytes = MessageSerialUtil.serialize(retNetMsg);
		channelRev.basicPublish(parentApp.getExchangename(),
				this.parentApp.getSndQueueRoutingKey(),
				MessageProperties.PERSISTENT_TEXT_PLAIN, bytes);
		
	}
	
	/**
	 * ���������Ϣ
	 * 
	 * @param e
	 * @return
	 */
	private String processErrorMessage(Exception e) {
		StringBuffer sb = new StringBuffer();
		if (e.getCause() != null){
			if (e.getCause() instanceof InvocationTargetException) {
				// ��װ���쳣
				InvocationTargetException ie = (InvocationTargetException) e
						.getCause();

				if (ie.getTargetException() != null) {
					Throwable realExeption = ie.getTargetException();
					sb.append(realExeption.getMessage());
				} else if (ie.getCause() != null) {
					Throwable realExeption = ie.getCause();
					sb.append(realExeption.getMessage());
				} else {
					sb.append(ie.getMessage());
				}
			}else
			{
				sb.append(e.getCause().getMessage());
			}
		}else
			sb.append(e.getMessage());
		String msg=sb.toString();
		if(StringUtil.isEmptyWithTrim(msg)||"null".endsWith(msg))
			msg="NCϵͳδ֪�쳣��";
		return msg;
	}

	/**
	 * ����ջ��Ϣ��ϳ��ַ���
	 * 
	 * @param e
	 * @return
	 */
	private String processErrorStackInfo(Exception e) {

		StringBuffer sb = new StringBuffer();

		if (e.getCause() != null
				&& e.getCause() instanceof InvocationTargetException) {
			InvocationTargetException ie = (InvocationTargetException) e
					.getCause();

			if (ie.getCause() != null)
				genenralInfo(ie.getCause(), sb);

			if (ie.getTargetException() != null)
				genenralInfo(ie.getTargetException(), sb);

		}
		if (e.getCause() != null)
			genenralInfo(e.getCause(), sb);

		genenralInfo(e, sb);
		sb.append(e.getMessage());
		return sb.toString();
	}

	private void genenralInfo(Throwable throwable, StringBuffer sb) {
		StackTraceElement[] ary1StackEle = throwable.getStackTrace();
		int c = 0;
		if (ary1StackEle != null && ary1StackEle.length > 0) {
			for (StackTraceElement ele : ary1StackEle) {
				if (c > 5)
					break;
				c++;
				sb.append("\n").append(ele.toString());
			}
		}
	}
	
	/**
	 * ����Ϣ֮ǰ���д���,Ԥ�Ƶ��ݺ�
	 */
	private void processMsgBeforSend(SSCEjbNetMessage netMsg)
	{
		try {
			if (netMsg.getBillno() == null) {
				nc.md.persist.framework.IMDPersistenceQueryService qryservice = NCLocator
						.getInstance().lookup(IMDPersistenceQueryService.class);
				String billPk = netMsg.getPk_bill();
				Class voClass = Class.forName(netMsg.getVoclass());
				AggregatedValueObject aggVo = qryservice.queryBillOfVOByPK(
						voClass, billPk, false);
				String billNo=getBillNo(aggVo);
				netMsg.setBillno(billNo);

			}
		} catch (Exception e) {
			Log.getInstance(TaskMQUtil.MODULENAME).error(e);
		}
	}
	
	public  String getBillNo(AggregatedValueObject aggvo)throws BusinessException {
		String billno = "";// cannot be null;
		if(aggvo.getParentVO() instanceof BaseBillVO  ){
			billno = ((BaseBillVO)aggvo.getParentVO()).getBillno() ;
		}else if(aggvo instanceof InvoiceVO   ){
			billno = ((InvoiceHeaderVO)aggvo.getParentVO()).getVbillcode()  ;
		}else if(aggvo instanceof AggApplyBillVO  ){
			billno = ((ApplyBillVO)aggvo.getParentVO()).getBill_no()  ;
		}else if( aggvo instanceof JKBXVO  ){
			billno = ((JKBXVO)aggvo).getParentVO().getDjbh() ;
		}else if(  aggvo instanceof AggAccruedBillVO ){ 
			billno=((AggAccruedBillVO)aggvo).getParentVO().getBillno();
		}else if(  aggvo instanceof AggMatterAppVO ){ 
			billno =  ((AggMatterAppVO)aggvo).getParentVO().getBillno() ;
		}else if(  aggvo instanceof AggLendExtVO ){ 
			billno =  ((AggLendExtVO)aggvo).getParentVO().getCode() ;
		}
		return billno ;
	}

}
