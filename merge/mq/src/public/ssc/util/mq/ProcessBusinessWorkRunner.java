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

	// 线程内必须设置token
	byte[] token;
	// 数据源
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
			throw new RuntimeException("运行时异常",e);
		}
	}

	/**
	 * 用于处理业务操作
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
			Log.getInstance(TaskMQUtil.MODULENAME).error("1.processBusinessWork开始处理:"+System.currentTimeMillis());
			sscVO = MessageSerialUtil.deSerialize(bodys);
			Log.getInstance(TaskMQUtil.MODULENAME).error("1.1.processBusinessWork已获得原始消息"+sscVO.getId()+":"+System.currentTimeMillis());
			ProcessResult pr = new ProcessResult();
			
			if(sscVO instanceof SSCEjbNetMessage)
			{
				SSCEjbNetMessage netMsg=(SSCEjbNetMessage) sscVO;
				SSCEjbMessage ejbMsg=netMsg.getEjgmsg();
				
				if(netMsg.getStatus()==1)
				{
					if(ejbMsg==null)
					{
						SSCEjbNetMessage retNetMsg=SSCEjbNetMessage.toAskErrorMsg(netMsg, "序列化出现错误,无法获得操作类型", "");
						// 打印出发送的消息，以备校对
						Log.getInstance(TaskMQUtil.MODULENAME).error("回发消息（FAILED）:"+retNetMsg.getId());
						byte[] bytes = MessageSerialUtil.serialize(retNetMsg);
						channelRev.basicPublish(parentApp.getExchangename(),
								this.parentApp.getSndQueueRoutingKey(),
								MessageProperties.PERSISTENT_TEXT_PLAIN, bytes);
					}
					else
					{
						Log.getInstance(TaskMQUtil.MODULENAME).error("2.processBusinessWork开始处理EJB消息"+ejbMsg.getId()+":"+System.currentTimeMillis());
						pr = recvHandlerImp.handle(netMsg, sscVO.getId());
						Log.getInstance(TaskMQUtil.MODULENAME).error("10.processBusinessWork消息处理结束"+ejbMsg.getId()+":"+System.currentTimeMillis());
						
						processMsgBeforSend(netMsg);
						
						// 发送成功消息
						SSCEjbNetMessage retMsg = SSCEjbNetMessage.toAskMsg(
								netMsg, pr.getReturnObj());
						byte[] bytes = MessageSerialUtil.serialize(retMsg);
						channelRev.basicPublish(parentApp.getExchangename(),
								this.parentApp.getSndQueueRoutingKey(),
								MessageProperties.PERSISTENT_TEXT_PLAIN, bytes);

						int i=0;
					}
				}
				else if(netMsg.getStatus()==0)	// 消息返回值成功
				{
					// 清除消息 
					
				}
				else if(netMsg.getStatus()==2)	// 消息返回失败
				{
					Log.getInstance(TaskMQUtil.MODULENAME).error("消息返回失敗:"+netMsg.getId()+netMsg.getStackError());
				}
			}
			else{
				throw new BusinessException("不支持的消息格式");
			}
			
		} catch (MQLockedExeption e) { // 如果是锁定失败，表明其他消费者线程处理中，但是可能时间比较长
			Log.getInstance(TaskMQUtil.MODULENAME).error("processBusinessWork-MQLockedExeption",e);
		}catch (Exception e) { // 其他异常，尽力回复
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
			Log.getInstance(TaskMQUtil.MODULENAME).error("11.processBusinessWork线程处理结束"+sscVO.getId()+":"+System.currentTimeMillis());
		}
		return false;
	}

	/**
	 * 回发错误消息
	 * 
	 * @param msg
	 * @param e
	 * @param channelRev
	 * @throws IOException 
	 */
	private void backFailedMsg(SSCMessage msg,Exception e, Channel channelRev) throws IOException{
		if(msg==null)
			Log.getInstance(TaskMQUtil.MODULENAME).error("backFailedMsg：无法获取消息内容");
		
		SSCEjbNetMessage netMsg=(SSCEjbNetMessage) msg;
		String stackInfo= processErrorStackInfo(e);
		String bizInfo=processErrorMessage(e);
		//SSCEjbNetMessage retNetMsg=SSCEjbNetMessage.toByErrorMsg(netMsg, bizInfo, stackInfo);
		SSCEjbNetMessage retNetMsg=SSCEjbNetMessage.toAskErrorMsg(netMsg, bizInfo, stackInfo);
		// 打印出发送的消息，以备校对
		Log.getInstance(TaskMQUtil.MODULENAME).error("回发消息（FAILED）:"+retNetMsg.getId());
		byte[] bytes = MessageSerialUtil.serialize(retNetMsg);
		channelRev.basicPublish(parentApp.getExchangename(),
				this.parentApp.getSndQueueRoutingKey(),
				MessageProperties.PERSISTENT_TEXT_PLAIN, bytes);
		
	}
	
	/**
	 * 处理错误信息
	 * 
	 * @param e
	 * @return
	 */
	private String processErrorMessage(Exception e) {
		StringBuffer sb = new StringBuffer();
		if (e.getCause() != null){
			if (e.getCause() instanceof InvocationTargetException) {
				// 包装的异常
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
			msg="NC系统未知异常！";
		return msg;
	}

	/**
	 * 将堆栈信息组合成字符串
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
	 * 发消息之前进行处理,预制单据号
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
