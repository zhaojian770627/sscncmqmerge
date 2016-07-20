package ssc.util.mq;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import nc.bs.dao.DAOException;
import nc.bs.framework.common.InvocationInfoProxy;
import nc.bs.framework.common.NCLocator;
import nc.bs.framework.common.UserExit;
import nc.bs.logging.Log;
import nc.bs.uap.lock.PKLock;
import nc.pub.ssc.mq.vo.SSCEjbMessage;
import nc.pub.ssc.mq.vo.SSCEjbNetMessage;
import nc.vo.pub.BusinessException;
import nc.vo.uap.pf.PfProcessBatchRetObject;

/**
 * 
 * @author zhaojianc
 * 
 */
public class MQEJBTaskProcessorImpl implements IMQTaskProcessor {

	@Override
	public ProcessResult process_RequiresNew(SSCEjbNetMessage netMsg, String tag)
			throws BusinessException, MQLockedExeption, ClassNotFoundException,
			IOException {

		SSCEjbMessage  msg=netMsg.getEjgmsg();
		
		// ��Ϣ��ʾ�����ڽ�������
		String msgID = msg.getId();

		UserExit.getInstance().setGroupId(msg.getGroupid());
		UserExit.getInstance().setUserId(msg.getUserid());
		// must put here,or lock and unlock failed
		InvocationInfoProxy.getInstance().setUserId(msg.getUserid());

		/*
		 * ʹ�þ�̬������̬�����������⣬����ȵ��߳̽���������ͷ�
		 */
		Log.getInstance(TaskMQUtil.MODULENAME).error("3.MQEJBTaskProcessorImpl��ȡ��̬����"+System.currentTimeMillis());
		boolean locked = PKLock.getInstance().acquireLock(tag, null, null);
		if (!locked) {
			throw new MQLockedExeption(msgID + " locked!");
		}

		try {
			IMQLockManager lockManager = NCLocator.getInstance().lookup(
					IMQLockManager.class);
			Log.getInstance(TaskMQUtil.MODULENAME).error("4.MQEJBTaskProcessorImpl��ȡ���ݿ�����"+System.currentTimeMillis());
			LockResult lockResult = lockManager.requireLock(tag);
			if (lockResult.getLockFlag() == 0)
				throw new MQLockedExeption(msgID + " locked!");

			ProcessResult pr = new ProcessResult();
			// ��������
			if (lockResult.getLockFlag() == 2) {
				pr.setReturnCode("SUCESS");
				if (lockResult.getResult() != null) {
					Object o = MessageSerialUtil
							.deSerialize((byte[]) lockResult.getResult());
					pr.setReturnObj(o);
				}
				return pr;
			}

			if (!(msg instanceof SSCEjbMessage))
				return null;
			SSCEjbMessage sscMsg = (SSCEjbMessage) msg;

			String servicename = sscMsg.getServiceName();
			String methodname = sscMsg.getMethodName();

			Class<?>[] parameterTypes = sscMsg.getParamtypeClass();

			Object svr = NCLocator.getInstance().lookup(servicename);
			Method method;

			method = svr.getClass().getDeclaredMethod(methodname,
					parameterTypes);
			Log.getInstance(TaskMQUtil.MODULENAME).error("5.MQEJBTaskProcessorImpl����ҵ�������ʼ��"+System.currentTimeMillis());
			Object retObj = method.invoke(svr, sscMsg.getRealparams());
			Log.getInstance(TaskMQUtil.MODULENAME).error("6.MQEJBTaskProcessorImpl����ҵ�������ɣ�"+System.currentTimeMillis());
			pr.setReturnCode("SUCCESS");
			pr.setReturnObj(retObj);
			
			// ������Ҫ�ظ�
//			SSCEjbNetMessage retMsg = SSCEjbNetMessage.toAskMsg(netMsg, retObj);
//			TaskMQUtil.getInstance().saveMQTaskToDB(retMsg);
//			Log.getInstance(TaskMQUtil.MODULENAME).error("6.1 MQEJBTaskProcessorImpl saveMQTaskToDB��" + System.currentTimeMillis());
						
			lockManager.finishLock(tag, retObj);
			return pr;
		} catch (NoSuchMethodException e) {
			Log.getInstance(TaskMQUtil.MODULENAME).error(e);
			throw new BusinessException(e);
		} catch (SecurityException e) {
			Log.getInstance(TaskMQUtil.MODULENAME).error(e);
			throw new BusinessException(e);
		} catch (IllegalAccessException e) {
			Log.getInstance(TaskMQUtil.MODULENAME).error(e);
			throw new BusinessException(e);
		} catch (IllegalArgumentException e) {
			Log.getInstance(TaskMQUtil.MODULENAME).error(e);
			throw new BusinessException(e);
		} catch(DAOException e){
			Log.getInstance(TaskMQUtil.MODULENAME).error(e);
			throw new BusinessException("���ݿ�ִ���쳣������Ϣ�ѱ���������������", e);
		}
		catch (InvocationTargetException e) {
			Log.getInstance(TaskMQUtil.MODULENAME).error("7.InvocationTargetException�쳣��"+System.currentTimeMillis());
			Log.getInstance(TaskMQUtil.MODULENAME).error("������ó��ִ���", e);
			if (e.getCause() != null)
				Log.getInstance(TaskMQUtil.MODULENAME).error(e.getCause());
			if (e.getTargetException() != null)
				Log.getInstance(TaskMQUtil.MODULENAME).error(
						e.getTargetException());
			throw new BusinessException(e);
		} finally {
			Log.getInstance(TaskMQUtil.MODULENAME).error("8.finally��ʼ��"+System.currentTimeMillis());
			if (locked) {
				PKLock.getInstance().releaseLock(tag, null, null);
			}

			// test DynamicLock release at thread end
			// because the thread don't exit ,so so here manually realeased
			// ���̴߳����������Ͳ��ü���
			// �����ԣ����������⣬�д�����
			PKLock.getInstance().releaseDynamicLocks();
			Log.getInstance(TaskMQUtil.MODULENAME).error("9.finally������"+System.currentTimeMillis());
		}

	}
}
