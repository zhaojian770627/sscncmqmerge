package ssc.util.mq;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;

import nc.bs.framework.common.InvocationInfoProxy;
import nc.pub.ssc.mq.vo.SSCEjbMessage;
import nc.pub.ssc.mq.vo.SSCMessage;

/**
 * 消息构建及序列化
 * @author chenzmb
 *
 */
public class MessageSerialUtil {
	public static byte[] serialize(Object obj) throws IOException{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos =  new ObjectOutputStream(bos);
		oos.writeObject(obj );
		byte[] bytes = bos.toByteArray() ;
		bos.close();
		oos.close();
		return bytes;
	}
	public static Object deSerializeObj(byte[] in) throws IOException, ClassNotFoundException{
		ByteArrayInputStream bis = new ByteArrayInputStream(in);
		ObjectInputStream ois =  new ObjectInputStream(bis);
		Object obj = (Object) ois.readObject() ;
		bis.close();
		ois.close();
		return obj;
	}
	
	public static SSCMessage deSerialize(byte[] in) throws IOException, ClassNotFoundException{
		return (SSCMessage) deSerializeObj(in);
	}
	/**
	 * 构建消息
	 * @param servicename
	 * @param method
	 * @param args
	 * @return
	 */
	public static SSCEjbMessage buildSSCEjbMessage(String servicename,Method method, Object... args){
		SSCEjbMessage ssc = new SSCEjbMessage();
		String groupId = InvocationInfoProxy.getInstance().getGroupId() ;
		String userId = InvocationInfoProxy.getInstance().getUserId() ;
		String userCode = InvocationInfoProxy.getInstance().getUserCode() ;
		ssc.setGroupid(groupId) ;
		ssc.setUserid(userId) ;
		ssc.setUsercode( userCode) ;
		ssc.setServiceName( servicename ) ;
		ssc.setMethodName( method.getName() ) ;
		ssc.setRealparams(args);
		ssc.setParamtypeClass(method.getParameterTypes()) ;
		return ssc;
	}

}
