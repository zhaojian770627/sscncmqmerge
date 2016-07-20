package nc.pub.ssc.mq.vo;

import nc.bs.framework.common.InvocationInfoProxy;
import nc.vo.pub.AggregatedValueObject;
import nc.vo.pub.SuperVO;
import nc.vo.pub.workflownote.WorkflownoteVO;

/**
 * MQ消息
 * @author chenzmb
 *
 */
public class SSCEjbMessage extends SSCMessage{
	private static final long serialVersionUID = 1L;
	private String usercode; //调用的用户编码，用于设置上下文
	private String groupid; //调用的集团编码，用于设置上下文
	private String userid; //调用的用户ID，用于设置上下文
	
	private String serviceName;  //服务名称
	private String methodName;	 //服务方法
	private Class<?>[] paramtypeClass; //服务方法形参
	private Object[] realparams;	//服务方法实参
	
	
	public SSCEjbMessage(String serviceName, String methodName,
			Class<?>[] paramtypeClass, Object[] realparams, String usercode,
			String groupid, String userid) {
		super();
		userid = InvocationInfoProxy.getInstance().getUserId() ;
		groupid = InvocationInfoProxy.getInstance().getGroupId() ;
		usercode =  InvocationInfoProxy.getInstance().getUserCode() ;
		
		this.serviceName = serviceName;
		this.methodName = methodName;
		this.paramtypeClass = paramtypeClass;
		this.realparams = realparams;
	}
	public SSCEjbMessage() {
		super();
	}
	public String getServiceName() {
		return serviceName;
	}
	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}
	public String getMethodName() {
		return methodName;
	}
	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}
	public Class<?>[] getParamtypeClass() {
		return paramtypeClass;
	}
	public void setParamtypeClass(Class<?>... paramtypeClass) {
		this.paramtypeClass = paramtypeClass;
	}
	public Object[] getRealparams() {
		return realparams;
	}
	public void setRealparams(Object... realparams) {
		this.realparams = realparams;
	}
	
	
	public String toDetailString() {
		StringBuffer str  = new StringBuffer() ;
//		str.append("id:").append(getId());
		str.append("service:").append(serviceName);
		str.append(",method:").append(methodName);
		str.append(",params:");
		if(realparams!=null){
			for(Object param:realparams){
				if( param instanceof Object[]){
					for(Object p:(Object[])param){
						str.append(getStr(p)).append(",");
					}
				}else {
					str.append(getStr(param)).append(",");
				}
			}
		}
		
		return str.toString();
	}

	public String getUsercode() {
		return usercode;
	}

	public void setUsercode(String usercode) {
		this.usercode = usercode;
	}

	public String getGroupid() {
		return groupid;
	}

	public void setGroupid(String groupid) {
		this.groupid = groupid;
	}

	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}
	
	
	private String getStr(Object obj){
		if(obj==null){
			return "null";
		}
		try {
			if(obj instanceof AggregatedValueObject){
				String pk_bill = ((AggregatedValueObject)obj).getParentVO().getPrimaryKey() ;
				return obj.getClass().getName()+"("+pk_bill+")" ;
			}else if(obj instanceof WorkflownoteVO){
				String pk_workflownote = ((WorkflownoteVO)obj).getPk_checkflow() ;
				return WorkflownoteVO.class.getName()+"("+pk_workflownote+")" ;
			}else if(obj instanceof SuperVO){
				return obj.getClass().getName()+"("+((SuperVO)obj).getPrimaryKey()+")" ;
			}
		} catch (Throwable e) {
		}
		return obj.toString() ;
	}
}

