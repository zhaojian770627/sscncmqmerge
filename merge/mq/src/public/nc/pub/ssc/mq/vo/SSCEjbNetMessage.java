package nc.pub.ssc.mq.vo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import nc.bs.logging.Log;

import ssc.util.mq.MessageSerialUtil;
import ssc.util.mq.TaskMQUtil;

/**
 * 消息。用于网络传输
 * @author chenzmb
 *
 */
public class SSCEjbNetMessage   extends SSCMessage {
	private static final long serialVersionUID = 1L;
	private byte[] ejbByte = null;
	private byte[] retByte = null;
	private int  status = 0;//0 normal finished , 1 ejgmsg excute ;2:return error.
	
	
	private String billno ;
	private String voclass ;
	private String pk_bill;
	private String pk_workflownote;
	private String bizError;
	private String stackError;
	private String detailString;
	
	private transient String ts ;
	private transient SSCEjbMessage ejgmsg = null;
	private transient Object result = null;
	
	public static transient final String normalattributes="pk_msg,userid,voclass,pk_bill,pk_workflownote,billno,detailString,status,bizError,stackError," ;
	public static transient final String attributes=normalattributes+",msg,result" ;
	public static transient final String TABLE_NAME = "ssc_ejbmsg";
	
	public static transient final String PK_MSG_KEY = "pk_msg";
	public static transient final String PK_BILL_KEY = "msg_pk_bill";
	public static transient final String PK_WORKFLOWNOTE_KEY = "msg_pk_workflownote";
	public static transient final String VO_CLASS_KEY = "msg_voclass";
	public static transient final String VO_BILLNO_KEY = "msg_billno";
	
	public static transient final String VO_ADD_KEY = "msg_voadd";
	public static transient final String MSG_ERROR_LEVEL = "msg_error_level";
	
	
	public SSCEjbNetMessage(){}
	
	public static SSCEjbNetMessage toAskMsg(SSCEjbNetMessage sourceMsg,Object result){
		SSCEjbNetMessage msg = sourceMsg.clone();
		msg.setResult(result) ;
		msg.setStatus(0);
		return msg;
	}
	
	public static SSCEjbNetMessage toByEjbMsg(SSCEjbMessage ejgmsg){
		SSCEjbNetMessage msg = new SSCEjbNetMessage();
		msg.setEjgmsg(ejgmsg) ;
		msg.setStatus(1);
		return msg;
	}
	
	public static SSCEjbNetMessage toAskErrorMsg(SSCEjbNetMessage sourceMsg,String bizError,String stackError){
		SSCEjbNetMessage msg = sourceMsg.clone();
		msg.setStatus(2);
		msg.setBizError(bizError);
		msg.setStackError(stackError);
		return msg;
	}
	
	public Object getResult() {
		return result;
	}

	public void setResult(Object result) {
		this.result = result;
	}

	@Override
	public SSCEjbNetMessage clone(){
		SSCEjbNetMessage msg = new SSCEjbNetMessage();
		msg.setId( getId()) ;
		msg.setEjgmsg( getEjgmsg() ) ;
		msg.setBillno(getBillno()) ;
		msg.setStatus( getStatus() ) ;
		msg.setVoclass( getVoclass() ) ;
		msg.setPk_bill( getPk_bill() ) ;
		msg.setPk_workflownote( getPk_workflownote()  ) ;
		msg.setBizError( getBizError() ) ;
		msg.setStackError( getStackError() ) ;
		msg.setDetailString( getDetailString() ) ;
		msg.setResult(getResult()) ;
		return msg;
	}
	

	
	public SSCEjbMessage getEjgmsg() {
		return ejgmsg;
	}
	public void setEjgmsg(SSCEjbMessage ejgmsg) {
		this.ejgmsg = ejgmsg;
	}

	
	public byte[] getEjbByte() {
		return ejbByte;
	}

	public void setEjbByte(byte[] ejbByte) {
		this.ejbByte = ejbByte;
	}


	public String getBillno() {
		return billno;
	}

	public void setBillno(String billno) {
		this.billno = billno;
	}

	public String getVoclass() {
		return voclass;
	}

	public void setVoclass(String voclass) {
		this.voclass = voclass;
	}

	public String getDetailString() {
		return detailString;
	}

	public void setDetailString(String detailString) {
		this.detailString = detailString;
	}

	public String getPk_bill() {
		return pk_bill;
	}

	public void setPk_bill(String pk_bill) {
		this.pk_bill = pk_bill;
	}

	public String getPk_workflownote() {
		return pk_workflownote;
	}

	public void setPk_workflownote(String pk_workflownote) {
		this.pk_workflownote = pk_workflownote;
	}

	public String getBizError() {
		return bizError;
	}

	public void setBizError(String bizError) {
		this.bizError = bizError;
	}

	public String getStackError() {
		return stackError;
	}

	public void setStackError(String stackError) {
		this.stackError = stackError;
	}

	/**
	 * //0 normal finished , 1 ejgmsg excute ;2:return error.
	 * @return
	 */
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}

	
	public String getTs() {
		return ts;
	}

	public void setTs(String ts) {
		this.ts = ts;
	}

	//pk_msg,userid,voclass,pk_bill,pk_workflownote,detailString,status,bizError,stackError
	public void setAttributeValue(String name, Object v) {
		if(!(v instanceof String) ){
			return ;
		}
		String value = (String) v;
		name = name.trim();
		if(name.equalsIgnoreCase("pk_msg")){
			setId(value);
		}else if(name.equalsIgnoreCase("voclass")){
			setVoclass(value);
		}else if(name.equalsIgnoreCase("pk_bill")){
			setPk_bill(value);
		}else if(name.equalsIgnoreCase("pk_workflownote")){
			setPk_workflownote(value);
		}else if(name.equalsIgnoreCase("detailString")){
			setDetailString(value);
		}else if(name.equalsIgnoreCase("status")){
			try {
				Integer i =Integer.parseInt( value ) ;
				setStatus(i);
			} catch (Exception e) {
			}
		}else if(name.equalsIgnoreCase("bizError")){
			setBizError(value);
		}else if(name.equalsIgnoreCase("stackError")){
			setStackError(value);
		}else if(name.equalsIgnoreCase("billno")){
			setBillno(value);
		}else if(name.equalsIgnoreCase("ts")){
			setTs(value);
		}
	}
	
	
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		
		try {
			if(ejbByte!=null){
				this.ejgmsg = (SSCEjbMessage) MessageSerialUtil.deSerialize(ejbByte);
			}
			
		} catch (Exception e) {
			Log.getInstance(TaskMQUtil.MODULENAME).error(e);
		}
		try {
			if(retByte!=null){
				this.result = MessageSerialUtil.deSerializeObj(retByte);
			}
		} catch (Exception e) {
			Log.getInstance(TaskMQUtil.MODULENAME).error(e);
		}
	}
	private void writeObject(ObjectOutputStream out) throws IOException {
		if(ejgmsg!=null){
			this.ejbByte = MessageSerialUtil.serialize(ejgmsg);
		}
		if(result!=null){
			this.retByte = MessageSerialUtil.serialize(result);
		}
		out.defaultWriteObject();
	}
	
	@Override
	public String toDetailString() {
		if(getStatus() == 0 && ejgmsg!=null){
			return "["+getId()+"]"+ejgmsg.toString();
		}else{
			return "["+getId()+"]" ;
		}
	}
	@Override
	public String toString() {
		return getId() ;
	}
	
}
