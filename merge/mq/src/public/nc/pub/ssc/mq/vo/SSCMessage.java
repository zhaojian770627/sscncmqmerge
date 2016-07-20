package nc.pub.ssc.mq.vo;

import java.io.Serializable;
import java.util.UUID;

/**
 * 消息基类，各个消息
 * @author chenzmb
 *
 */
public class SSCMessage  implements Serializable{
	private static final long serialVersionUID = 1L;
	private String id;
	public SSCMessage(){
		id = UUID.randomUUID().toString();//+new java.util.Date().getTime()
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	
	@Override
	public String toString() {
		return getId();
	}
	
	public String toDetailString() {
		return getId();
	}
}
