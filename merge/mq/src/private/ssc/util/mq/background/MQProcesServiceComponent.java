package ssc.util.mq.background;

import java.io.File;
import java.net.InetAddress;

import org.springframework.context.support.FileSystemXmlApplicationContext;

import ssc.util.mq.MQApp;
import ssc.util.mq.TaskMQUtil;
import nc.bs.framework.common.RuntimeEnv;
import nc.bs.framework.component.ServiceComponent;

public class MQProcesServiceComponent implements ServiceComponent {
	boolean bStarted;

	@Override
	public boolean isStarted() {
		return false;
	}

	@Override
	public void start() throws Exception {
		bStarted = true;
		
		// ��ʱ����ʹ����Ϣ
//		if(1==1)
//			return;
		
		MQApp mqtaskProcessApp = null;
		MQApp mqsndProcessApp = null;
		String mqconfilg;
		String pre = File.separator;
		if (pre.equals("\\")) {
			mqconfilg = RuntimeEnv.getInstance().getNCHome()
					+ "/modules/sscncmq/META-INF/sscrevmqcfg.xml";
		} else
			mqconfilg = File.separator
					+ RuntimeEnv.getInstance().getNCHome()
					+ "/modules/sscncmq/META-INF/sscrevmqcfg.xml";
		
		FileSystemXmlApplicationContext factory = new FileSystemXmlApplicationContext(
				mqconfilg);
		mqtaskProcessApp = (MQApp) factory.getBean("mqAdmin");
		mqsndProcessApp = (MQApp) factory.getBean("mqAdmin");
		TaskMQUtil.getInstance().registerMQTaskProcessApp(mqtaskProcessApp);
		TaskMQUtil.getInstance().registerMQSendProcessApp(mqsndProcessApp);
		
		// ��ȡ����IP
		InetAddress ia=InetAddress.getLocalHost();
		String ip=ia.getHostAddress();
		// �����̴߳���
		if(mqtaskProcessApp.getConfig().getSsc2ncallowips().contains(ip))
			mqtaskProcessApp.startMulThreadProcessEJBTask();
		// �����̷߳���
		if(mqtaskProcessApp.getConfig().getNc2ncallowips().contains(ip))
			mqsndProcessApp.startThreadSndTxMsgToSSC();
	}

	@Override
	public void stop() throws Exception {
		// TODO �Զ����ɵķ������

	}

}
