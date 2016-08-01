package nc.itf.ssc.remote;

import java.util.Map;

import nc.jdbc.framework.SQLParameter;
import nc.jdbc.framework.processor.ResultSetProcessor;
import nc.vo.pub.BusinessException;
import nc.vo.wfengine.core.workflow.BasicWorkflowProcess;
import nc.vo.wfengine.pub.WFTask;
import uap.iweb.wf.vo.WFProInsVO;
/**
 * WF
 * @author chenzmb
 *
 */
public interface ISSC2NcService {
	public Object retrieveByPK(String className, String pk,String[] fields) throws BusinessException;
	
	public Object executeQuery(String sql, SQLParameter parameter, ResultSetProcessor processor)  throws BusinessException;
	public Map<String, WFTask> queryTask(String billID, String billtype) throws BusinessException;
	public BasicWorkflowProcess getWorkFlowProcess(String processdefid) throws BusinessException;
	public WFProInsVO queryProcessInsByBillID(String billID, String billtype) throws BusinessException;
}
