package nc.impl.ssc.remote;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import nc.bs.dao.BaseDAO;
import nc.bs.dao.DAOException;
import nc.bs.framework.common.NCLocator;
import nc.bs.logging.Logger;
import nc.bs.pf.pub.PfDataCache;
import nc.itf.ssc.remote.ISSC2NcService;
import nc.itf.uap.IUAPQueryBS;
import nc.jdbc.framework.SQLParameter;
import nc.jdbc.framework.processor.ResultSetProcessor;
import nc.vo.pub.BusinessException;
import nc.vo.wfengine.core.workflow.BasicWorkflowProcess;
import nc.vo.wfengine.definition.WorkflowTypeEnum;
import nc.vo.wfengine.pub.WFTask;
import nc.vo.wfengine.pub.WFTaskMappingMeta;
import nc.vo.wfengine.pub.WfTaskOrInstanceStatus;
import uap.iweb.wf.vo.WFProInsVO;

public class SSC2NcServiceImpl implements ISSC2NcService{
	@Override
	public Object executeQuery(String sql, SQLParameter parameter,
			ResultSetProcessor processor) throws BusinessException {
		BaseDAO dao = new BaseDAO();
		try { 
			if(parameter == null){
				return dao.executeQuery(sql,processor);
			}else{
				return dao.executeQuery(sql, parameter, processor);
			}
			
		} catch (DAOException e) {
			Logger.error(sql);
			if(parameter!=null){
				Logger.error(parameter.getParameters().toString());
			}
			 Logger.error(e.getMessage(), e);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public WFProInsVO queryProcessInsByBillID(String billID, String billtype) throws BusinessException {
		String cond = "billversionpk=? and billtype=? and procstatus!=? and workflow_type in (" + WorkflowTypeEnum.Approveflow.getIntValue() + "," + WorkflowTypeEnum.Workflow.getIntValue() + ")"
				+ " order by pk_wf_instance";
		SQLParameter param = new SQLParameter();
		param.addParam(billID);
		param.addParam(billtype);
		param.addParam(WfTaskOrInstanceStatus.Inefficient.getIntValue());

		Collection<WFProInsVO> col;
		try {
			col = new BaseDAO().retrieveByClause(WFProInsVO.class, cond, param);
		} catch (BusinessException e) {
			Logger.error(e.getMessage(), e);
			throw new BusinessException(e.getMessage(), e);
		}

		if ((col != null) && (col.size() > 0)) {
			return (WFProInsVO) col.iterator().next();
		}
		return null;
	}

	public BasicWorkflowProcess getWorkFlowProcess(String processdefid) {
		try {
			return PfDataCache.getWorkflowProcess(processdefid);
		} catch (Exception e) {
			Logger.error(e.getMessage(), e);
		}

		return null;
	}

	@SuppressWarnings("unchecked")
	public Map<String, WFTask> queryTask(String billID, String billtype) throws BusinessException {
		Map<String, WFTask> taskMap = new HashMap<String, WFTask>();
		try {
			Collection<WFTask> result = new BaseDAO().retrieveByClause(WFTask.class, new WFTaskMappingMeta(), "pk_wf_instance in (select pk_wf_instance from pub_wf_instance where billversionpk='"
					+ billID + "' and billtype='" + billtype + "')");

			for (WFTask wfTask : result) {
				taskMap.put(wfTask.getTaskPK(), wfTask);
			}
		} catch (BusinessException e) {
			Logger.error(e.getMessage(), e);
			throw new BusinessException(e.getMessage(), e);
		}
		return taskMap;
	}

	@Override
	public Object retrieveByPK(String className, String pk, String[] fields)
			throws BusinessException {
		IUAPQueryBS  ibs = (IUAPQueryBS) NCLocator.getInstance().lookup(IUAPQueryBS.class.getName());
		Class<?> c = null;
		try {
			 c = Class.forName(className) ;
		} catch (Exception e) {
			throw new BusinessException (e.getMessage(),e);
		}
		Object ret = ibs.retrieveByPK(c,pk,fields);
		return ret;
	}
}
