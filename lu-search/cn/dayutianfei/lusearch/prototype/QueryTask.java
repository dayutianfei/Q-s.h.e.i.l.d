package cn.dayutianfei.lusearch.prototype;

import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Query;

/**
 * 一个Query要执行的一个查询任务的数据对象
 * 在多次查询过程中（含同一任务的多次处理过程）会保存该对象
 * 在全局会一直保存这个对象，对应一次用户的查询过程，直到用户释放这次查询连接
 * 
 * @author wzy
 * 
 */
public class QueryTask {

	private int queryID = -1; // 对应每次的查询链接，大于0时候有效
	private int limit; // 用户需要的返回记录的条数

	private List<String> returnFieldsName = null; // 用户结果集中需要返回数据的列
	private Map<String, String> typeMap = null; // 字段对应类型记录
	private String dbName = null;
	private String tableName = null;
	private String queryStr = null;
	private Query query = null;
	private List<String> shardNames = null;
	private int threadNum = 1;
	private boolean isCount = false;

	// 目前的查询中还未使用到下面的属性
	private String sortFiled = "";
	private boolean isDesc = false;

	private String group = null;
	public List<String> aggregateFunctions = null;

	public QueryTask(){}

	public int getLimit() {
		return limit;
	}

	public List<String> getReturnFieldsName() {
		return returnFieldsName;
	}

	public String getDbName() {
		return dbName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getQueryStr() {
		return queryStr;
	}

	public Query getQuery() {
		return query;
	}

	public List<String> getShardNames() {
		return shardNames;
	}

	public int getThreadNum() {
		return threadNum;
	}

	public void setThreadNum(int threadNum) {
		this.threadNum = threadNum;
	}

	public int getQueryID() {
		return queryID;
	}

	public void setQueryID(int queryID) {
		this.queryID = queryID;
	}

	public String getSortFiled() {
		return sortFiled;
	}

	public void setSortFiled(String sortFiled) {
		this.sortFiled = sortFiled;
	}

	public boolean isDesc() {
		return isDesc;
	}

	public void setDesc(boolean isDesc) {
		this.isDesc = isDesc;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public List<String> getAggregateFunctions() {
		return aggregateFunctions;
	}

	public void setAggregateFunctions(List<String> aggregateFunctions) {
		this.aggregateFunctions = aggregateFunctions;
	}

	public Map<String, String> getTypeMap() {
		return typeMap;
	}

	public void setTypeMap(Map<String, String> typeMap) {
		this.typeMap = typeMap;
	}

	public void clear() {
		queryID = -1; // 对应每次的查询链接，大于0时候有效
		returnFieldsName = null; // 用户结果集中需要返回数据的列
		dbName = null;
		tableName = null;
		queryStr = null;
		query = null;
		shardNames = null;
		threadNum = 1;
		// 目前的查询中还未使用到下面的属性
		sortFiled = "";
		group = null;
		aggregateFunctions = null;
	}

	public boolean isCount() {
		return isCount;
	}

	public void setCount(boolean isCount) {
		this.isCount = isCount;
	}

}
