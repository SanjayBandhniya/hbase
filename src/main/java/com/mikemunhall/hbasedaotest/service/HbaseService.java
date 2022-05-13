package com.mikemunhall.hbasedaotest.service;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;

public interface HbaseService {

	List<Result> scaner(String tablename);

	Result getRow(String tableName, String rowKey);

	List<Result> getRegexRow(String tableName, String regxKey);

	List<Result> getRegexRow(String tableName, String regxKey, int num);

	List<Result> getStartRowAndEndRow(String tableName, String startKey, String stopKey);

	List<Result> getRegexRow(String tableName, String startKey, String stopKey, String regxKey);

	List<Result> getRegexRow(String tableName, String startKey, String stopKey, String regxKey, int num);

	void addData(String rowKey, String tableName,String familyName, String[] column, String[] value);

	void delRecord(String tableName, String... rowKeys);

	void updateTable(String tableName, String rowKey, String familyName, String column[], String value[])
			throws IOException;

	Result getNewRow(String tableName);

	List<String> queryKeys(String tableName, String regxKey);

	long incrQualifier(String tableName, String cf, String rowKey, String column, long num);

	Result getNewRow(String tableName, String regxKey);
	
	List<Result> getListRowKey(String tableName, List<String> rowKeys, String familyColumn, String column);

	List<Result> findById( String tableName,String familyName ,String columnName,  String columnValue);
}
