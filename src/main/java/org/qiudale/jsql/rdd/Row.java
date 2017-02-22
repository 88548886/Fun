package org.qiudale.jsql.rdd;

import java.util.LinkedHashMap;
import java.util.Map;

public class Row {
	protected Map<String,Object> cellValues = new LinkedHashMap<String,Object>();
	
	public void addColumn(String colName,Object cellValue){
		cellValues.put(colName, cellValue);
	}
	
	public Object getRaw(String columnName){
		return cellValues.get(columnName);
	}
	public String getAsString(String columnName){
		return cellValues.get(columnName).toString();
	}
	public int getAsInt(String columnName){
		return Integer.parseInt(cellValues.get(columnName).toString());
	}
}
