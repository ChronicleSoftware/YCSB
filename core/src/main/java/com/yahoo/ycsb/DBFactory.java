package com.yahoo.ycsb;

import java.util.Properties;

/**
 * Creates a DB layer by dynamically classloading the specified DB class.
 */
public class DBFactory
{
      @SuppressWarnings("unchecked")
	public static DB newDB(String dbname, Properties properties) throws UnknownDBException
      {
	 ClassLoader classLoader = DBFactory.class.getClassLoader();

	 DB ret=null;

	 try 
	 {
	    Class dbclass = classLoader.loadClass(dbname);
	    //System.out.println("dbclass.getName() = " + dbclass.getName());
	    
	    ret=(DB)dbclass.newInstance();
	 }
	 catch (Exception e) 
	 {  
	    e.printStackTrace();
	    return null;
	 }
	 
	 ret.setProperties(properties);

	 return new DBWrapper(ret);
      }
      
}
