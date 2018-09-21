package com.esoft.dp.dao;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.esoft.dp.utils.InterpreterResult;
import com.esoft.dp.utils.InterpreterResult.Code;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveDaoUtility {
	
	private static Logger logger = LoggerFactory.getLogger(HiveDaoUtility.class);
	
	private String hiveServerURL;//Config.props.getProperty("hiveServerURL");
	
	private String hiveServerUser;
	
	private String hiveServerPassword ;
	
	Connection jdbcConnection;
	
	Exception exceptionOnConnect;
	
	public Connection getJdbcConnection()
		      throws SQLException {
		    return DriverManager.getConnection(hiveServerURL, hiveServerUser, hiveServerPassword);
	}
	
	public void open() {
	    logger.info("Jdbc open connection called!");
	    try {
	      String driverName = "org.apache.hive.jdbc.HiveDriver";
	      Class.forName(driverName);
	    } catch (ClassNotFoundException e) {
	      logger.error("Can not open connection", e);
	      exceptionOnConnect = e;
	      return;
	    }
	    try {
	      jdbcConnection = getJdbcConnection();
	      exceptionOnConnect = null;
	      logger.info("Successfully created Jdbc connection");
	    }
	    catch (SQLException e) {
	      logger.error("Cannot open connection", e);
	      exceptionOnConnect = e;
	    }
	  }
	
	 public void close() {
		    try {
		      if (jdbcConnection != null) {
		        jdbcConnection.close();
		      }
		    }
		    catch (SQLException e) {
		      logger.error("Cannot close connection", e);
		    }
		    finally {
		      jdbcConnection = null;
		      exceptionOnConnect = null;
		    }
	 }
	 
	 
	 public InterpreterResult executeSql(String sql) {
		 
		 Statement currentStatement;
		 open();
	    try {
	      if (exceptionOnConnect != null) {
	        return new InterpreterResult(Code.ERROR, exceptionOnConnect.getMessage());
	      }
	      currentStatement = jdbcConnection.createStatement();
	      StringBuilder msg = null;
	      if (StringUtils.containsIgnoreCase(sql, "EXPLAIN ")) {
	        //return the explain as text, make this visual explain later
	        msg = new StringBuilder();
	      }
	      else {
	        msg = new StringBuilder("%table ");
	      }
	      ResultSet res = currentStatement.executeQuery(sql);
	      try {
	        ResultSetMetaData md = res.getMetaData();
	        for (int i = 1; i < md.getColumnCount() + 1; i++) {
	          if (i == 1) {
	            msg.append(md.getColumnName(i));
	          } else {
	            msg.append("\t" + md.getColumnName(i));
	          }
	        }
	        msg.append("\n");
	        while (res.next()) {
	          for (int i = 1; i < md.getColumnCount() + 1; i++) {
	            msg.append(res.getString(i) + "\t");
	          }
	          msg.append("\n");
	        }
	      }
	      finally {
	        try {
	          res.close();
	          currentStatement.close();
	        }
	        finally {
	        	close();
	          currentStatement = null;
	        }
	      }

	      InterpreterResult rett = new InterpreterResult(Code.SUCCESS, msg.toString());
	      return rett;
	    }
	    catch (SQLException ex) {
	      logger.error("Can not run " + sql, ex);
	      return new InterpreterResult(Code.ERROR, ex.getMessage());
	    }
	  }
	 
	 public InterpreterResult execute(String sql) {
		 Statement currentStatement = null;
		 open();
	    try {
	      if (exceptionOnConnect != null) {
	        return new InterpreterResult(Code.ERROR, exceptionOnConnect.getMessage());
	      }
	      currentStatement = jdbcConnection.createStatement();
	      StringBuilder msg = null;
	      if (StringUtils.containsIgnoreCase(sql, "EXPLAIN ")) {
	        //return the explain as text, make this visual explain later
	        msg = new StringBuilder();
	      }
	      else {
	        msg = new StringBuilder("%table ");
	      }
	      currentStatement.execute(sql);
	      InterpreterResult rett = new InterpreterResult(Code.SUCCESS, msg.toString());
	      return rett;
	    }catch (SQLException ex) {
		      logger.error("Can not run " + sql, ex);
		      return new InterpreterResult(Code.ERROR, ex.getMessage());
		    }
	      finally {
	    	  try {
				currentStatement.close();
			} catch (SQLException e) {
			}
	        	close();
	          //currentStatement = null;
	      }
	    
	    
	  }
	
	public void setHiveServerURL(String hiveServerURL) {
		this.hiveServerURL = hiveServerURL;
	}

	public void setHiveServerUser(String hiveServerUser) {
		this.hiveServerUser = hiveServerUser;
	}

	public void setHiveServerPassword(String hiveServerPassword) {
		this.hiveServerPassword = hiveServerPassword;
	}

}
