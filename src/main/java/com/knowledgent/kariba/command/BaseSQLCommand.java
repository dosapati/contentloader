package com.knowledgent.kariba.command;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by Hari.Dosapati on 12/20/2016.
 */
public class BaseSQLCommand  {
  private static final Logger log = LoggerFactory.getLogger(BaseSQLCommand.class);


  public DataSource dataSource(String driver, String url, String username, String password) {
    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName(driver);
    dataSource.setUrl(url);
    dataSource.setUsername(username);
    dataSource.setPassword(password);
    return dataSource;
  }

  public Set<String> getMaps(Connection conn, String sqlQuery, String[] colNames) throws SQLException {
    PreparedStatement ps = null;
    ResultSet rs = null;

    Set<String> retData = new LinkedHashSet<>();

    try {
      ps = conn.prepareStatement(sqlQuery.toString());

      rs = ps.executeQuery();


       Set<String> dataMap = new LinkedHashSet<>();
      while (rs.next()) {
        for (String colName : colNames) {
          String val = rs.getString(colName);
          if (StringUtils.isNotBlank(val)) {

            retData.add(val);
          }
        }
      }
    }finally {
      closeAllConnections(null,ps,rs);
    }
    return retData;
  }


  public void closeAllConnections(Connection conn, PreparedStatement ps, ResultSet rs) {
    if (rs != null) {
      try {
        rs.close();
      } catch (Exception e) {
      }
    }
    if (ps != null) {
      try {
        ps.close();
      } catch (Exception e) {
      }
    }
    if (conn != null) {
      try {
        conn.close();
      } catch (Exception e) {
      }
    }
  }

}
