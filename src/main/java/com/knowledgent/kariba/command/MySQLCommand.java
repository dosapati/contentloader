package com.knowledgent.kariba.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.*;

/**
 * Created by Hari.Dosapati on 12/5/2016.
 */
@Component
public class MySQLCommand extends BaseSQLCommand implements Command {
  // Implements Command Interface...
  private static final Logger log = LoggerFactory.getLogger(MySQLCommand.class);


  public Map<String, Set<String>> loadData(final Map<String, String> configInfo) {
    System.out.println("MySQL Command is getting executed... !!");
    Connection conn = null;

    try {
      DataSource dataSource = dataSource(configInfo.get("jdbc.driver"), configInfo.get("jdbc.url"), configInfo.get("db.username"), configInfo.get("db.password"));
      conn = dataSource.getConnection();
      log.info("Got the MYSQL connection");


      String[] tables = configInfo.get("table.names").split("\\|");
      String[] columns = configInfo.get("columns").split("\\|");
      String[] schemas = configInfo.get("schemas").split("\\|");

      String[] docIds = configInfo.get("solr.update.docids").split("\\|");

      Map<String,Set<String>> retData = new LinkedHashMap<>();
      int cnt = 0;
      for(String table:tables){
        String[] colNames = columns[cnt].split(",");

        StringBuilder sqlQuery = new StringBuilder().append("select distinct ").append(columns[cnt]).append(" from  ").append(schemas[cnt]).append(".").append(table);
        log.info("Executing SQL Query as -->{} & table being queried is {} , columns are -> {} ",sqlQuery, table,columns[cnt]);

        //should maintain the order of table entries..
        retData.put( docIds[cnt],getMaps(conn, sqlQuery.toString(), colNames));

        log.info("dataMap size at the end is -->{} .. table being queried is {}",retData.size(), table);

        cnt++;
      }

      return retData;

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      closeAllConnections(conn, null, null);
    }
    return null;
  }



}
