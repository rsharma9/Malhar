/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.Context.OperatorContext;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public abstract class JDBCNonTransactionOutputOperator<T> extends JDBCOutputOperator<T>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCNonTransactionOutputOperator.class);
  protected Statement statement;

  /**
   * Prepare insert query statement using column names from mapping.
   *
   */
  @Override
  protected void prepareInsertStatement()
  {
    int num = getColumnNames().size();
    if (num < 1) {
      return;
    }
    String columns = "";
    String values = "";
    String space = " ";
    String comma = ",";
    String question = "?";

    for (int idx = 0; idx < num; ++idx) {
      if (idx == 0) {
        columns = getColumnNames().get(idx);
        values = question;
      }
      else {
        columns = columns + comma + space + getColumnNames().get(idx);
        values = values + comma + space + question;
      }
    }

    columns = columns + comma + space + "winid";
    values = values + comma + space + question;

    String insertQuery = "INSERT INTO " + getTableName() + " (" + columns + ") VALUES (" + values + ")";
    logger.debug(String.format("%s", insertQuery));
    try {
      setInsertStatement(getConnection().prepareStatement(insertQuery));
    }
    catch (SQLException ex) {
      throw new RuntimeException("Error while preparing insert statement", ex);
    }
  }

  public void initLastWindowInfo()
  {
    try {
      statement = getConnection().createStatement();
      String stmt = "SELECT MAX(winid) AS winid FROM " + getTableName();
      ResultSet rs = statement.executeQuery(stmt);
        logger.debug(stmt);
      if (rs.next() == false) {
        logger.error("table " + getTableName() + " winid column not ready!");
        throw new RuntimeException("table " + getTableName() + " winid column not ready!");
      }
      lastWindowId = rs.getLong("winid");
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Implement Component Interface.
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    initLastWindowInfo();
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    if (windowId < lastWindowId) {
      ignoreWindow = true;
    }
    else if (windowId == lastWindowId) {
      ignoreWindow = false;
      try {
        String stmt = "DELETE FROM " + getTableName() + " WHERE winid=" + windowId;
        statement.execute(stmt);
        logger.debug(stmt);
      }
      catch (SQLException ex) {
        throw new RuntimeException("Error while deleting windowId from db", ex);
      }
    }
    else {
      ignoreWindow = false;
    }
  }
}