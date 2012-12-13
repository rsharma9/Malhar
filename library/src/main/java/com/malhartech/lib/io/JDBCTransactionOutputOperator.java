/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.Context.OperatorContext;
import java.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public abstract class JDBCTransactionOutputOperator<T> extends JDBCOutputOperator<T>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCTransactionOutputOperator.class);
  protected Statement transactionStatement;
  private String operatorId;

  public void initTransactionInfo(OperatorContext context)
  {
    try {
      transactionStatement = getConnection().createStatement();
      DatabaseMetaData meta = getConnection().getMetaData();
      ResultSet rs1 = meta.getTables(null, null, "maxwindowid", null);
      if (rs1.next() == false) {
        logger.error("maxwindowid table not exist!");
        throw new RuntimeException("maxwindowid table not exist!");
      }

      String querySQL = "SELECT winid FROM maxwindowid WHERE operatorid ='" + context.getId() + "' AND appid=" + 0; // how can I get the appid
      ResultSet rs = transactionStatement.executeQuery(querySQL);
      if (rs.next() == false) {
        String insertSQL = "INSERT maxwindowid set appid=0, winid=0, operatorid='" + context.getId() + "'";
        transactionStatement.executeUpdate(insertSQL);
        logger.debug(insertSQL);
        lastWindowId = 0;
      }
      else {
        lastWindowId = rs.getLong("winid");
      }
      getConnection().setAutoCommit(false);
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
    initTransactionInfo(context);
    operatorId = context.getId();
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    this.windowId = windowId;
    if (windowId <= lastWindowId) {
      ignoreWindow = true;
    }
    else {
      ignoreWindow = false;
    }
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
    if (ignoreWindow) {
      return;
    }
    super.endWindow();
    try {
      String str = "UPDATE maxwindowid set winid=" + windowId + " WHERE appid=0 AND operatorid='" + operatorId + "'";
      transactionStatement.execute(str);
      logger.debug(str);
      getConnection().commit();
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }
}