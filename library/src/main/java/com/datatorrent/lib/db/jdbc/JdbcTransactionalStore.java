/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.db.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.annotation.Nonnull;

import com.datatorrent.lib.db.TransactionableStore;

/**
 * <p>JdbcTransactionalStore class.</p>
 *
 * @since 0.9.4
 */
public class JdbcTransactionalStore extends JdbcStore implements TransactionableStore
{
  public static String DEFAULT_APP_ID_COL = "dt_app_id";
  public static String DEFAULT_OPERATOR_ID_COL = "dt_operator_id";
  public static String DEFAULT_WINDOW_COL = "dt_window";
  public static String DEFAULT_META_TABLE = "dt_meta";

  @Nonnull
  protected String metaTableAppIdColumn;
  @Nonnull
  protected String metaTableOperatorIdColumn;
  @Nonnull
  protected String metaTableWindowColumn;
  @Nonnull
  private String metaTable;

  private boolean inTransaction;
  private transient PreparedStatement lastWindowFetchCommand;
  private transient PreparedStatement lastWindowInsertCommand;
  private transient PreparedStatement lastWindowUpdateCommand;
  private transient PreparedStatement lastWindowDeleteCommand;

  public JdbcTransactionalStore()
  {
    super();
    metaTable = DEFAULT_META_TABLE;
    metaTableAppIdColumn = DEFAULT_APP_ID_COL;
    metaTableOperatorIdColumn = DEFAULT_OPERATOR_ID_COL;
    metaTableWindowColumn = DEFAULT_WINDOW_COL;
    inTransaction = false;
  }

  /**
   * Sets the name of the meta table.<br/>
   * <b>Default:</b> {@value #DEFAULT_META_TABLE}
   *
   * @param metaTable meta table name.
   */
  public void setMetaTable(@Nonnull String metaTable)
  {
    this.metaTable = metaTable;
  }

  /**
   * Sets the name of app id column.<br/>
   * <b>Default:</b> {@value #DEFAULT_APP_ID_COL}
   *
   * @param appIdColumn application id column name.
   */
  public void setMetaTableAppIdColumn(@Nonnull String appIdColumn)
  {
    this.metaTableAppIdColumn = appIdColumn;
  }

  /**
   * Sets the name of operator id column.<br/>
   * <b>Default:</b> {@value #DEFAULT_OPERATOR_ID_COL}
   *
   * @param operatorIdColumn operator id column name.
   */
  public void setMetaTableOperatorIdColumn(@Nonnull String operatorIdColumn)
  {
    this.metaTableOperatorIdColumn = operatorIdColumn;
  }

  /**
   * Sets the name of the window column.<br/>
   * <b>Default:</b> {@value #DEFAULT_WINDOW_COL}
   *
   * @param windowColumn window column name.
   */
  public void setMetaTableWindowColumn(@Nonnull String windowColumn)
  {
    this.metaTableWindowColumn = windowColumn;
  }

  @Override
  public void connect()
  {
    super.connect();
    try {
      String command = "select " + metaTableWindowColumn + " from " + metaTable + " where " + metaTableAppIdColumn +
        " = ? and " + metaTableOperatorIdColumn + " = ?";
      logger.debug(command);
      lastWindowFetchCommand = connection.prepareStatement(command);

      command = "insert into " + metaTable + " (" + metaTableAppIdColumn + ", " + metaTableOperatorIdColumn + ", " +
        metaTableWindowColumn + ") values (?,?,?)";
      logger.debug(command);
      lastWindowInsertCommand = connection.prepareStatement(command);

      command = "update " + metaTable + " set " + metaTableWindowColumn + " = ? where " + metaTableAppIdColumn + " = ? " +
        " and " + metaTableOperatorIdColumn + " = ?";
      logger.debug(command);
      lastWindowUpdateCommand = connection.prepareStatement(command);

      command = "delete from " + metaTable + " where " + metaTableAppIdColumn + " = ? and " + metaTableOperatorIdColumn + " = ?";
      logger.debug(command);
      lastWindowDeleteCommand = connection.prepareStatement(command);

      connection.setAutoCommit(false);
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void disconnect()
  {
    if (lastWindowUpdateCommand != null) {
      try {
        lastWindowUpdateCommand.close();
      }
      catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    super.disconnect();
  }

  @Override
  public void beginTransaction()
  {
    inTransaction = true;
  }

  @Override
  public void commitTransaction()
  {
    try {
      connection.commit();
      inTransaction = false;
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void rollbackTransaction()
  {
    try {
      connection.rollback();
      inTransaction = false;
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isInTransaction()
  {
    return inTransaction;
  }

  @Override
  public long getCommittedWindowId(String appId, int operatorId)
  {
    try {
      lastWindowFetchCommand.setString(1, appId);
      lastWindowFetchCommand.setInt(2, operatorId);
      long lastWindow = -1;
      ResultSet resultSet = lastWindowFetchCommand.executeQuery();
      if (resultSet.next()) {
        lastWindow = resultSet.getLong(1);
      }
      else {
        lastWindowInsertCommand.setString(1, appId);
        lastWindowInsertCommand.setInt(2, operatorId);
        lastWindowInsertCommand.setLong(3, -1);
        lastWindowInsertCommand.executeUpdate();
        lastWindowInsertCommand.close();
        connection.commit();
      }
      lastWindowFetchCommand.close();
      return lastWindow;
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void storeCommittedWindowId(String appId, int operatorId, long windowId)
  {
    try {
      lastWindowUpdateCommand.setLong(1, windowId);
      lastWindowUpdateCommand.setString(2, appId);
      lastWindowUpdateCommand.setInt(3, operatorId);
      lastWindowUpdateCommand.executeUpdate();
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void removeCommittedWindowId(String appId, int operatorId)
  {
    try {
      lastWindowDeleteCommand.setString(1, appId);
      lastWindowDeleteCommand.setInt(2, operatorId);
      lastWindowDeleteCommand.executeUpdate();
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

}
