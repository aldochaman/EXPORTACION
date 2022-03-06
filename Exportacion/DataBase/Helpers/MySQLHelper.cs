// ===============================================================================
// Microsoft Data Access Application Block for .NET
// http://msdn.microsoft.com/library/en-us/dnbda/html/daab-rm.asp
//
// SQLHelper.cs
//
// This file contains the implementations of the MySqlHelper and MySqlHelperParameterCache
// classes.
//
// For more information see the Data Access Application Block Implementation Overview.
// ===============================================================================
// Release history
// VERSION      DESCRIPTION
//   2.0        Added support for FillDataset, UpdateDataset and "Param" helper methods
//
// ===============================================================================
// Copyright (C) 2000-2001 Microsoft Corporation
// All rights reserved.
// THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY
// OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
// LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR
// FITNESS FOR A PARTICULAR PURPOSE.
// ==============================================================================

using Framework.DataBase.Utilities;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Xml;

namespace Framework.DataBase
{
     /// <summary>
     /// The MySqlHelper class is intended to encapsulate high performance, scalable best practices for  common uses of SqlClient.
     /// </summary>
     public sealed class MySqlHelper
     {
          #region "private utility methods & constructors"

          /// <summary>
          /// Since this class provides only static methods, make the default constructor private to prevent instances from being created with "new MySqlHelper()".
          /// </summary>
          private MySqlHelper()
          {
          }

          /// <summary>
          /// This method is used to attach array of MySqlParameters to a MySqlCommand.
          /// This method will assign a value of DbNull to any parameter with a direction of
          /// InputOutput and a value of null.
          /// This behavior will prevent default values from being used, but
          /// this will be the less common case than an intended pure output parameter (derived as InputOutput)
          /// where the user provided no input value.
          /// </summary>
          /// <param name="command">The command to which the parameters will be added</param>
          /// <param name="commandParameters">an array of MySqlParameters to be added to command</param>
          private static void AttachParameters(MySqlCommand command, MySqlParameter[] commandParameters)
          {
               if ((command == null))
                    throw new ArgumentNullException("command");
               if (((commandParameters != null)))
               {
                    MySqlParameter p = null;
                    foreach (MySqlParameter p_loopVariable in commandParameters)
                    {
                         p = p_loopVariable;
                         if (((p != null)))
                         {
                              // Check for derived output value with no value assigned
                              if ((p.Direction == ParameterDirection.InputOutput || p.Direction == ParameterDirection.Input) && p.Value == null)
                                   p.Value = DBNull.Value;
                              command.Parameters.Add(p);
                         }
                    }
               }
          }

          /// <summary>
          /// Attaches the parameters.
          /// </summary>
          /// <param name="command">The command.</param>
          /// <param name="commandParameters">The command parameters.</param>
          private static void AttachParameters(MySqlCommand command, IEnumerable<ParameterSql> commandParameters)
          {              
               if (commandParameters == null)
                    return;
               HashSet<string> parameter = new HashSet<string>();
               foreach (ParameterSql loParametro in commandParameters)
               {
                    if (parameter.Contains(loParametro.Parameter))
                         continue;
                    if (loParametro.Value == null)
                    {
                         // Esto se da para evitar consultas con parametros con valores nulos, asi se rempleza el null (lenguaje)  por el dbnull del motor
                         command.Parameters.AddWithValue(loParametro.Parameter, DBNull.Value);
                    }
                    else
                    {
                         command.Parameters.AddWithValue(loParametro.Parameter, loParametro.Value);
                    }
                    parameter.Add(loParametro.Parameter);
               }
          }

          /// <summary>
          /// This method assigns dataRow column values to an array of MySqlParameters
          /// </summary>
          /// <param name="commandParameters">Array of MySqlParameters to be assigned values</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values</param>
          private static void AssignParameterValues(MySqlParameter[] commandParameters, DataRow dataRow)
          {
               if (commandParameters == null || dataRow == null)
               {
                    // Do nothing if we get no data
                    return;
               }

               // Set the parameters values
               MySqlParameter commandParameter = null;
               int i = 0;
               foreach (MySqlParameter commandParameter_loopVariable in commandParameters)
               {
                    commandParameter = commandParameter_loopVariable;
                    // Check the parameter name
                    if ((commandParameter.ParameterName == null || commandParameter.ParameterName.Length <= 1))
                    {
                         throw new Exception(string.Format("Please provide a valid parameter name on the parameter #{0}, the ParameterName property has the following value: ' {1}' .", i, commandParameter.ParameterName));
                    }
                    if (dataRow.Table.Columns.IndexOf(commandParameter.ParameterName.Substring(1)) != -1)
                    {
                         commandParameter.Value = dataRow[commandParameter.ParameterName.Substring(1)];
                    }
                    i = i + 1;
               }
          }

          /// <summary>
          /// This method assigns an array of values to an array of MySqlParameters
          /// </summary>
          /// <param name="commandParameters">Array of MySqlParameters to be assigned values</param>
          /// <param name="parameterValues">Array of objects holding the values to be assigned</param>
          private static void AssignParameterValues(MySqlParameter[] commandParameters, object[] parameterValues)
          {
               int i = 0;
               int j = 0;
               IDbDataParameter paramInstance;
               if ((commandParameters == null) && (parameterValues == null))
               {
                    // Do nothing if we get no data
                    return;
               }

               // We must have the same number of values as we pave parameters to put them in
               if (commandParameters.Length != parameterValues.Length)
               {
                    throw new ArgumentException("Parameter count does not match Parameter Value count.");
               }

               // Value array
               j = commandParameters.Length - 1;
               for (i = 0; i <= j; i++)
               {
                    // If the current array value derives from IDbDataParameter, then assign its Value property
                    if (parameterValues[i] is IDbDataParameter)
                    {
                         paramInstance = (IDbDataParameter)parameterValues[i];
                         if ((paramInstance.Value == null))
                         {
                              commandParameters[i].Value = DBNull.Value;
                         }
                         else
                         {
                              commandParameters[i].Value = paramInstance.Value;
                         }
                    }
                    else if ((parameterValues[i] == null))
                    {
                         commandParameters[i].Value = DBNull.Value;
                    }
                    else
                    {
                         commandParameters[i].Value = parameterValues[i];
                    }
               }
          }

          /// <summary>
          /// This method opens (if necessary) and assigns a connection, transaction, command type and parameters
          /// to the provided command
          /// </summary>
          /// <param name="command">The MySqlCommand to be prepared</param>
          /// <param name="connection">A valid MySqlConnection, on which to execute this command</param>
          /// <param name="transaction">A valid MySqlTransaction, or 'null'</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of MySqlParameters to be associated with the command or 'null' if no parameters are required</param>
          /// <param name="mustCloseConnection"><c>true</c> if the connection was opened by the method, otherwose is false.</param>
          private static void PrepareCommand(MySqlCommand command, MySqlConnection connection, MySqlTransaction transaction, CommandType commandType, string commandText, MySqlParameter[] commandParameters, ref bool mustCloseConnection)
          {
               if ((command == null))
                    throw new ArgumentNullException("command");
               if ((commandText == null || commandText.Length == 0))
                    throw new ArgumentNullException("commandText");
               // If the provided connection is not open, we will open it
               if (connection.State != ConnectionState.Open)
               {
                    connection.Open();
                    mustCloseConnection = true;
               }
               else
               {
                    mustCloseConnection = false;
               }

               // Associate the connection with the command
               command.Connection = connection;
               command.CommandTimeout = 0;
               //14/Junio/2007 Iván para que espere indefinidamente

               // Set the command text (stored procedure name or SQL statement)
               command.CommandText = commandText;

               // If we were provided a transaction, assign it.
               if ((transaction != null))
               {
                    if (transaction.Connection == null)
                         throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
                    command.Transaction = transaction;
               }
               // Set the command type
               command.CommandType = commandType;
               // Attach the command parameters if they are provided
               if ((commandParameters != null))
                    AttachParameters(command, commandParameters);
               return;
          }

          /// <summary>
          /// Prepares the command.
          /// </summary>
          /// <param name="command">The command.</param>
          /// <param name="connection">The connection.</param>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandParameters">The command parameters.</param>
          /// <param name="pbIlimitado">if set to <c>true</c> [pb ilimitado].</param>
          /// <exception cref="ArgumentException">The transaction was rollbacked or commited, please provide an open transaction.;transaction</exception>
          private static void PrepareCommand(MySqlCommand command, MySqlConnection connection, MySqlTransaction transaction, CommandType commandType, IEnumerable<ParameterSql> commandParameters, bool pbIlimitado)
          {
               // If the provided connection is not open, we will open it
               if (connection.State != ConnectionState.Open)
               {
                    connection.Open();
               }
               // Associate the connection with the command
               command.Connection = connection;
               if (pbIlimitado)
                    command.CommandTimeout = 0;
               // If we were provided a transaction, assign it.
               if ((transaction != null))
               {
                    if (transaction.Connection == null)
                         throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
                    command.Transaction = transaction;
               }
               // Set the command type
               command.CommandType = commandType;
               // Attach the command parameters if they are provided
               if (commandParameters != null)
                    AttachParameters(command, commandParameters);
               return;
          }

          #endregion "private utility methods & constructors"

          #region "ExecuteNonQuery"

          /// <summary>
          /// Execute a MySqlCommand (that returns no resultset and takes no parameters) against the database specified in
          /// the connection string
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders");
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of MySqlParameters
               return ExecuteNonQuery(connectionString, commandType, commandText, (MySqlParameter[])null);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns no resultset) against the database specified in the connection string
          /// using the provided parameters
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders", new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
          {
               if ((connectionString == null || connectionString.Length == 0))
                    throw new ArgumentNullException("connectionString");
               // Create & open a MySqlConnection, and dispose of it after we are done
               using (MySqlConnection connection = new MySqlConnection(connectionString))
               {
                    connection.Open();
                    // Call the overload that takes a connection in place of the connection string
                    return ExecuteNonQuery(connection, commandType, commandText, commandParameters);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns no resultset) against the database specified in
          /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  int result = ExecuteNonQuery(connString, "PublishOrders", 24, 36);
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="spName">The name of the stored prcedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQuery(string connectionString, string spName, params object[] parameterValues)
          {
               if ((connectionString == null || connectionString.Length == 0))
                    throw new ArgumentNullException("connectionString");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               MySqlParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connectionString, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of MySqlParameters
                    return ExecuteNonQuery(connectionString, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteNonQuery(connectionString, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns no resultset and takes no parameters) against the provided MySqlConnection.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int result = ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders");
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQuery(MySqlConnection connection, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of MySqlParameters
               return ExecuteNonQuery(connection, commandType, commandText, (MySqlParameter[])null);
          }

          /// <summary>
          /// Executes the non query.
          /// </summary>
          /// <param name="connection">The connection.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <param name="pbIlimitado">if set to <c>true</c> [pb ilimitado].</param>
          /// <returns></returns>
          public static int ExecuteNonQuery(MySqlConnection connection, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> poParametros, bool pbIlimitado)
          {
               return ExecuteNonQuery(connection, (MySqlTransaction)null, commandType, commandText, poParametros, pbIlimitado);
          }

          /// <summary>
          /// Executes the non query.
          /// </summary>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <param name="pbIlimitado">if set to <c>true</c> [pb ilimitado].</param>
          /// <returns></returns>
          public static int ExecuteNonQuery(MySqlTransaction transaction, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> poParametros, bool pbIlimitado)
          {
               return ExecuteNonQuery(transaction.Connection, transaction, commandType, commandText, poParametros, pbIlimitado);
          }

          /// <summary>
          /// Executes the non query.
          /// </summary>
          /// <param name="connection">The connection.</param>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <param name="pbIlimitado">if set to <c>true</c> [pb ilimitado].</param>
          /// <returns></returns>
          private static int ExecuteNonQuery(MySqlConnection connection, MySqlTransaction transaction, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> poParametros, bool pbIlimitado)
          {
               int retval = 0;
               // Pass through the call providing null for the set of MySqlParameters
               using (MySqlCommand loComando = new MySqlCommand(commandText.ToString()))
               {
                    //Si el tiempo de espera es mayor al esperado, Se colaca un cero para que el tiempo sea ilimitado, esto es para carga de archivos
                    //Esto se arregla poniendo un cero en el time out para que sea ilimitado
                    PrepareCommand(loComando, connection, transaction, commandType, poParametros, pbIlimitado);
                    retval = loComando.ExecuteNonQuery();
                    loComando.Parameters.Clear();
                    return retval;
               }
          }

          /// <summary>
          /// Stores the procedure execute non query.
          /// </summary>
          /// <param name="connection">The connection.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <returns></returns>
          public static int StoreProcedureExecuteNonQuery(MySqlConnection connection, CommandType commandType, string commandText, IEnumerable<ParameterSql> poParametros)
          {
               return StoreProcedureExecuteNonQuery(connection, (MySqlTransaction)null, commandType, commandText, poParametros);
          }

          /// <summary>
          /// Stores the procedure execute non query.
          /// </summary>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <returns></returns>
          public static int StoreProcedureExecuteNonQuery(MySqlTransaction transaction, CommandType commandType, string commandText, IEnumerable<ParameterSql> poParametros)
          {
               return StoreProcedureExecuteNonQuery(transaction.Connection, transaction, commandType, commandText, poParametros);
          }

          /// <summary>
          /// Stores the procedure execute non query.
          /// </summary>
          /// <param name="connection">The connection.</param>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <returns></returns>
          private static int StoreProcedureExecuteNonQuery(MySqlConnection connection, MySqlTransaction transaction, CommandType commandType, string commandText, IEnumerable<ParameterSql> poParametros)
          {
               int retval = 0;
               // Pass through the call providing null for the set of MySqlParameters
               // Create a command and prepare it for execution
               using (MySqlCommand command = new MySqlCommand(commandText))
               {
                    PrepareCommand(command, connection, transaction, commandType, poParametros, true);
                    // Create the DataAdapter & DataSet
                    using (MySqlDataAdapter dataAdapter = new MySqlDataAdapter(command))
                    {
                         // Add the table mappings specified by the user
                         // Fill the DataSet using default values for DataTable names, etc
                         retval = dataAdapter.SelectCommand.ExecuteNonQuery();
                         // Detach the MySqlParameters from the command object, so they can be used again
                         command.Parameters.Clear();
                         return retval;
                    }
               }
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns no resultset) against the specified MySqlConnection
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int result = ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders", new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQuery(MySqlConnection connection, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               // Create a command and prepare it for execution
               MySqlCommand cmd = new MySqlCommand();
               int retval = 0;
               bool mustCloseConnection = false;
               PrepareCommand(cmd, connection, (MySqlTransaction)null, commandType, commandText, commandParameters, ref mustCloseConnection);
               // Finally, execute the command
               retval = cmd.ExecuteNonQuery();
               // Detach the MySqlParameters from the command object, so they can be used again
               cmd.Parameters.Clear();
               if ((mustCloseConnection))
                    connection.Close();
               return retval;
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns no resultset) against the specified MySqlConnection
          /// using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  int result = ExecuteNonQuery(conn, "PublishOrders", 24, 36);
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQuery(MySqlConnection connection, string spName, params object[] parameterValues)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               MySqlParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of MySqlParameters
                    return ExecuteNonQuery(connection, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteNonQuery(connection, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns no resultset and takes no parameters) against the provided MySqlTransaction.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int result = ExecuteNonQuery(trans, CommandType.StoredProcedure, "PublishOrders");
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <returns>An int representing the number of rows affected by the command</returns>

          public static int ExecuteNonQuery(MySqlTransaction transaction, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of MySqlParameters
               return ExecuteNonQuery(transaction, commandType, commandText, (MySqlParameter[])null);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns no resultset) against the specified MySqlTransaction
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int result = ExecuteNonQuery(trans, CommandType.StoredProcedure, "GetOrders", new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQuery(MySqlTransaction transaction, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               // Create a command and prepare it for execution
               MySqlCommand cmd = new MySqlCommand();
               int retval = 0;
               bool mustCloseConnection = false;
               PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters, ref mustCloseConnection);
               // Finally, execute the command
               retval = cmd.ExecuteNonQuery();
               // Detach the MySqlParameters from the command object, so they can be used again
               cmd.Parameters.Clear();
               return retval;
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns no resultset) against the specified
          /// MySqlTransaction using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  int result = ExecuteNonQuery(conn, trans, "PublishOrders", 24, 36);
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQuery(MySqlTransaction transaction, string spName, params object[] parameterValues)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               MySqlParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = MySqlHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of MySqlParameters
                    return ExecuteNonQuery(transaction, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteNonQuery(transaction, CommandType.StoredProcedure, spName);
               }
          }

          // ExecuteNonQuery

          #endregion "ExecuteNonQuery"

          #region "ExecuteDataset"

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset and takes no parameters) against the database specified in
          /// the connection string.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(connString, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>

          public static DataSet ExecuteDataset(string connectionString, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of MySqlParameters
               return ExecuteDataset(connectionString, commandType, commandText, (MySqlParameter[])null);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset) against the database specified in the connection string
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(connString, CommandType.StoredProcedure, "GetOrders", new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDataset(string connectionString, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
          {
               if ((connectionString == null || connectionString.Length == 0))
               {
                    throw new ArgumentNullException("connectionString");
               }
               // Create & open a MySqlConnection, and dispose of it after we are done
               using (MySqlConnection connection = new MySqlConnection(connectionString))
               {
                    connection.Open();
                    // Call the overload that takes a connection in place of the connection string
                    return ExecuteDataset(connection, commandType, commandText, commandParameters);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the database specified in
          /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(connString, "GetOrders", 24, 36);
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDataset(string connectionString, string spName, params object[] parameterValues)
          {
               if ((connectionString == null || connectionString.Length == 0))
                    throw new ArgumentNullException("connectionString");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               MySqlParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connectionString, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of MySqlParameters
                    return ExecuteDataset(connectionString, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteDataset(connectionString, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset and takes no parameters) against the provided MySqlConnection.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(conn, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>

          public static DataSet ExecuteDataset(MySqlConnection connection, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of MySqlParameters
               return ExecuteDataset(connection, commandType, commandText, (MySqlParameter[])null);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset) against the specified MySqlConnection
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(conn, CommandType.StoredProcedure, "GetOrders", new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDataset(MySqlConnection connection, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               // Create a command and prepare it for execution
               MySqlCommand cmd = new MySqlCommand();
               DataSet ds = new DataSet();
               bool mustCloseConnection = false;
               PrepareCommand(cmd, connection, (MySqlTransaction)null, commandType, commandText, commandParameters, ref mustCloseConnection);
               using (MySqlDataAdapter dataAdatpter = new MySqlDataAdapter(cmd))
               {
                    // Fill the DataSet using default values for DataTable names, etc
                    dataAdatpter.Fill(ds);
                    // Detach the MySqlParameters from the command object, so they can be used again
                    cmd.Parameters.Clear();
                    if ((mustCloseConnection))
                         connection.Close();
                    // Return the dataset
                    return ds;
               }
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the specified MySqlConnection
          /// using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(conn, "GetOrders", 24, 36);
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDataset(MySqlConnection connection, string spName, params object[] parameterValues)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               MySqlParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);

                    // Call the overload that takes an array of MySqlParameters
                    return ExecuteDataset(connection, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteDataset(connection, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset and takes no parameters) against the provided MySqlTransaction.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(trans, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDataset(MySqlTransaction transaction, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of MySqlParameters
               return ExecuteDataset(transaction, commandType, commandText, (MySqlParameter[])null);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset) against the specified MySqlTransaction
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(trans, CommandType.StoredProcedure, "GetOrders", new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDataset(MySqlTransaction transaction, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               // Create a command and prepare it for execution
               MySqlCommand cmd = new MySqlCommand();
               DataSet ds = new DataSet();
               bool mustCloseConnection = false;
               PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters, ref mustCloseConnection);
               using (MySqlDataAdapter dataAdatpter = new MySqlDataAdapter(cmd))
               {
                    // Fill the DataSet using default values for DataTable names, etc
                    dataAdatpter.Fill(ds);
                    // Detach the MySqlParameters from the command object, so they can be used again
                    cmd.Parameters.Clear();
                    // Return the dataset
                    return ds;
               }
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the specified
          /// MySqlTransaction using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(trans, "GetOrders", 24, 36);
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDataset(MySqlTransaction transaction, string spName, params object[] parameterValues)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               MySqlParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = MySqlHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of MySqlParameters
                    return ExecuteDataset(transaction, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteDataset(transaction, CommandType.StoredProcedure, spName);
               }
          }

          // ExecuteDataset

          #endregion "ExecuteDataset"

          #region "ExecuteReader"

          /// <summary>
          /// This enum is used to indicate whether the connection was provided by the caller, or created by MySqlHelper, so that
          /// we can set the appropriate CommandBehavior when calling ExecuteReader()
          /// </summary>

          private enum MySqlConnectionOwnership
          {
               /// <summary>Connection is owned and managed by MySqlHelper</summary>
               Internal,

               /// <summary>Connection is owned and managed by the caller</summary>
               External
          }

          /// <summary>
          /// Create and prepare a MySqlCommand, and call ExecuteReader with the appropriate CommandBehavior.
          /// </summary>
          /// <remarks>
          /// If we created and opened the connection, we want the connection to be closed when the DataReader is closed.
          ///
          /// If the caller provided the connection, we want to leave it to them to manage.
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection, on which to execute this command</param>
          /// <param name="transaction">A valid MySqlTransaction, or 'null'</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of MySqlParameters to be associated with the command or 'null' if no parameters are required</param>
          /// <param name="connectionOwnership">Indicates whether the connection parameter was provided by the caller, or created by MySqlHelper</param>
          /// <returns>MySqlDataReader containing the results of the command</returns>
          private static MySqlDataReader ExecuteReader(MySqlConnection connection, MySqlTransaction transaction, CommandType commandType, string commandText, MySqlParameter[] commandParameters, MySqlConnectionOwnership connectionOwnership)
          {
               MySqlDataReader dataReader;
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               bool mustCloseConnection = false;
               // Create a command and prepare it for execution
               MySqlCommand cmd = new MySqlCommand();
               try
               {
                    // Create a reader
                    PrepareCommand(cmd, connection, transaction, commandType, commandText, commandParameters, ref mustCloseConnection);
                    // Call ExecuteReader with the appropriate CommandBehavior
                    if (connectionOwnership == MySqlConnectionOwnership.External)
                         dataReader = cmd.ExecuteReader();
                    else
                         dataReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                    // Detach the MySqlParameters from the command object, so they can be used again.
                    // HACK: There is a problem here, the output parameter values are fletched
                    // when the reader is closed, so if the parameters are detached from the command
                    // then the SqlReader can´t set its values.
                    // When this happen, the parameters can´t be used again in other command.
                    bool canClear = true;
                    foreach (MySqlParameter commandParameter in cmd.Parameters)
                    {
                         if (commandParameter.Direction != ParameterDirection.Input)
                              canClear = false;
                    }
                    if ((canClear))
                         cmd.Parameters.Clear();
                    return dataReader;
               }
               catch
               {
                    if ((mustCloseConnection))
                         connection.Close();
                    throw;
               }
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset and takes no parameters) against the database specified in
          /// the connection string.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  MySqlDataReader dr = ExecuteReader(connString, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <returns>A MySqlDataReader containing the resultset generated by the command</returns>
          public static MySqlDataReader ExecuteReader(string connectionString, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of MySqlParameters
               return ExecuteReader(connectionString, commandType, commandText, (MySqlParameter[])null);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset) against the database specified in the connection string
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  MySqlDataReader dr = ExecuteReader(connString, CommandType.StoredProcedure, "GetOrders", new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          /// <returns>A MySqlDataReader containing the resultset generated by the command</returns>
          public static MySqlDataReader ExecuteReader(string connectionString, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
          {
               if ((connectionString == null || connectionString.Length == 0))
                    throw new ArgumentNullException("connectionString");
               // Create & open a MySqlConnection
               using (MySqlConnection connection = new MySqlConnection(connectionString))
               {
                    connection.Open();
                    // Call the private overload that takes an internally owned connection in place of the connection string
                    return ExecuteReader(connection, (MySqlTransaction)null, commandType, commandText, commandParameters, MySqlConnectionOwnership.Internal);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the database specified in
          /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  MySqlDataReader dr = ExecuteReader(connString, "GetOrders", 24, 36);
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>A MySqlDataReader containing the resultset generated by the command</returns>
          public static MySqlDataReader ExecuteReader(string connectionString, string spName, params object[] parameterValues)
          {
               if ((connectionString == null || connectionString.Length == 0))
                    throw new ArgumentNullException("connectionString");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               MySqlParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connectionString, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of MySqlParameters
                    return ExecuteReader(connectionString, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteReader(connectionString, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset and takes no parameters) against the provided MySqlConnection.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  MySqlDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <returns>A MySqlDataReader containing the resultset generated by the command</returns>
          public static MySqlDataReader ExecuteReader(MySqlConnection connection, CommandType commandType, string commandText)
          {
               return ExecuteReader(connection, commandType, commandText, (MySqlParameter[])null);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset) against the specified MySqlConnection
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  MySqlDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders", new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          /// <returns>A MySqlDataReader containing the resultset generated by the command</returns>
          public static MySqlDataReader ExecuteReader(MySqlConnection connection, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
          {
               // Pass through the call to private overload using a null transaction value
               return ExecuteReader(connection, (MySqlTransaction)null, commandType, commandText, commandParameters, MySqlConnectionOwnership.External);
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the specified MySqlConnection
          /// using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  MySqlDataReader dr = ExecuteReader(conn, "GetOrders", 24, 36);
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>A MySqlDataReader containing the resultset generated by the command</returns>
          public static MySqlDataReader ExecuteReader(MySqlConnection connection, string spName, params object[] parameterValues)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               MySqlParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connection, spName);
                    AssignParameterValues(commandParameters, parameterValues);
                    return ExecuteReader(connection, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteReader(connection, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset and takes no parameters) against the provided MySqlTransaction.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  MySqlDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <returns>A MySqlDataReader containing the resultset generated by the command</returns>
          public static MySqlDataReader ExecuteReader(MySqlTransaction transaction, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of MySqlParameters
               return ExecuteReader(transaction, commandType, commandText, (MySqlParameter[])null);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset) against the specified MySqlTransaction
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///   MySqlDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders", new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          /// <returns>A MySqlDataReader containing the resultset generated by the command</returns>
          public static MySqlDataReader ExecuteReader(MySqlTransaction transaction, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               // Pass through to private overload, indicating that the connection is owned by the caller
               return ExecuteReader(transaction.Connection, transaction, commandType, commandText, commandParameters, MySqlConnectionOwnership.External);
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the specified
          /// MySqlTransaction using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  MySqlDataReader dr = ExecuteReader(trans, "GetOrders", 24, 36);
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>A MySqlDataReader containing the resultset generated by the command</returns>

          public static MySqlDataReader ExecuteReader(MySqlTransaction transaction, string spName, params object[] parameterValues)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               MySqlParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    commandParameters = MySqlHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    AssignParameterValues(commandParameters, parameterValues);
                    return ExecuteReader(transaction, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteReader(transaction, CommandType.StoredProcedure, spName);
               }
          }

          // ExecuteReader

          #endregion "ExecuteReader"

          #region "ExecuteScalar"

          ///// <summary>
          ///// Execute a MySqlCommand (that returns a 1x1 resultset and takes no parameters) against the database specified in
          ///// the connection string.
          ///// </summary>
          ///// <remarks>
          ///// e.g.:
          /////  int orderCount = (int)ExecuteScalar(connString, CommandType.StoredProcedure, "GetOrderCount");
          ///// </remarks>
          ///// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          ///// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          ///// <param name="commandText">The stored procedure name or T-SQL command</param>
          ///// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          //public static object ExecuteScalar(string connectionString, CommandType commandType, string commandText)
          //{
          //     // Pass through the call providing null for the set of MySqlParameters
          //     return ExecuteScalar(connectionString, commandType, commandText, (MySqlParameter[])null);
          //}

          ///// <summary>
          ///// Execute a MySqlCommand (that returns a 1x1 resultset) against the database specified in the connection string
          ///// using the provided parameters.
          ///// </summary>
          ///// <remarks>
          ///// e.g.:
          /////  int orderCount = (int)ExecuteScalar(connString, CommandType.StoredProcedure, "GetOrderCount", new MySqlParameter("@prodid", 24));
          ///// </remarks>
          ///// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          ///// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          ///// <param name="commandText">The stored procedure name or T-SQL command</param>
          ///// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          ///// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          //public static object ExecuteScalar(string connectionString, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
          //{
          //     if ((connectionString == null || connectionString.Length == 0))
          //          throw new ArgumentNullException("connectionString");
          //     // Create & open a MySqlConnection, and dispose of it after we are done.
          //     using (MySqlConnection connection = new MySqlConnection(connectionString))
          //     {
          //          connection.Open();
          //          // Call the overload that takes a connection in place of the connection string
          //          return ExecuteScalar(connection, commandType, commandText, commandParameters);
          //     }
          //}

          ///// <summary>
          ///// Execute a stored procedure via a MySqlCommand (that returns a 1x1 resultset) against the database specified in
          ///// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the
          ///// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          ///// </summary>
          ///// <remarks>
          ///// This method provides no access to output parameters or the stored procedure's return value parameter.
          /////
          ///// e.g.:
          /////  int orderCount = (int)ExecuteScalar(connString, "GetOrderCount", 24, 36);
          ///// </remarks>
          ///// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          ///// <param name="spName">The name of the stored procedure</param>
          ///// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          ///// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          //public static object ExecuteScalar(string connectionString, string spName, params object[] parameterValues)
          //{
          //     if ((connectionString == null || connectionString.Length == 0))
          //          throw new ArgumentNullException("connectionString");
          //     if ((spName == null || spName.Length == 0))
          //          throw new ArgumentNullException("spName");
          //     MySqlParameter[] commandParameters = null;
          //     // If we receive parameter values, we need to figure out where they go
          //     if ((parameterValues != null) && parameterValues.Length > 0)
          //     {
          //          // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
          //          commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connectionString, spName);
          //          // Assign the provided values to these parameters based on parameter order
          //          AssignParameterValues(commandParameters, parameterValues);
          //          // Call the overload that takes an array of MySqlParameters
          //          return ExecuteScalar(connectionString, CommandType.StoredProcedure, spName, commandParameters);
          //          // Otherwise we can just call the SP without params
          //     }
          //     else
          //     {
          //          return ExecuteScalar(connectionString, CommandType.StoredProcedure, spName);
          //     }
          //}

          /// <summary>
          /// Execute a MySqlCommand (that returns a 1x1 resultset and takes no parameters) against the provided MySqlConnection.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int orderCount = (int)ExecuteScalar(conn, CommandType.StoredProcedure, "GetOrderCount");
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          public static object ExecuteScalar(MySqlConnection connection, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of MySqlParameters
               return ExecuteScalar(connection, commandType, commandText, (MySqlParameter[])null);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a 1x1 resultset) against the specified MySqlConnection
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int orderCount = (int)ExecuteScalar(conn, CommandType.StoredProcedure, "GetOrderCount", new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          public static object ExecuteScalar(MySqlConnection connection, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               // Create a command and prepare it for execution
               MySqlCommand cmd = new MySqlCommand();
               object retval = null;
               bool mustCloseConnection = false;
               PrepareCommand(cmd, connection, (MySqlTransaction)null, commandType, commandText, commandParameters, ref mustCloseConnection);
               // Execute the command & return the results
               retval = cmd.ExecuteScalar();
               // Detach the MySqlParameters from the command object, so they can be used again
               cmd.Parameters.Clear();
               if ((mustCloseConnection))
                    connection.Close();
               return retval;
          }

          /// <summary>
          /// Executes the scalar.
          /// </summary>
          /// <param name="connection">The connection.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <returns></returns>
          public static object ExecuteScalar(MySqlConnection connection, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> poParametros)
          {
               return ExecuteScalar(connection, (MySqlTransaction)null, commandType, commandText, poParametros);
          }

          /// <summary>
          /// Executes the scalar.
          /// </summary>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <returns></returns>
          public static object ExecuteScalar(MySqlTransaction transaction, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> poParametros)
          {
               return ExecuteScalar(transaction.Connection, transaction, commandType, commandText, poParametros);
          }

          /// <summary>
          /// Executes the scalar.
          /// </summary>
          /// <param name="connection">The connection.</param>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <returns></returns>
          private static object ExecuteScalar(MySqlConnection connection, MySqlTransaction transaction, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> poParametros)
          {
               object retval = null;
               // Create a command and prepare it for execution
               using (MySqlCommand cmd = new MySqlCommand(commandText.ToString()))
               {
                    PrepareCommand(cmd, connection, transaction, commandType, poParametros, true);
                    retval = cmd.ExecuteScalar();
                    // Detach the MySqlParameters from the command object, so they can be used again
                    cmd.Parameters.Clear();
                    return retval;
               }
          }

          /// <summary>
          /// Stores the procedure execute scalar.
          /// </summary>
          /// <param name="connection">The connection.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <returns></returns>
          public static object StoreProcedureExecuteScalar(MySqlConnection connection, CommandType commandType, string commandText, IEnumerable<ParameterSql> poParametros)
          {
               return StoreProcedureExecuteScalar(connection, (MySqlTransaction)null, commandType, commandText, poParametros);
          }

          /// <summary>
          /// Stores the procedure execute scalar.
          /// </summary>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <returns></returns>
          public static object StoreProcedureExecuteScalar(MySqlTransaction transaction, CommandType commandType, string commandText, IEnumerable<ParameterSql> poParametros)
          {
               return StoreProcedureExecuteScalar(transaction.Connection, transaction, commandType, commandText, poParametros);
          }

          /// <summary>
          /// Stores the procedure execute scalar.
          /// </summary>
          /// <param name="connection">The connection.</param>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <returns></returns>
          private static object StoreProcedureExecuteScalar(MySqlConnection connection, MySqlTransaction transaction, CommandType commandType, string commandText, IEnumerable<ParameterSql> poParametros)
          {
               object retval = null;
               // Pass through the call providing null for the set of MySqlParameters
               // Create a command and prepare it for execution
               using (MySqlCommand command = new MySqlCommand(commandText))
               {
                    PrepareCommand(command, connection, transaction, commandType, poParametros, true);
                    // Create the DataAdapter & DataSet
                    using (MySqlDataAdapter dataAdapter = new MySqlDataAdapter(command))
                    {
                         // Add the table mappings specified by the user
                         // Fill the DataSet using default values for DataTable names, etc
                         retval = dataAdapter.SelectCommand.ExecuteScalar();
                         // Detach the MySqlParameters from the command object, so they can be used again
                         command.Parameters.Clear();
                         return retval;
                    }
               }
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a 1x1 resultset) against the specified MySqlConnection
          /// using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  int orderCount = (int)ExecuteScalar(conn, "GetOrderCount", 24, 36);
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          public static object ExecuteScalar(MySqlConnection connection, string spName, params object[] parameterValues)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               MySqlParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of MySqlParameters
                    return ExecuteScalar(connection, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteScalar(connection, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a 1x1 resultset and takes no parameters) against the provided MySqlTransaction.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int orderCount = (int)ExecuteScalar(trans, CommandType.StoredProcedure, "GetOrderCount");
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          public static object ExecuteScalar(MySqlTransaction transaction, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of MySqlParameters
               return ExecuteScalar(transaction, commandType, commandText, (MySqlParameter[])null);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a 1x1 resultset) against the specified MySqlTransaction
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int orderCount = (int)ExecuteScalar(trans, CommandType.StoredProcedure, "GetOrderCount", new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          public static object ExecuteScalar(MySqlTransaction transaction, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               // Create a command and prepare it for execution
               MySqlCommand cmd = new MySqlCommand();
               object retval = null;
               bool mustCloseConnection = false;
               PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters, ref mustCloseConnection);
               // Execute the command & return the results
               retval = cmd.ExecuteScalar();
               // Detach the MySqlParameters from the command object, so they can be used again
               cmd.Parameters.Clear();
               return retval;
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a 1x1 resultset) against the specified
          /// MySqlTransaction using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  int orderCount = (int)ExecuteScalar(trans, "GetOrderCount", 24, 36);
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          public static object ExecuteScalar(MySqlTransaction transaction, string spName, params object[] parameterValues)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               MySqlParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = MySqlHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);

                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);

                    // Call the overload that takes an array of MySqlParameters
                    return ExecuteScalar(transaction, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteScalar(transaction, CommandType.StoredProcedure, spName);
               }
          }

          // ExecuteScalar

          #endregion "ExecuteScalar"

          #region "ExecuteXmlReader"

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset and takes no parameters) against the provided MySqlConnection.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  XmlReader r = ExecuteXmlReader(conn, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command using "FOR XML AUTO"</param>
          /// <returns>An XmlReader containing the resultset generated by the command</returns>
          public static XmlReader ExecuteXmlReader(MySqlConnection connection, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of MySqlParameters
               return ExecuteXmlReader(connection, commandType, commandText, (MySqlParameter[])null);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset) against the specified MySqlConnection
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  XmlReader r = ExecuteXmlReader(conn, CommandType.StoredProcedure, "GetOrders", new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command using "FOR XML AUTO"</param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          /// <returns>An XmlReader containing the resultset generated by the command</returns>
          public static XmlReader ExecuteXmlReader(MySqlConnection connection, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
          {
               // Pass through the call using a null transaction value
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               // Create a command and prepare it for execution
               MySqlCommand cmd = new MySqlCommand();
               bool mustCloseConnection = false;
               try
               {
                    XmlReader retval = null;
                    PrepareCommand(cmd, connection, (MySqlTransaction)null, commandType, commandText, commandParameters, ref mustCloseConnection);
                    // Create the DataAdapter & DataSet
                    retval = null;//cmd.ExecuteXmlReader();
                    // Detach the MySqlParameters from the command object, so they can be used again
                    cmd.Parameters.Clear();
                    return retval;
               }
               catch
               {
                    if ((mustCloseConnection))
                         connection.Close();
                    throw;
               }
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the specified MySqlConnection
          /// using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  XmlReader r = ExecuteXmlReader(conn, "GetOrders", 24, 36);
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="spName">The name of the stored procedure using "FOR XML AUTO"</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          public static XmlReader ExecuteXmlReader(MySqlConnection connection, string spName, params object[] parameterValues)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               MySqlParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of MySqlParameters
                    return ExecuteXmlReader(connection, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteXmlReader(connection, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset and takes no parameters) against the provided MySqlTransaction.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  XmlReader r = ExecuteXmlReader(trans, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command using "FOR XML AUTO"</param>
          /// <returns>An XmlReader containing the resultset generated by the command</returns>
          public static XmlReader ExecuteXmlReader(MySqlTransaction transaction, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of MySqlParameters
               return ExecuteXmlReader(transaction, commandType, commandText, (MySqlParameter[])null);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset) against the specified MySqlTransaction
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  XmlReader r = ExecuteXmlReader(trans, CommandType.StoredProcedure, "GetOrders", new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command using "FOR XML AUTO"</param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          /// <returns>An XmlReader containing the resultset generated by the command</returns>
          public static XmlReader ExecuteXmlReader(MySqlTransaction transaction, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
          {
               // Create a command and prepare it for execution
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               MySqlCommand cmd = new MySqlCommand();
               XmlReader retval = null;
               bool mustCloseConnection = false;
               PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters, ref mustCloseConnection);
               // Create the DataAdapter & DataSet
               retval = null;//cmd.ExecuteXmlReader();
               // Detach the MySqlParameters from the command object, so they can be used again
               cmd.Parameters.Clear();
               return retval;
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the specified
          /// MySqlTransaction using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  XmlReader r = ExecuteXmlReader(trans, "GetOrders", 24, 36);
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static XmlReader ExecuteXmlReader(MySqlTransaction transaction, string spName, params object[] parameterValues)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               MySqlParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = MySqlHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of MySqlParameters
                    return ExecuteXmlReader(transaction, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteXmlReader(transaction, CommandType.StoredProcedure, spName);
               }
          }

          // ExecuteXmlReader

          #endregion "ExecuteXmlReader"

          #region "FillDataset"

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset and takes no parameters) against the database specified in
          /// the connection string.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  FillDataset(connString, CommandType.StoredProcedure, "GetOrders", ds, new string[] {"orders"});
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)</param>
          public static void FillDataset(string connectionString, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames)
          {
               if ((connectionString == null || connectionString.Length == 0))
                    throw new ArgumentNullException("connectionString");
               if ((dataSet == null))
                    throw new ArgumentNullException("dataSet");
               // Create & open a MySqlConnection, and dispose of it after we are done
               using (MySqlConnection connection = new MySqlConnection(connectionString))
               {
                    connection.Open();
                    // Call the overload that takes a connection in place of the connection string
                    FillDataset(connection, commandType, commandText, dataSet, tableNames);
               }
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset) against the database specified in the connection string
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  FillDataset(connString, CommandType.StoredProcedure, "GetOrders", ds, new string[] {"orders"}, new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          /// /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          public static void FillDataset(string connectionString, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames, params MySqlParameter[] commandParameters)
          {
               if ((connectionString == null || connectionString.Length == 0))
                    throw new ArgumentNullException("connectionString");
               if ((dataSet == null))
                    throw new ArgumentNullException("dataSet");
               // Create & open a MySqlConnection, and dispose of it after we are done
               using (MySqlConnection connection = new MySqlConnection(connectionString))
               {
                    connection.Open();
                    // Call the overload that takes a connection in place of the connection string
                    FillDataset(connection, commandType, commandText, dataSet, tableNames, commandParameters);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the database specified in
          /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  FillDataset(connString, CommandType.StoredProcedure, "GetOrders", ds, new string[] {"orders"}, 24);
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          public static void FillDataset(string connectionString, string spName, DataSet dataSet, string[] tableNames, params object[] parameterValues)
          {
               if ((connectionString == null || connectionString.Length == 0))
                    throw new ArgumentNullException("connectionString");
               if ((dataSet == null))
                    throw new ArgumentNullException("dataSet");
               // Create & open a MySqlConnection, and dispose of it after we are done
               using (MySqlConnection connection = new MySqlConnection(connectionString))
               {
                    connection.Open();
                    // Call the overload that takes a connection in place of the connection string
                    FillDataset(connection, spName, dataSet, tableNames, parameterValues);
               }
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset and takes no parameters) against the provided MySqlConnection.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  FillDataset(conn, CommandType.StoredProcedure, "GetOrders", ds, new string[] {"orders"});
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          public static void FillDataset(MySqlConnection connection, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames)
          {
               FillDataset(connection, commandType, commandText, dataSet, tableNames, null);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset) against the specified MySqlConnection
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  FillDataset(conn, CommandType.StoredProcedure, "GetOrders", ds, new string[] {"orders"}, new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          public static void FillDataset(MySqlConnection connection, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames, params MySqlParameter[] commandParameters)
          {
               FillDataset(connection, null, commandType, commandText, dataSet, tableNames, commandParameters);
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the specified MySqlConnection
          /// using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  FillDataset(conn, "GetOrders", ds, new string[] {"orders"}, 24, 36);
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          public static void FillDataset(MySqlConnection connection, string spName, DataSet dataSet, string[] tableNames, params object[] parameterValues)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((dataSet == null))
                    throw new ArgumentNullException("dataSet");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of MySqlParameters
                    FillDataset(connection, CommandType.StoredProcedure, spName, dataSet, tableNames, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    FillDataset(connection, CommandType.StoredProcedure, spName, dataSet, tableNames);
               }
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset and takes no parameters) against the provided MySqlTransaction.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  FillDataset(trans, CommandType.StoredProcedure, "GetOrders", ds, new string[] {"orders"});
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          public static void FillDataset(MySqlTransaction transaction, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames)
          {
               FillDataset(transaction, commandType, commandText, dataSet, tableNames, null);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset) against the specified MySqlTransaction
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  FillDataset(trans, CommandType.StoredProcedure, "GetOrders", ds, new string[] {"orders"}, new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          public static void FillDataset(MySqlTransaction transaction, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames, params MySqlParameter[] commandParameters)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               FillDataset(transaction.Connection, transaction, commandType, commandText, dataSet, tableNames, commandParameters);
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the specified
          /// MySqlTransaction using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  FillDataset(trans, "GetOrders", ds, new string[]{"orders"}, 24, 36);
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          public static void FillDataset(MySqlTransaction transaction, string spName, DataSet dataSet, string[] tableNames, params object[] parameterValues)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               if ((dataSet == null))
                    throw new ArgumentNullException("dataSet");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of MySqlParameters
                    FillDataset(transaction, CommandType.StoredProcedure, spName, dataSet, tableNames, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    FillDataset(transaction, CommandType.StoredProcedure, spName, dataSet, tableNames);
               }
          }

          /// <summary>
          /// Private helper method that execute a MySqlCommand (that returns a resultset) against the specified MySqlTransaction and MySqlConnection
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  FillDataset(conn, trans, CommandType.StoredProcedure, "GetOrders", ds, new string[] {"orders"}, new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          private static void FillDataset(MySqlConnection connection, MySqlTransaction transaction, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames, params MySqlParameter[] commandParameters)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((dataSet == null))
                    throw new ArgumentNullException("dataSet");
               // Create a command and prepare it for execution
               MySqlCommand command = new MySqlCommand();
               bool mustCloseConnection = false;
               PrepareCommand(command, connection, transaction, commandType, commandText, commandParameters, ref mustCloseConnection);
               // Create the DataAdapter & DataSet
               using (MySqlDataAdapter dataAdapter = new MySqlDataAdapter(command))
               {
                    // Add the table mappings specified by the user
                    if ((tableNames != null) && tableNames.Length > 0)
                    {
                         string tableName = "Table";
                         int index = 0;
                         for (index = 0; index <= tableNames.Length - 1; index++)
                         {
                              if ((tableNames[index] == null || tableNames[index].Length == 0))
                                   throw new ArgumentException("The tableNames parameter must contain a list of tables, a value was provided as null or empty string.", "tableNames");
                              dataAdapter.TableMappings.Add(tableName, tableNames[index]);
                              tableName = string.Format("{0}{1}", tableName, (index + 1).ToString());
                         }
                    }
                    // Fill the DataSet using default values for DataTable names, etc
                    dataAdapter.Fill(dataSet);
                    // Detach the MySqlParameters from the command object, so they can be used again
                    command.Parameters.Clear();
               }
               if ((mustCloseConnection))
                    connection.Close();
          }

          #endregion "FillDataset"

          #region "UpdateDataset"

          /// <summary>
          /// Executes the respective command for each inserted, updated, or deleted row in the DataSet.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  UpdateDataset(conn, insertCommand, deleteCommand, updateCommand, dataSet, "Order");
          /// </remarks>
          /// <param name="insertCommand">A valid transact-SQL statement or stored procedure to insert new records into the data source</param>
          /// <param name="deleteCommand">A valid transact-SQL statement or stored procedure to delete records from the data source</param>
          /// <param name="updateCommand">A valid transact-SQL statement or stored procedure used to update records in the data source</param>
          /// <param name="dataSet">The DataSet used to update the data source</param>
          /// <param name="tableName">The DataTable used to update the data source.</param>
          public static void UpdateDataset(MySqlCommand insertCommand, MySqlCommand deleteCommand, MySqlCommand updateCommand, DataSet dataSet, string tableName)
          {
               if ((insertCommand == null))
                    throw new ArgumentNullException("insertCommand");
               if ((deleteCommand == null))
                    throw new ArgumentNullException("deleteCommand");
               if ((updateCommand == null))
                    throw new ArgumentNullException("updateCommand");
               if ((dataSet == null))
                    throw new ArgumentNullException("dataSet");
               if ((tableName == null || tableName.Length == 0))
                    throw new ArgumentNullException("tableName");
               // Create a MySqlDataAdapter, and dispose of it after we are done
               using (MySqlDataAdapter dataAdapter = new MySqlDataAdapter())
               {
                    // Set the data adapter commands
                    dataAdapter.UpdateCommand = updateCommand;
                    dataAdapter.InsertCommand = insertCommand;
                    dataAdapter.DeleteCommand = deleteCommand;

                    // Update the dataset changes in the data source
                    dataAdapter.Update(dataSet, tableName);

                    // Commit all the changes made to the DataSet
                    dataSet.AcceptChanges();
               }
          }

          #endregion "UpdateDataset"

          #region "CreateCommand"

          /// <summary>
          /// Simplify the creation of a Sql command object by allowing
          /// a stored procedure and optional parameters to be provided
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  MySqlCommand command = CreateCommand(conn, "AddCustomer", "CustomerID", "CustomerName");
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="sourceColumns">An array of string to be assigned as the source columns of the stored procedure parameters</param>
          /// <returns>A valid MySqlCommand object</returns>
          public static MySqlCommand CreateCommand(MySqlConnection connection, string spName, params string[] sourceColumns)
          {
               if (connection == null)
                    throw new ArgumentNullException("connection");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");

               // Create a MySqlCommand
               MySqlCommand cmd = new MySqlCommand(spName, connection)
               {
                    CommandType = CommandType.StoredProcedure
               };
               // If we receive parameter values, we need to figure out where they go
               if ((sourceColumns != null) && sourceColumns.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Assign the provided source columns to these parameters based on parameter order
                    int index = 0;
                    for (index = 0; index <= sourceColumns.Length - 1; index++)
                    {
                         commandParameters[index].SourceColumn = sourceColumns[index];
                    }
                    // Attach the discovered parameters to the MySqlCommand object
                    AttachParameters(cmd, commandParameters);
               }
               cmd.CommandTimeout = 0;
               //14/Junio/2007 para que dure todo el tiempo la espera por el query
               return cmd;
          }

          #endregion "CreateCommand"

          #region "ExecuteNonQueryTypedParams"

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns no resultset) against the database specified in
          /// the connection string using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on row values.
          /// </summary>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQueryTypedParams(string connectionString, string spName, DataRow dataRow)
          {
               if (connectionString == null || connectionString.Length == 0)
                    throw new ArgumentNullException("connectionString");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");
               // If the row has values, the store procedure parameters must be initialized
               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connectionString, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    return MySqlHelper.ExecuteNonQuery(connectionString, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return MySqlHelper.ExecuteNonQuery(connectionString, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns no resultset) against the specified MySqlConnection
          /// using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on row values.
          /// </summary>
          /// <param name="connection">A valid MySqlConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQueryTypedParams(MySqlConnection connection, string spName, DataRow dataRow)
          {
               if (connection == null)
                    throw new ArgumentNullException("connection");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");
               // If the row has values, the store procedure parameters must be initialized
               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    return MySqlHelper.ExecuteNonQuery(connection, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return MySqlHelper.ExecuteNonQuery(connection, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns no resultset) against the specified
          /// MySqlTransaction using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on row values.
          /// </summary>
          /// <param name="transaction">A valid MySqlTransaction object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQueryTypedParams(MySqlTransaction transaction, string spName, DataRow dataRow)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               // If the row has values, the store procedure parameters must be initialized
               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    return MySqlHelper.ExecuteNonQuery(transaction, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return MySqlHelper.ExecuteNonQuery(transaction, CommandType.StoredProcedure, spName);
               }
          }

          #endregion "ExecuteNonQueryTypedParams"

          #region "ExecuteDatasetTypedParams"

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the database specified in
          /// the connection string using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on row values.
          /// </summary>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDatasetTypedParams(string connectionString, string spName, DataRow dataRow)
          {
               DataSet functionReturnValue = null;
               if (connectionString == null || connectionString.Length == 0)
                    throw new ArgumentNullException("connectionString");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");
               // If the row has values, the store procedure parameters must be initialized
               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connectionString, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    functionReturnValue = MySqlHelper.ExecuteDataset(connectionString, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    functionReturnValue = MySqlHelper.ExecuteDataset(connectionString, CommandType.StoredProcedure, spName);
               }
               return functionReturnValue;
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the specified MySqlConnection
          /// using the dataRow column values as the store procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on row values.
          /// </summary>
          /// <param name="connection">A valid MySqlConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDatasetTypedParams(MySqlConnection connection, string spName, DataRow dataRow)
          {
               DataSet functionReturnValue = null;
               if (connection == null)
                    throw new ArgumentNullException("connection");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");
               // If the row has values, the store procedure parameters must be initialized
               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    functionReturnValue = MySqlHelper.ExecuteDataset(connection, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    functionReturnValue = MySqlHelper.ExecuteDataset(connection, CommandType.StoredProcedure, spName);
               }
               return functionReturnValue;
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the specified MySqlTransaction
          /// using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on row values.
          /// </summary>
          /// <param name="transaction">A valid MySqlTransaction object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter </param>
          public static DataSet ExecuteDatasetTypedParams(MySqlTransaction transaction, string spName, DataRow dataRow)
          {
               if (transaction == null)
                    throw new ArgumentNullException("transaction");
               if (transaction != null && transaction.Connection == null)
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");
               // If the row has values, the store procedure parameters must be initialized
               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    return MySqlHelper.ExecuteDataset(transaction, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return MySqlHelper.ExecuteDataset(transaction, CommandType.StoredProcedure, spName);
               }
          }

          #endregion "ExecuteDatasetTypedParams"

          #region "ExecuteReaderTypedParams"

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the database specified in
          /// the connection string using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>A MySqlDataReader containing the resultset generated by the command</returns>
          public static MySqlDataReader ExecuteReaderTypedParams(string connectionString, string spName, DataRow dataRow)
          {
               if (connectionString == null || connectionString.Length == 0)
                    throw new ArgumentNullException("connectionString");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");
               // If the row has values, the store procedure parameters must be initialized
               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connectionString, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    return MySqlHelper.ExecuteReader(connectionString, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return MySqlHelper.ExecuteReader(connectionString, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the specified MySqlConnection
          /// using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <param name="connection">A valid MySqlConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>A MySqlDataReader containing the resultset generated by the command</returns>
          public static MySqlDataReader ExecuteReaderTypedParams(MySqlConnection connection, string spName, DataRow dataRow)
          {
               if (connection == null)
                    throw new ArgumentNullException("connection");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");
               // If the row has values, the store procedure parameters must be initialized
               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    return MySqlHelper.ExecuteReader(connection, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return MySqlHelper.ExecuteReader(connection, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the specified MySqlTransaction
          /// using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <param name="transaction">A valid MySqlTransaction object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>A MySqlDataReader containing the resultset generated by the command</returns>
          public static MySqlDataReader ExecuteReaderTypedParams(MySqlTransaction transaction, string spName, DataRow dataRow)
          {
               MySqlDataReader functionReturnValue = null;
               if (transaction == null)
                    throw new ArgumentNullException("transaction");
               if (transaction != null && transaction.Connection == null)
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");
               // If the row has values, the store procedure parameters must be initialized
               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    functionReturnValue = MySqlHelper.ExecuteReader(transaction, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    functionReturnValue = MySqlHelper.ExecuteReader(transaction, CommandType.StoredProcedure, spName);
               }
               return functionReturnValue;
          }

          #endregion "ExecuteReaderTypedParams"

          #region "ExecuteScalarTypedParams"

          ///// <summary>
          ///// Execute a stored procedure via a MySqlCommand (that returns a 1x1 resultset) against the database specified in
          ///// the connection string using the dataRow column values as the stored procedure's parameters values.
          ///// This method will query the database to discover the parameters for the
          ///// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          ///// </summary>
          ///// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          ///// <param name="spName">The name of the stored procedure</param>
          ///// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          ///// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          //public static object ExecuteScalarTypedParams(string connectionString, string spName, DataRow dataRow)
          //{
          //     if (connectionString == null || connectionString.Length == 0)
          //          throw new ArgumentNullException("connectionString");
          //     if (spName == null || spName.Length == 0)
          //          throw new ArgumentNullException("spName");
          //     // If the row has values, the store procedure parameters must be initialized
          //     if (((dataRow != null) && dataRow.ItemArray.Length > 0))
          //     {
          //          // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
          //          MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connectionString, spName);
          //          // Set the parameters values
          //          AssignParameterValues(commandParameters, dataRow);
          //          return MySqlHelper.ExecuteScalar(connectionString, CommandType.StoredProcedure, spName, commandParameters);
          //     }
          //     else
          //     {
          //          return MySqlHelper.ExecuteScalar(connectionString, CommandType.StoredProcedure, spName);
          //     }
          //}

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a 1x1 resultset) against the specified MySqlConnection
          /// using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <param name="connection">A valid MySqlConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          public static object ExecuteScalarTypedParams(MySqlConnection connection, string spName, DataRow dataRow)
          {
               if (connection == null)
                    throw new ArgumentNullException("connection");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");
               // If the row has values, the store procedure parameters must be initialized
               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    return MySqlHelper.ExecuteScalar(connection, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return MySqlHelper.ExecuteScalar(connection, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a 1x1 resultset) against the specified MySqlTransaction
          /// using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <param name="transaction">A valid MySqlTransaction object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          public static object ExecuteScalarTypedParams(MySqlTransaction transaction, string spName, DataRow dataRow)
          {
               if (transaction == null)
                    throw new ArgumentNullException("transaction");
               if (transaction != null && transaction.Connection == null)
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");
               // If the row has values, the store procedure parameters must be initialized
               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    return MySqlHelper.ExecuteScalar(transaction, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return MySqlHelper.ExecuteScalar(transaction, CommandType.StoredProcedure, spName);
               }
          }

          #endregion "ExecuteScalarTypedParams"

          #region "ExecuteXmlReaderTypedParams"

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the specified MySqlConnection
          /// using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <param name="connection">A valid MySqlConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>An XmlReader containing the resultset generated by the command</returns>
          public static XmlReader ExecuteXmlReaderTypedParams(MySqlConnection connection, string spName, DataRow dataRow)
          {
               XmlReader functionReturnValue = null;
               if (connection == null)
                    throw new ArgumentNullException("connection");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");

               // If the row has values, the store procedure parameters must be initialized

               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(connection, spName);

                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);

                    functionReturnValue = MySqlHelper.ExecuteXmlReader(connection, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    functionReturnValue = MySqlHelper.ExecuteXmlReader(connection, CommandType.StoredProcedure, spName);
               }
               return functionReturnValue;
          }

          /// <summary>
          /// Execute a stored procedure via a MySqlCommand (that returns a resultset) against the specified MySqlTransaction
          /// using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <param name="transaction">A valid MySqlTransaction object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>An XmlReader containing the resultset generated by the command</returns>
          public static XmlReader ExecuteXmlReaderTypedParams(MySqlTransaction transaction, string spName, DataRow dataRow)
          {
               if (transaction == null)
                    throw new ArgumentNullException("transaction");
               if (transaction != null && transaction.Connection == null)
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");

               // if the row has values, the store procedure parameters must be initialized

               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    MySqlParameter[] commandParameters = MySqlHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);

                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);

                    return MySqlHelper.ExecuteXmlReader(transaction, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return MySqlHelper.ExecuteXmlReader(transaction, CommandType.StoredProcedure, spName);
               }
          }

          #endregion "ExecuteXmlReaderTypedParams"

          #region "ExecuteDataTable"

          ///// <summary>
          /////
          ///// </summary>
          ///// <param name="connectionString"></param>
          ///// <param name="commandType"></param>
          ///// <param name="commandText"></param>
          ///// <returns></returns>
          //public static DataTable ExecuteDataTable(string connectionString, CommandType commandType, string commandText)
          //{
          //     // Pass through the call providing null for the set of MySqlParameters
          //     return ExecuteDataTable(connectionString, commandType, commandText, (MySqlParameter[])null);
          //}

          ///// <summary>
          /////
          ///// </summary>
          ///// <param name="connectionString"></param>
          ///// <param name="commandType"></param>
          ///// <param name="commandText"></param>
          ///// <param name="commandParameters"></param>
          ///// <returns></returns>
          //public static DataTable ExecuteDataTable(string connectionString, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
          //{
          //     if ((connectionString == null || connectionString.Length == 0))
          //          throw new ArgumentNullException("connectionString");
          //     // Create & open a MySqlConnection, and dispose of it after we are done
          //     using (MySqlConnection connection = new MySqlConnection(connectionString))
          //     {
          //          connection.Open();
          //          // Call the overload that takes a connection in place of the connection string
          //          return ExecuteDataTable(connection, commandType, commandText, commandParameters);
          //     }
          //}

          ///// <summary>
          /////
          ///// </summary>
          ///// <param name="connection"></param>
          ///// <param name="commandType"></param>
          ///// <param name="commandText"></param>
          ///// <param name="commandParameters"></param>
          ///// <returns></returns>
          //public static DataTable ExecuteDataTable(MySqlConnection connection, CommandType commandType, string commandText, params MySqlParameter[] commandParameters)
          //{
          //     if ((connection == null))
          //          throw new ArgumentNullException("connection");
          //     // Create a command and prepare it for execution
          //     MySqlCommand cmd = new MySqlCommand();
          //     DataTable dt = new DataTable();
          //     bool mustCloseConnection = false;
          //     PrepareCommand(cmd, connection, (MySqlTransaction)null, commandType, commandText, commandParameters, ref mustCloseConnection);
          //     using (MySqlDataAdapter dataAdatpter = new MySqlDataAdapter(cmd))
          //     {
          //          // Create the DataAdapter & Datatable
          //          // Fill the DataSet using default values for DataTable names, etc
          //          dataAdatpter.Fill(dt);
          //          // Detach the MySqlParameters from the command object, so they can be used again
          //          cmd.Parameters.Clear();
          //     }
          //     if ((mustCloseConnection))
          //          connection.Close();
          //     // Return the datatable
          //     return dt;
          //}

          #endregion "ExecuteDataTable"

          #region "FillDataTable"

          /// <summary>
          /// Fills the data table.
          /// </summary>
          /// <param name="connection">The connection.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="dataTable">The data table.</param>
          /// <param name="tableNames">The table names.</param>
          public static void FillDataTable(MySqlConnection connection, CommandType commandType, string commandText, DataTable dataTable, string[] tableNames)
          {
               FillDataTable(connection, commandType, commandText, dataTable, tableNames, null);
          }

          /// <summary>
          /// Fills the data table.
          /// </summary>
          /// <param name="connection">The connection.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="dataSet">The data set.</param>
          /// <param name="tableNames">The table names.</param>
          /// <param name="commandParameters">The command parameters.</param>
          public static void FillDataTable(MySqlConnection connection, CommandType commandType, string commandText, DataTable dataSet, string[] tableNames, params MySqlParameter[] commandParameters)
          {
               FillDataTable(connection, null, commandType, commandText, dataSet, tableNames, commandParameters);
          }

          /// <summary>
          /// Fills the data table.
          /// </summary>
          /// <param name="connection">The connection.</param>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="dataTable">The data table.</param>
          /// <param name="tableNames">The table names.</param>
          /// <param name="commandParameters">The command parameters.</param>
          /// <exception cref="ArgumentNullException">
          /// connection
          /// or
          /// dataTable
          /// </exception>
          /// <exception cref="ArgumentException">The tableNames parameter must contain a list of tables, a value was provided as null or empty string.;tableNames</exception>
          private static void FillDataTable(MySqlConnection connection, MySqlTransaction transaction, CommandType commandType, string commandText, DataTable dataTable, string[] tableNames, params MySqlParameter[] commandParameters)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((dataTable == null))
                    throw new ArgumentNullException("dataTable");
               // Create a command and prepare it for execution
               MySqlCommand command = new MySqlCommand();
               bool mustCloseConnection = false;
               PrepareCommand(command, connection, transaction, commandType, commandText, commandParameters, ref mustCloseConnection);
               // Create the DataAdapter & DataSet
               using (MySqlDataAdapter dataAdapter = new MySqlDataAdapter(command))
               {
                    // Add the table mappings specified by the user
                    if ((tableNames != null) && tableNames.Length > 0)
                    {
                         string tableName = "Table";
                         int index = 0;

                         for (index = 0; index <= tableNames.Length - 1; index++)
                         {
                              if ((tableNames[index] == null || tableNames[index].Length == 0))
                                   throw new ArgumentException("The tableNames parameter must contain a list of tables, a value was provided as null or empty string.", "tableNames");
                              dataAdapter.TableMappings.Add(tableName, tableNames[index]);
                              tableName = string.Format("{0}{1}", tableName, (index + 1).ToString());
                         }
                    }
                    // Fill the DataSet using default values for DataTable names, etc
                    dataAdapter.Fill(dataTable);
                    // Detach the MySqlParameters from the command object, so they can be used again
                    command.Parameters.Clear();
               }
               if ((mustCloseConnection))
                    connection.Close();
          }

          /// <summary>
          /// Fills the data table.
          /// </summary>
          /// <param name="connection">The connection.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="dataTable">The data table.</param>
          /// <param name="commandParameters">The command parameters.</param>
          public static void FillDataTable(MySqlConnection connection, CommandType commandType, StringBuilder commandText, DataTable dataTable, IEnumerable<ParameterSql> commandParameters)
          {
               FillDataTable(connection, (MySqlTransaction)null, commandType, commandText, dataTable, commandParameters);
          }

          /// <summary>
          /// Fills the data table.
          /// </summary>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="dataTable">The data table.</param>
          /// <param name="commandParameters">The command parameters.</param>
          public static void FillDataTable(MySqlTransaction transaction, CommandType commandType, StringBuilder commandText, DataTable dataTable, IEnumerable<ParameterSql> commandParameters)
          {
               FillDataTable(transaction.Connection, transaction, commandType, commandText, dataTable, commandParameters);
          }

          /// <summary>
          /// Fills the data table.
          /// </summary>
          /// <param name="connection">The connection.</param>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="dataTable">The data table.</param>
          /// <param name="commandParameters">The command parameters.</param>
          private static void FillDataTable(MySqlConnection connection, MySqlTransaction transaction, CommandType commandType, StringBuilder commandText, DataTable dataTable, IEnumerable<ParameterSql> commandParameters)
          {
               // Create a command and prepare it for execution
               using (MySqlCommand command = new MySqlCommand(commandText.ToString()))
               {
                    PrepareCommand(command, connection, transaction, commandType, commandParameters, true);
                    // Create the DataAdapter & DataSet
                    using (MySqlDataAdapter dataAdapter = new MySqlDataAdapter(command))
                    {
                         // Add the table mappings specified by the user
                         // Fill the DataSet using default values for DataTable names, etc
                         dataAdapter.Fill(dataTable);
                         // Detach the MySqlParameters from the command object, so they can be used again
                         command.Parameters.Clear();
                    }
               }
          }

          /// <summary>
          /// Fills the data table.
          /// </summary>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="dataTable">The data table.</param>
          /// <param name="tableNames">The table names.</param>
          public static void FillDataTable(MySqlTransaction transaction, CommandType commandType, string commandText, DataTable dataTable, string[] tableNames)
          {
               FillDataTable(transaction, commandType, commandText, dataTable, tableNames, null);
          }

          /// <summary>
          /// Fills the data table.
          /// </summary>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="dataTable">The data table.</param>
          /// <param name="tableNames">The table names.</param>
          /// <param name="commandParameters">The command parameters.</param>
          /// <exception cref="ArgumentNullException">transaction</exception>
          /// <exception cref="ArgumentException">The transaction was rollbacked or commited, please provide an open transaction. - transaction</exception>
          public static void FillDataTable(MySqlTransaction transaction, CommandType commandType, string commandText, DataTable dataTable, string[] tableNames, params MySqlParameter[] commandParameters)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               FillDataTable(transaction.Connection, transaction, commandType, commandText, dataTable, tableNames, commandParameters);
          }

          #endregion "FillDataTable"

          #region FillDataReader

          /// <summary>
          /// Create and prepare a MySqlCommand, and call ExecuteReader with the appropriate CommandBehavior.
          /// </summary>
          /// <remarks>
          /// If we created and opened the connection, we want the connection to be closed when the DataReader is closed.
          ///
          /// If the caller provided the connection, we want to leave it to them to manage.
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection, on which to execute this command</param>
          /// <param name="transaction">A valid MySqlTransaction, or 'null'</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of MySqlParameters to be associated with the command or 'null' if no parameters are required</param>
          /// <param name="connectionOwnership">Indicates whether the connection parameter was provided by the caller, or created by MySqlHelper</param>
          /// <returns>MySqlDataReader containing the results of the command</returns>
          private static MySqlDataReader FillDataReader(MySqlConnection connection, MySqlTransaction transaction, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> commandParameters, MySqlConnectionOwnership connectionOwnership)
          {
               MySqlDataReader dataReader;
               if ((connection == null))
                    throw new ArgumentNullException("connection");

               // Create a command and prepare it for execution
               using (MySqlCommand command = new MySqlCommand(commandText.ToString()))
               {
                    // Create a reader
                    PrepareCommand(command, connection, transaction, commandType, commandParameters, true);
                    // Call ExecuteReader with the appropriate CommandBehavior
                    if (connectionOwnership == MySqlConnectionOwnership.External)
                         dataReader = command.ExecuteReader();
                    else
                         dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);
                    // Detach the MySqlParameters from the command object, so they can be used again.
                    // HACK: There is a problem here, the output parameter values are fletched
                    // when the reader is closed, so if the parameters are detached from the command
                    // then the SqlReader can´t set its values.
                    // When this happen, the parameters can´t be used again in other command.
                    bool canClear = true;
                    foreach (MySqlParameter commandParameter in command.Parameters)
                    {
                         if (commandParameter.Direction != ParameterDirection.Input)
                              canClear = false;
                    }
                    if ((canClear))
                         command.Parameters.Clear();
                    return dataReader;
               }
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset and takes no parameters) against the provided MySqlConnection.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  MySqlDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <returns>A MySqlDataReader containing the resultset generated by the command</returns>
          public static MySqlDataReader FillDataReader(MySqlConnection connection, CommandType commandType, StringBuilder commandText)
          {
               return FillDataReader(connection, commandType, commandText, null);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset) against the specified MySqlConnection
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  MySqlDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders", new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          /// <returns>A MySqlDataReader containing the resultset generated by the command</returns>
          public static MySqlDataReader FillDataReader(MySqlConnection connection, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> commandParameters)
          {
               // Pass through the call to private overload using a null transaction value
               return FillDataReader(connection, (MySqlTransaction)null, commandType, commandText, commandParameters, MySqlConnectionOwnership.External);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset and takes no parameters) against the provided MySqlTransaction.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  MySqlDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <returns>A MySqlDataReader containing the resultset generated by the command</returns>
          public static MySqlDataReader FillDataReader(MySqlTransaction transaction, CommandType commandType, StringBuilder commandText)
          {
               // Pass through the call providing null for the set of MySqlParameters
               return FillDataReader(transaction, commandType, commandText, null);
          }

          /// <summary>
          /// Execute a MySqlCommand (that returns a resultset) against the specified MySqlTransaction
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///   MySqlDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders", new MySqlParameter("@prodid", 24));
          /// </remarks>
          /// <param name="transaction">A valid MySqlTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of SqlParamters used to execute the command</param>
          /// <returns>A MySqlDataReader containing the resultset generated by the command</returns>
          public static MySqlDataReader FillDataReader(MySqlTransaction transaction, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> commandParameters)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               // Pass through to private overload, indicating that the connection is owned by the caller
               return FillDataReader(transaction.Connection, transaction, commandType, commandText, commandParameters, MySqlConnectionOwnership.External);
          }

          #endregion FillDataReader
     }
}