using Framework.DataBase.Utilities;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace Framework.DataBase
{
     /// <summary>
     /// The OracleHelper class is intended to encapsulate high performance, scalable best practices for  common uses of OracleClient.
     /// </summary>
     public sealed class OracleHelper
     {
          #region "private utility methods & constructors"

          /// <summary>
          /// Since this class provides only static methods, make the default constructor private to prevent instances from being created with "new OracleHelper()".
          /// </summary>
          private OracleHelper()
          {
          }

          /// <summary>
          /// This method is used to attach array of OracleParameters to a OracleCommand.
          /// This method will assign a value of DbNull to any parameter with a direction of
          /// InputOutput and a value of null.
          /// This behavior will prevent default values from being used, but
          /// this will be the less common case than an intended pure output parameter (derived as InputOutput)
          /// where the user provided no input value.
          /// </summary>
          /// <param name="command">The command to which the parameters will be added</param>
          /// <param name="commandParameters">an array of OracleParameters to be added to command</param>
          private static void AttachParameters(OracleCommand command, OracleParameter[] commandParameters)
          {
               if ((command == null))
                    throw new ArgumentNullException("command");
               if (((commandParameters != null)))
               {
                    OracleParameter p = null;
                    foreach (OracleParameter p_loopVariable in commandParameters)
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
          private static void AttachParameters(OracleCommand command, IEnumerable<ParameterSql> commandParameters)
          {
               if (commandParameters == null)
                    return;
               HashSet<string> parameter = new HashSet<string>();
               foreach (ParameterSql loParametro in commandParameters)
               {
                    if (parameter.Contains(loParametro.Parameter))
                         continue;
                    if (loParametro.Value == null)
                         // Esto se da para evitar consultas con parametros con valores nulos, asi se rempleza el null (lenguaje)  por el dbnull del motor
                         command.Parameters.Add(loParametro.Parameter, DBNull.Value);
                    else
                         command.Parameters.Add(loParametro.Parameter, loParametro.Value);
                    parameter.Add(loParametro.Parameter);
               }

          }

          /// <summary>
          /// This method assigns dataRow column values to an array of OracleParameters
          /// </summary>
          /// <param name="commandParameters">Array of OracleParameters to be assigned values</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values</param>
          private static void AssignParameterValues(OracleParameter[] commandParameters, DataRow dataRow)
          {
               if (commandParameters == null || dataRow == null)
               {
                    // Do nothing if we get no data
                    return;
               }

               // Set the parameters values
               OracleParameter commandParameter = null;
               int i = 0;
               foreach (OracleParameter commandParameter_loopVariable in commandParameters)
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
          /// This method assigns an array of values to an array of OracleParameters
          /// </summary>
          /// <param name="commandParameters">Array of OracleParameters to be assigned values</param>
          /// <param name="parameterValues">Array of objects holding the values to be assigned</param>
          private static void AssignParameterValues(OracleParameter[] commandParameters, object[] parameterValues)
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
          /// <param name="command">The OracleCommand to be prepared</param>
          /// <param name="connection">A valid OracleConnection, on which to execute this command</param>
          /// <param name="transaction">A valid OracleTransaction, or 'null'</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParameters to be associated with the command or 'null' if no parameters are required</param>
          /// <param name="mustCloseConnection"><c>true</c> if the connection was opened by the method, otherwose is false.</param>
          private static void PrepareCommand(OracleCommand command, OracleConnection connection, OracleTransaction transaction, CommandType commandType, string commandText, OracleParameter[] commandParameters, ref bool mustCloseConnection)
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

               // Set the command text (stored procedure name or Oracle statement)
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
          private static void PrepareCommand(OracleCommand command, OracleConnection connection, OracleTransaction transaction, CommandType commandType, IEnumerable<ParameterSql> commandParameters, bool pbIlimitado)
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
          /// Execute a OracleCommand (that returns no resultset and takes no parameters) against the database specified in
          /// the connection string
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders");
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of OracleParameters
               return ExecuteNonQuery(connectionString, commandType, commandText, (OracleParameter[])null);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns no resultset) against the database specified in the connection string
          /// using the provided parameters
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders", new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText, params OracleParameter[] commandParameters)
          {
               if ((connectionString == null || connectionString.Length == 0))
                    throw new ArgumentNullException("connectionString");
               // Create & open a OracleConnection, and dispose of it after we are done
               using (OracleConnection connection = new OracleConnection(connectionString))
               {
                    connection.Open();
                    // Call the overload that takes a connection in place of the connection string
                    return ExecuteNonQuery(connection, commandType, commandText, commandParameters);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns no resultset) against the database specified in
          /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  int result = ExecuteNonQuery(connString, "PublishOrders", 24, 36);
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a OracleConnection</param>
          /// <param name="spName">The name of the stored prcedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQuery(string connectionString, string spName, params object[] parameterValues)
          {
               if ((connectionString == null || connectionString.Length == 0))
                    throw new ArgumentNullException("connectionString");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               OracleParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = OracleHelperParameterCache.GetSpParameterSet(connectionString, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of OracleParameters
                    return ExecuteNonQuery(connectionString, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteNonQuery(connectionString, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a OracleCommand (that returns no resultset and takes no parameters) against the provided OracleConnection.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int result = ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders");
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQuery(OracleConnection connection, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of OracleParameters
               return ExecuteNonQuery(connection, commandType, commandText, (OracleParameter[])null);
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
          public static int ExecuteNonQuery(OracleConnection connection, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> poParametros, bool pbIlimitado)
          {
               return ExecuteNonQuery(connection, (OracleTransaction)null, commandType, commandText, poParametros, pbIlimitado);
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
          public static int ExecuteNonQuery(OracleTransaction transaction, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> poParametros, bool pbIlimitado)
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
          private static int ExecuteNonQuery(OracleConnection connection, OracleTransaction transaction, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> poParametros, bool pbIlimitado)
          {
               int retval = 0;
               // Pass through the call providing null for the set of OracleParameters
               using (OracleCommand loComando = new OracleCommand(commandText.ToString()))
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
          public static int StoreProcedureExecuteNonQuery(OracleConnection connection, CommandType commandType, string commandText, IEnumerable<ParameterSql> poParametros)
          {
               return StoreProcedureExecuteNonQuery(connection, (OracleTransaction)null, commandType, commandText, poParametros);
          }

          /// <summary>
          /// Stores the procedure execute non query.
          /// </summary>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <returns></returns>
          public static int StoreProcedureExecuteNonQuery(OracleTransaction transaction, CommandType commandType, string commandText, IEnumerable<ParameterSql> poParametros)
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
          private static int StoreProcedureExecuteNonQuery(OracleConnection connection, OracleTransaction transaction, CommandType commandType, string commandText, IEnumerable<ParameterSql> poParametros)
          {
               int retval = 0;
               // Pass through the call providing null for the set of OracleParameters
               // Create a command and prepare it for execution
               using (OracleCommand command = new OracleCommand(commandText))
               {
                    PrepareCommand(command, connection, transaction, commandType, poParametros, true);
                    // Create the DataAdapter & DataSet
                    using (OracleDataAdapter dataAdapter = new OracleDataAdapter(command))
                    {
                         // Add the table mappings specified by the user
                         // Fill the DataSet using default values for DataTable names, etc
                         retval = dataAdapter.SelectCommand.ExecuteNonQuery();
                         // Detach the OracleParameters from the command object, so they can be used again
                         command.Parameters.Clear();
                         return retval;
                    }
               }
          }

          /// <summary>
          /// Execute a OracleCommand (that returns no resultset) against the specified OracleConnection
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int result = ExecuteNonQuery(conn, CommandType.StoredProcedure, "PublishOrders", new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQuery(OracleConnection connection, CommandType commandType, string commandText, params OracleParameter[] commandParameters)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               // Create a command and prepare it for execution
               OracleCommand cmd = new OracleCommand();
               int retval = 0;
               bool mustCloseConnection = false;
               PrepareCommand(cmd, connection, (OracleTransaction)null, commandType, commandText, commandParameters, ref mustCloseConnection);
               // Finally, execute the command
               retval = cmd.ExecuteNonQuery();
               // Detach the OracleParameters from the command object, so they can be used again
               cmd.Parameters.Clear();
               if ((mustCloseConnection))
                    connection.Close();
               return retval;
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns no resultset) against the specified OracleConnection
          /// using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  int result = ExecuteNonQuery(conn, "PublishOrders", 24, 36);
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQuery(OracleConnection connection, string spName, params object[] parameterValues)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               OracleParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = OracleHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of OracleParameters
                    return ExecuteNonQuery(connection, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteNonQuery(connection, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a OracleCommand (that returns no resultset and takes no parameters) against the provided OracleTransaction.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int result = ExecuteNonQuery(trans, CommandType.StoredProcedure, "PublishOrders");
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <returns>An int representing the number of rows affected by the command</returns>

          public static int ExecuteNonQuery(OracleTransaction transaction, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of OracleParameters
               return ExecuteNonQuery(transaction, commandType, commandText, (OracleParameter[])null);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns no resultset) against the specified OracleTransaction
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int result = ExecuteNonQuery(trans, CommandType.StoredProcedure, "GetOrders", new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQuery(OracleTransaction transaction, CommandType commandType, string commandText, params OracleParameter[] commandParameters)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               // Create a command and prepare it for execution
               OracleCommand cmd = new OracleCommand();
               int retval = 0;
               bool mustCloseConnection = false;
               PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters, ref mustCloseConnection);
               // Finally, execute the command
               retval = cmd.ExecuteNonQuery();
               // Detach the OracleParameters from the command object, so they can be used again
               cmd.Parameters.Clear();
               return retval;
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns no resultset) against the specified
          /// OracleTransaction using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  int result = ExecuteNonQuery(conn, trans, "PublishOrders", 24, 36);
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQuery(OracleTransaction transaction, string spName, params object[] parameterValues)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               OracleParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = OracleHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of OracleParameters
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
          /// Execute a OracleCommand (that returns a resultset and takes no parameters) against the database specified in
          /// the connection string.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(connString, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>

          public static DataSet ExecuteDataset(string connectionString, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of OracleParameters
               return ExecuteDataset(connectionString, commandType, commandText, (OracleParameter[])null);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset) against the database specified in the connection string
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(connString, CommandType.StoredProcedure, "GetOrders", new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDataset(string connectionString, CommandType commandType, string commandText, params OracleParameter[] commandParameters)
          {
               if ((connectionString == null || connectionString.Length == 0))
               {
                    throw new ArgumentNullException("connectionString");
               }
               // Create & open a OracleConnection, and dispose of it after we are done
               using (OracleConnection connection = new OracleConnection(connectionString))
               {
                    connection.Open();
                    // Call the overload that takes a connection in place of the connection string
                    return ExecuteDataset(connection, commandType, commandText, commandParameters);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the database specified in
          /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(connString, "GetOrders", 24, 36);
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a OracleConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDataset(string connectionString, string spName, params object[] parameterValues)
          {
               if ((connectionString == null || connectionString.Length == 0))
                    throw new ArgumentNullException("connectionString");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               OracleParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = OracleHelperParameterCache.GetSpParameterSet(connectionString, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of OracleParameters
                    return ExecuteDataset(connectionString, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteDataset(connectionString, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset and takes no parameters) against the provided OracleConnection.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(conn, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>

          public static DataSet ExecuteDataset(OracleConnection connection, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of OracleParameters
               return ExecuteDataset(connection, commandType, commandText, (OracleParameter[])null);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset) against the specified OracleConnection
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(conn, CommandType.StoredProcedure, "GetOrders", new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDataset(OracleConnection connection, CommandType commandType, string commandText, params OracleParameter[] commandParameters)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               // Create a command and prepare it for execution
               OracleCommand cmd = new OracleCommand();
               DataSet ds = new DataSet();
               bool mustCloseConnection = false;
               PrepareCommand(cmd, connection, (OracleTransaction)null, commandType, commandText, commandParameters, ref mustCloseConnection);
               using (OracleDataAdapter dataAdatpter = new OracleDataAdapter(cmd))
               {
                    // Fill the DataSet using default values for DataTable names, etc
                    dataAdatpter.Fill(ds);
                    // Detach the OracleParameters from the command object, so they can be used again
                    cmd.Parameters.Clear();
                    if ((mustCloseConnection))
                         connection.Close();
                    // Return the dataset
                    return ds;
               }
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the specified OracleConnection
          /// using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(conn, "GetOrders", 24, 36);
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDataset(OracleConnection connection, string spName, params object[] parameterValues)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               OracleParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = OracleHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);

                    // Call the overload that takes an array of OracleParameters
                    return ExecuteDataset(connection, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteDataset(connection, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset and takes no parameters) against the provided OracleTransaction.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(trans, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDataset(OracleTransaction transaction, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of OracleParameters
               return ExecuteDataset(transaction, commandType, commandText, (OracleParameter[])null);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset) against the specified OracleTransaction
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(trans, CommandType.StoredProcedure, "GetOrders", new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDataset(OracleTransaction transaction, CommandType commandType, string commandText, params OracleParameter[] commandParameters)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               // Create a command and prepare it for execution
               OracleCommand cmd = new OracleCommand();
               DataSet ds = new DataSet();
               bool mustCloseConnection = false;
               PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters, ref mustCloseConnection);
               using (OracleDataAdapter dataAdatpter = new OracleDataAdapter(cmd))
               {
                    // Fill the DataSet using default values for DataTable names, etc
                    dataAdatpter.Fill(ds);
                    // Detach the OracleParameters from the command object, so they can be used again
                    cmd.Parameters.Clear();
                    // Return the dataset
                    return ds;
               }
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the specified
          /// OracleTransaction using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  DataSet ds = ExecuteDataset(trans, "GetOrders", 24, 36);
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDataset(OracleTransaction transaction, string spName, params object[] parameterValues)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               OracleParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = OracleHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of OracleParameters
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
          /// This enum is used to indicate whether the connection was provided by the caller, or created by OracleHelper, so that
          /// we can set the appropriate CommandBehavior when calling ExecuteReader()
          /// </summary>

          private enum OracleConnectionOwnership
          {
               /// <summary>Connection is owned and managed by OracleHelper</summary>
               Internal,

               /// <summary>Connection is owned and managed by the caller</summary>
               External
          }

          /// <summary>
          /// Create and prepare a OracleCommand, and call ExecuteReader with the appropriate CommandBehavior.
          /// </summary>
          /// <remarks>
          /// If we created and opened the connection, we want the connection to be closed when the DataReader is closed.
          ///
          /// If the caller provided the connection, we want to leave it to them to manage.
          /// </remarks>
          /// <param name="connection">A valid OracleConnection, on which to execute this command</param>
          /// <param name="transaction">A valid OracleTransaction, or 'null'</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParameters to be associated with the command or 'null' if no parameters are required</param>
          /// <param name="connectionOwnership">Indicates whether the connection parameter was provided by the caller, or created by OracleHelper</param>
          /// <returns>OracleDataReader containing the results of the command</returns>
          private static OracleDataReader ExecuteReader(OracleConnection connection, OracleTransaction transaction, CommandType commandType, string commandText, OracleParameter[] commandParameters, OracleConnectionOwnership connectionOwnership)
          {
               OracleDataReader dataReader;
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               bool mustCloseConnection = false;
               // Create a command and prepare it for execution
               OracleCommand cmd = new OracleCommand();
               try
               {
                    // Create a reader
                    PrepareCommand(cmd, connection, transaction, commandType, commandText, commandParameters, ref mustCloseConnection);
                    // Call ExecuteReader with the appropriate CommandBehavior
                    if (connectionOwnership == OracleConnectionOwnership.External)
                         dataReader = cmd.ExecuteReader();
                    else
                         dataReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                    // Detach the OracleParameters from the command object, so they can be used again.
                    // HACK: There is a problem here, the output parameter values are fletched
                    // when the reader is closed, so if the parameters are detached from the command
                    // then the OracleReader can´t set its values.
                    // When this happen, the parameters can´t be used again in other command.
                    bool canClear = true;
                    foreach (OracleParameter commandParameter in cmd.Parameters)
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
          /// Execute a OracleCommand (that returns a resultset and takes no parameters) against the database specified in
          /// the connection string.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  OracleDataReader dr = ExecuteReader(connString, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <returns>A OracleDataReader containing the resultset generated by the command</returns>
          public static OracleDataReader ExecuteReader(string connectionString, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of OracleParameters
               return ExecuteReader(connectionString, commandType, commandText, (OracleParameter[])null);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset) against the database specified in the connection string
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  OracleDataReader dr = ExecuteReader(connString, CommandType.StoredProcedure, "GetOrders", new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          /// <returns>A OracleDataReader containing the resultset generated by the command</returns>
          public static OracleDataReader ExecuteReader(string connectionString, CommandType commandType, string commandText, params OracleParameter[] commandParameters)
          {
               if ((connectionString == null || connectionString.Length == 0))
                    throw new ArgumentNullException("connectionString");
               // Create & open a OracleConnection
               using (OracleConnection connection = new OracleConnection(connectionString))
               {
                    connection.Open();
                    // Call the private overload that takes an internally owned connection in place of the connection string
                    return ExecuteReader(connection, (OracleTransaction)null, commandType, commandText, commandParameters, OracleConnectionOwnership.Internal);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the database specified in
          /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  OracleDataReader dr = ExecuteReader(connString, "GetOrders", 24, 36);
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a OracleConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>A OracleDataReader containing the resultset generated by the command</returns>
          public static OracleDataReader ExecuteReader(string connectionString, string spName, params object[] parameterValues)
          {
               if ((connectionString == null || connectionString.Length == 0))
                    throw new ArgumentNullException("connectionString");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               OracleParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = OracleHelperParameterCache.GetSpParameterSet(connectionString, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of OracleParameters
                    return ExecuteReader(connectionString, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteReader(connectionString, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset and takes no parameters) against the provided OracleConnection.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  OracleDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <returns>A OracleDataReader containing the resultset generated by the command</returns>
          public static OracleDataReader ExecuteReader(OracleConnection connection, CommandType commandType, string commandText)
          {
               return ExecuteReader(connection, commandType, commandText, (OracleParameter[])null);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset) against the specified OracleConnection
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  OracleDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders", new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          /// <returns>A OracleDataReader containing the resultset generated by the command</returns>
          public static OracleDataReader ExecuteReader(OracleConnection connection, CommandType commandType, string commandText, params OracleParameter[] commandParameters)
          {
               // Pass through the call to private overload using a null transaction value
               return ExecuteReader(connection, (OracleTransaction)null, commandType, commandText, commandParameters, OracleConnectionOwnership.External);
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the specified OracleConnection
          /// using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  OracleDataReader dr = ExecuteReader(conn, "GetOrders", 24, 36);
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>A OracleDataReader containing the resultset generated by the command</returns>
          public static OracleDataReader ExecuteReader(OracleConnection connection, string spName, params object[] parameterValues)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               OracleParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    commandParameters = OracleHelperParameterCache.GetSpParameterSet(connection, spName);
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
          /// Execute a OracleCommand (that returns a resultset and takes no parameters) against the provided OracleTransaction.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  OracleDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <returns>A OracleDataReader containing the resultset generated by the command</returns>
          public static OracleDataReader ExecuteReader(OracleTransaction transaction, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of OracleParameters
               return ExecuteReader(transaction, commandType, commandText, (OracleParameter[])null);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset) against the specified OracleTransaction
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///   OracleDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders", new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          /// <returns>A OracleDataReader containing the resultset generated by the command</returns>
          public static OracleDataReader ExecuteReader(OracleTransaction transaction, CommandType commandType, string commandText, params OracleParameter[] commandParameters)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               // Pass through to private overload, indicating that the connection is owned by the caller
               return ExecuteReader(transaction.Connection, transaction, commandType, commandText, commandParameters, OracleConnectionOwnership.External);
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the specified
          /// OracleTransaction using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  OracleDataReader dr = ExecuteReader(trans, "GetOrders", 24, 36);
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>A OracleDataReader containing the resultset generated by the command</returns>

          public static OracleDataReader ExecuteReader(OracleTransaction transaction, string spName, params object[] parameterValues)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               OracleParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    commandParameters = OracleHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
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
          ///// Execute a OracleCommand (that returns a 1x1 resultset and takes no parameters) against the database specified in
          ///// the connection string.
          ///// </summary>
          ///// <remarks>
          ///// e.g.:
          /////  int orderCount = (int)ExecuteScalar(connString, CommandType.StoredProcedure, "GetOrderCount");
          ///// </remarks>
          ///// <param name="connectionString">A valid connection string for a OracleConnection</param>
          ///// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          ///// <param name="commandText">The stored procedure name or T-Oracle command</param>
          ///// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          //public static object ExecuteScalar(string connectionString, CommandType commandType, string commandText)
          //{
          //     // Pass through the call providing null for the set of OracleParameters
          //     return ExecuteScalar(connectionString, commandType, commandText, (OracleParameter[])null);
          //}

          ///// <summary>
          ///// Execute a OracleCommand (that returns a 1x1 resultset) against the database specified in the connection string
          ///// using the provided parameters.
          ///// </summary>
          ///// <remarks>
          ///// e.g.:
          /////  int orderCount = (int)ExecuteScalar(connString, CommandType.StoredProcedure, "GetOrderCount", new OracleParameter("@prodid", 24));
          ///// </remarks>
          ///// <param name="connectionString">A valid connection string for a OracleConnection</param>
          ///// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          ///// <param name="commandText">The stored procedure name or T-Oracle command</param>
          ///// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          ///// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          //public static object ExecuteScalar(string connectionString, CommandType commandType, string commandText, params OracleParameter[] commandParameters)
          //{
          //     if ((connectionString == null || connectionString.Length == 0))
          //          throw new ArgumentNullException("connectionString");
          //     // Create & open a OracleConnection, and dispose of it after we are done.
          //     using (OracleConnection connection = new OracleConnection(connectionString))
          //     {
          //          connection.Open();
          //          // Call the overload that takes a connection in place of the connection string
          //          return ExecuteScalar(connection, commandType, commandText, commandParameters);
          //     }
          //}

          ///// <summary>
          ///// Execute a stored procedure via a OracleCommand (that returns a 1x1 resultset) against the database specified in
          ///// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the
          ///// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          ///// </summary>
          ///// <remarks>
          ///// This method provides no access to output parameters or the stored procedure's return value parameter.
          /////
          ///// e.g.:
          /////  int orderCount = (int)ExecuteScalar(connString, "GetOrderCount", 24, 36);
          ///// </remarks>
          ///// <param name="connectionString">A valid connection string for a OracleConnection</param>
          ///// <param name="spName">The name of the stored procedure</param>
          ///// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          ///// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          //public static object ExecuteScalar(string connectionString, string spName, params object[] parameterValues)
          //{
          //     if ((connectionString == null || connectionString.Length == 0))
          //          throw new ArgumentNullException("connectionString");
          //     if ((spName == null || spName.Length == 0))
          //          throw new ArgumentNullException("spName");
          //     OracleParameter[] commandParameters = null;
          //     // If we receive parameter values, we need to figure out where they go
          //     if ((parameterValues != null) && parameterValues.Length > 0)
          //     {
          //          // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
          //          commandParameters = OracleHelperParameterCache.GetSpParameterSet(connectionString, spName);
          //          // Assign the provided values to these parameters based on parameter order
          //          AssignParameterValues(commandParameters, parameterValues);
          //          // Call the overload that takes an array of OracleParameters
          //          return ExecuteScalar(connectionString, CommandType.StoredProcedure, spName, commandParameters);
          //          // Otherwise we can just call the SP without params
          //     }
          //     else
          //     {
          //          return ExecuteScalar(connectionString, CommandType.StoredProcedure, spName);
          //     }
          //}

          /// <summary>
          /// Execute a OracleCommand (that returns a 1x1 resultset and takes no parameters) against the provided OracleConnection.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int orderCount = (int)ExecuteScalar(conn, CommandType.StoredProcedure, "GetOrderCount");
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          public static object ExecuteScalar(OracleConnection connection, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of OracleParameters
               return ExecuteScalar(connection, commandType, commandText, (OracleParameter[])null);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a 1x1 resultset) against the specified OracleConnection
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int orderCount = (int)ExecuteScalar(conn, CommandType.StoredProcedure, "GetOrderCount", new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          public static object ExecuteScalar(OracleConnection connection, CommandType commandType, string commandText, params OracleParameter[] commandParameters)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               // Create a command and prepare it for execution
               OracleCommand cmd = new OracleCommand();
               object retval = null;
               bool mustCloseConnection = false;
               PrepareCommand(cmd, connection, (OracleTransaction)null, commandType, commandText, commandParameters, ref mustCloseConnection);
               // Execute the command & return the results
               retval = cmd.ExecuteScalar();
               // Detach the OracleParameters from the command object, so they can be used again
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
          public static object ExecuteScalar(OracleConnection connection, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> poParametros)
          {
               return ExecuteScalar(connection, (OracleTransaction)null, commandType, commandText, poParametros);
          }

          /// <summary>
          /// Executes the scalar.
          /// </summary>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <returns></returns>
          public static object ExecuteScalar(OracleTransaction transaction, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> poParametros)
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
          private static object ExecuteScalar(OracleConnection connection, OracleTransaction transaction, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> poParametros)
          {
               object retval = null;
               // Create a command and prepare it for execution
               using (OracleCommand cmd = new OracleCommand(commandText.ToString()))
               {
                    PrepareCommand(cmd, connection, transaction, commandType, poParametros, true);
                    retval = cmd.ExecuteScalar();
                    // Detach the OracleParameters from the command object, so they can be used again
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
          public static object StoreProcedureExecuteScalar(OracleConnection connection, CommandType commandType, string commandText, IEnumerable<ParameterSql> poParametros)
          {
               return StoreProcedureExecuteScalar(connection, (OracleTransaction)null, commandType, commandText, poParametros);
          }

          /// <summary>
          /// Stores the procedure execute scalar.
          /// </summary>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <returns></returns>
          public static object StoreProcedureExecuteScalar(OracleTransaction transaction, CommandType commandType, string commandText, IEnumerable<ParameterSql> poParametros)
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
          private static object StoreProcedureExecuteScalar(OracleConnection connection, OracleTransaction transaction, CommandType commandType, string commandText, IEnumerable<ParameterSql> poParametros)
          {
               object retval = null;
               // Pass through the call providing null for the set of OracleParameters
               // Create a command and prepare it for execution
               using (OracleCommand command = new OracleCommand(commandText))
               {
                    PrepareCommand(command, connection, transaction, commandType, poParametros, true);
                    // Create the DataAdapter & DataSet
                    using (OracleDataAdapter dataAdapter = new OracleDataAdapter(command))
                    {
                         // Add the table mappings specified by the user
                         // Fill the DataSet using default values for DataTable names, etc
                         retval = dataAdapter.SelectCommand.ExecuteScalar();
                         // Detach the OracleParameters from the command object, so they can be used again
                         command.Parameters.Clear();
                         return retval;
                    }
               }
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a 1x1 resultset) against the specified OracleConnection
          /// using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  int orderCount = (int)ExecuteScalar(conn, "GetOrderCount", 24, 36);
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          public static object ExecuteScalar(OracleConnection connection, string spName, params object[] parameterValues)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               OracleParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = OracleHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of OracleParameters
                    return ExecuteScalar(connection, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteScalar(connection, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a 1x1 resultset and takes no parameters) against the provided OracleTransaction.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int orderCount = (int)ExecuteScalar(trans, CommandType.StoredProcedure, "GetOrderCount");
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          public static object ExecuteScalar(OracleTransaction transaction, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of OracleParameters
               return ExecuteScalar(transaction, commandType, commandText, (OracleParameter[])null);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a 1x1 resultset) against the specified OracleTransaction
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  int orderCount = (int)ExecuteScalar(trans, CommandType.StoredProcedure, "GetOrderCount", new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          public static object ExecuteScalar(OracleTransaction transaction, CommandType commandType, string commandText, params OracleParameter[] commandParameters)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               // Create a command and prepare it for execution
               OracleCommand cmd = new OracleCommand();
               object retval = null;
               bool mustCloseConnection = false;
               PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters, ref mustCloseConnection);
               // Execute the command & return the results
               retval = cmd.ExecuteScalar();
               // Detach the OracleParameters from the command object, so they can be used again
               cmd.Parameters.Clear();
               return retval;
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a 1x1 resultset) against the specified
          /// OracleTransaction using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  int orderCount = (int)ExecuteScalar(trans, "GetOrderCount", 24, 36);
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          public static object ExecuteScalar(OracleTransaction transaction, string spName, params object[] parameterValues)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               OracleParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = OracleHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);

                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);

                    // Call the overload that takes an array of OracleParameters
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
          /// Execute a OracleCommand (that returns a resultset and takes no parameters) against the provided OracleConnection.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  XmlReader r = ExecuteXmlReader(conn, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command using "FOR XML AUTO"</param>
          /// <returns>An XmlReader containing the resultset generated by the command</returns>
          public static XmlReader ExecuteXmlReader(OracleConnection connection, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of OracleParameters
               return ExecuteXmlReader(connection, commandType, commandText, (OracleParameter[])null);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset) against the specified OracleConnection
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  XmlReader r = ExecuteXmlReader(conn, CommandType.StoredProcedure, "GetOrders", new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command using "FOR XML AUTO"</param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          /// <returns>An XmlReader containing the resultset generated by the command</returns>
          public static XmlReader ExecuteXmlReader(OracleConnection connection, CommandType commandType, string commandText, params OracleParameter[] commandParameters)
          {
               // Pass through the call using a null transaction value
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               // Create a command and prepare it for execution
               OracleCommand cmd = new OracleCommand();
               bool mustCloseConnection = false;
               try
               {
                    XmlReader retval = null;
                    PrepareCommand(cmd, connection, (OracleTransaction)null, commandType, commandText, commandParameters, ref mustCloseConnection);
                    // Create the DataAdapter & DataSet
                    retval = cmd.ExecuteXmlReader();
                    // Detach the OracleParameters from the command object, so they can be used again
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
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the specified OracleConnection
          /// using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  XmlReader r = ExecuteXmlReader(conn, "GetOrders", 24, 36);
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="spName">The name of the stored procedure using "FOR XML AUTO"</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          public static XmlReader ExecuteXmlReader(OracleConnection connection, string spName, params object[] parameterValues)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               OracleParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = OracleHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of OracleParameters
                    return ExecuteXmlReader(connection, CommandType.StoredProcedure, spName, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    return ExecuteXmlReader(connection, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset and takes no parameters) against the provided OracleTransaction.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  XmlReader r = ExecuteXmlReader(trans, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command using "FOR XML AUTO"</param>
          /// <returns>An XmlReader containing the resultset generated by the command</returns>
          public static XmlReader ExecuteXmlReader(OracleTransaction transaction, CommandType commandType, string commandText)
          {
               // Pass through the call providing null for the set of OracleParameters
               return ExecuteXmlReader(transaction, commandType, commandText, (OracleParameter[])null);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset) against the specified OracleTransaction
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  XmlReader r = ExecuteXmlReader(trans, CommandType.StoredProcedure, "GetOrders", new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command using "FOR XML AUTO"</param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          /// <returns>An XmlReader containing the resultset generated by the command</returns>
          public static XmlReader ExecuteXmlReader(OracleTransaction transaction, CommandType commandType, string commandText, params OracleParameter[] commandParameters)
          {
               // Create a command and prepare it for execution
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               OracleCommand cmd = new OracleCommand();
               XmlReader retval = null;
               bool mustCloseConnection = false;
               PrepareCommand(cmd, transaction.Connection, transaction, commandType, commandText, commandParameters, ref mustCloseConnection);
               // Create the DataAdapter & DataSet
               retval = cmd.ExecuteXmlReader();
               // Detach the OracleParameters from the command object, so they can be used again
               cmd.Parameters.Clear();
               return retval;
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the specified
          /// OracleTransaction using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  XmlReader r = ExecuteXmlReader(trans, "GetOrders", 24, 36);
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static XmlReader ExecuteXmlReader(OracleTransaction transaction, string spName, params object[] parameterValues)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               OracleParameter[] commandParameters = null;
               // If we receive parameter values, we need to figure out where they go
               if ((parameterValues != null) && parameterValues.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    commandParameters = OracleHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of OracleParameters
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
          /// Execute a OracleCommand (that returns a resultset and takes no parameters) against the database specified in
          /// the connection string.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  FillDataset(connString, CommandType.StoredProcedure, "GetOrders", ds, new string[] {"orders"});
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)</param>
          public static void FillDataset(string connectionString, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames)
          {
               if ((connectionString == null || connectionString.Length == 0))
                    throw new ArgumentNullException("connectionString");
               if ((dataSet == null))
                    throw new ArgumentNullException("dataSet");
               // Create & open a OracleConnection, and dispose of it after we are done
               using (OracleConnection connection = new OracleConnection(connectionString))
               {
                    connection.Open();
                    // Call the overload that takes a connection in place of the connection string
                    FillDataset(connection, commandType, commandText, dataSet, tableNames);
               }
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset) against the database specified in the connection string
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  FillDataset(connString, CommandType.StoredProcedure, "GetOrders", ds, new string[] {"orders"}, new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          /// /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          public static void FillDataset(string connectionString, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames, params OracleParameter[] commandParameters)
          {
               if ((connectionString == null || connectionString.Length == 0))
                    throw new ArgumentNullException("connectionString");
               if ((dataSet == null))
                    throw new ArgumentNullException("dataSet");
               // Create & open a OracleConnection, and dispose of it after we are done
               using (OracleConnection connection = new OracleConnection(connectionString))
               {
                    connection.Open();
                    // Call the overload that takes a connection in place of the connection string
                    FillDataset(connection, commandType, commandText, dataSet, tableNames, commandParameters);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the database specified in
          /// the connection string using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  FillDataset(connString, CommandType.StoredProcedure, "GetOrders", ds, new string[] {"orders"}, 24);
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a OracleConnection</param>
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
               // Create & open a OracleConnection, and dispose of it after we are done
               using (OracleConnection connection = new OracleConnection(connectionString))
               {
                    connection.Open();
                    // Call the overload that takes a connection in place of the connection string
                    FillDataset(connection, spName, dataSet, tableNames, parameterValues);
               }
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset and takes no parameters) against the provided OracleConnection.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  FillDataset(conn, CommandType.StoredProcedure, "GetOrders", ds, new string[] {"orders"});
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          public static void FillDataset(OracleConnection connection, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames)
          {
               FillDataset(connection, commandType, commandText, dataSet, tableNames, null);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset) against the specified OracleConnection
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  FillDataset(conn, CommandType.StoredProcedure, "GetOrders", ds, new string[] {"orders"}, new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          public static void FillDataset(OracleConnection connection, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames, params OracleParameter[] commandParameters)
          {
               FillDataset(connection, null, commandType, commandText, dataSet, tableNames, commandParameters);
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the specified OracleConnection
          /// using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  FillDataset(conn, "GetOrders", ds, new string[] {"orders"}, 24, 36);
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          public static void FillDataset(OracleConnection connection, string spName, DataSet dataSet, string[] tableNames, params object[] parameterValues)
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
                    OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of OracleParameters
                    FillDataset(connection, CommandType.StoredProcedure, spName, dataSet, tableNames, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    FillDataset(connection, CommandType.StoredProcedure, spName, dataSet, tableNames);
               }
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset and takes no parameters) against the provided OracleTransaction.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  FillDataset(trans, CommandType.StoredProcedure, "GetOrders", ds, new string[] {"orders"});
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          public static void FillDataset(OracleTransaction transaction, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames)
          {
               FillDataset(transaction, commandType, commandText, dataSet, tableNames, null);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset) against the specified OracleTransaction
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  FillDataset(trans, CommandType.StoredProcedure, "GetOrders", ds, new string[] {"orders"}, new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          public static void FillDataset(OracleTransaction transaction, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames, params OracleParameter[] commandParameters)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               FillDataset(transaction.Connection, transaction, commandType, commandText, dataSet, tableNames, commandParameters);
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the specified
          /// OracleTransaction using the provided parameter values.  This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <remarks>
          /// This method provides no access to output parameters or the stored procedure's return value parameter.
          ///
          /// e.g.:
          ///  FillDataset(trans, "GetOrders", ds, new string[]{"orders"}, 24, 36);
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          /// <param name="parameterValues">An array of objects to be assigned as the input values of the stored procedure</param>
          public static void FillDataset(OracleTransaction transaction, string spName, DataSet dataSet, string[] tableNames, params object[] parameterValues)
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
                    OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    // Assign the provided values to these parameters based on parameter order
                    AssignParameterValues(commandParameters, parameterValues);
                    // Call the overload that takes an array of OracleParameters
                    FillDataset(transaction, CommandType.StoredProcedure, spName, dataSet, tableNames, commandParameters);
                    // Otherwise we can just call the SP without params
               }
               else
               {
                    FillDataset(transaction, CommandType.StoredProcedure, spName, dataSet, tableNames);
               }
          }

          /// <summary>
          /// Private helper method that execute a OracleCommand (that returns a resultset) against the specified OracleTransaction and OracleConnection
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  FillDataset(conn, trans, CommandType.StoredProcedure, "GetOrders", ds, new string[] {"orders"}, new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="dataSet">A dataset wich will contain the resultset generated by the command</param>
          /// <param name="tableNames">This array will be used to create table mappings allowing the DataTables to be referenced
          /// by a user defined name (probably the actual table name)
          /// </param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          private static void FillDataset(OracleConnection connection, OracleTransaction transaction, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames, params OracleParameter[] commandParameters)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((dataSet == null))
                    throw new ArgumentNullException("dataSet");
               // Create a command and prepare it for execution
               OracleCommand command = new OracleCommand();
               bool mustCloseConnection = false;
               PrepareCommand(command, connection, transaction, commandType, commandText, commandParameters, ref mustCloseConnection);
               // Create the DataAdapter & DataSet
               using (OracleDataAdapter dataAdapter = new OracleDataAdapter(command))
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
                    // Detach the OracleParameters from the command object, so they can be used again
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
          /// <param name="insertCommand">A valid transact-Oracle statement or stored procedure to insert new records into the data source</param>
          /// <param name="deleteCommand">A valid transact-Oracle statement or stored procedure to delete records from the data source</param>
          /// <param name="updateCommand">A valid transact-Oracle statement or stored procedure used to update records in the data source</param>
          /// <param name="dataSet">The DataSet used to update the data source</param>
          /// <param name="tableName">The DataTable used to update the data source.</param>
          public static void UpdateDataset(OracleCommand insertCommand, OracleCommand deleteCommand, OracleCommand updateCommand, DataSet dataSet, string tableName)
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
               // Create a OracleDataAdapter, and dispose of it after we are done
               using (OracleDataAdapter dataAdapter = new OracleDataAdapter())
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
          /// Simplify the creation of a Oracle command object by allowing
          /// a stored procedure and optional parameters to be provided
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  OracleCommand command = CreateCommand(conn, "AddCustomer", "CustomerID", "CustomerName");
          /// </remarks>
          /// <param name="connection">A valid OracleConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="sourceColumns">An array of string to be assigned as the source columns of the stored procedure parameters</param>
          /// <returns>A valid OracleCommand object</returns>
          public static OracleCommand CreateCommand(OracleConnection connection, string spName, params string[] sourceColumns)
          {
               if (connection == null)
                    throw new ArgumentNullException("connection");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");

               // Create a OracleCommand
               OracleCommand cmd = new OracleCommand(spName, connection)
               {
                    CommandType = CommandType.StoredProcedure
               };
               // If we receive parameter values, we need to figure out where they go
               if ((sourceColumns != null) && sourceColumns.Length > 0)
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Assign the provided source columns to these parameters based on parameter order
                    int index = 0;
                    for (index = 0; index <= sourceColumns.Length - 1; index++)
                    {
                         commandParameters[index].SourceColumn = sourceColumns[index];
                    }
                    // Attach the discovered parameters to the OracleCommand object
                    AttachParameters(cmd, commandParameters);
               }
               cmd.CommandTimeout = 0;
               //14/Junio/2007 para que dure todo el tiempo la espera por el query
               return cmd;
          }

          #endregion "CreateCommand"

          #region "ExecuteNonQueryTypedParams"

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns no resultset) against the database specified in
          /// the connection string using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on row values.
          /// </summary>
          /// <param name="connectionString">A valid connection string for a OracleConnection</param>
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
                    OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(connectionString, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    return OracleHelper.ExecuteNonQuery(connectionString, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return OracleHelper.ExecuteNonQuery(connectionString, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns no resultset) against the specified OracleConnection
          /// using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on row values.
          /// </summary>
          /// <param name="connection">A valid OracleConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQueryTypedParams(OracleConnection connection, string spName, DataRow dataRow)
          {
               if (connection == null)
                    throw new ArgumentNullException("connection");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");
               // If the row has values, the store procedure parameters must be initialized
               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    return OracleHelper.ExecuteNonQuery(connection, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return OracleHelper.ExecuteNonQuery(connection, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns no resultset) against the specified
          /// OracleTransaction using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on row values.
          /// </summary>
          /// <param name="transaction">A valid OracleTransaction object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>An int representing the number of rows affected by the command</returns>
          public static int ExecuteNonQueryTypedParams(OracleTransaction transaction, string spName, DataRow dataRow)
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
                    OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    return OracleHelper.ExecuteNonQuery(transaction, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return OracleHelper.ExecuteNonQuery(transaction, CommandType.StoredProcedure, spName);
               }
          }

          #endregion "ExecuteNonQueryTypedParams"

          #region "ExecuteDatasetTypedParams"

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the database specified in
          /// the connection string using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on row values.
          /// </summary>
          /// <param name="connectionString">A valid connection string for a OracleConnection</param>
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
                    OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(connectionString, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    functionReturnValue = OracleHelper.ExecuteDataset(connectionString, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    functionReturnValue = OracleHelper.ExecuteDataset(connectionString, CommandType.StoredProcedure, spName);
               }
               return functionReturnValue;
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the specified OracleConnection
          /// using the dataRow column values as the store procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on row values.
          /// </summary>
          /// <param name="connection">A valid OracleConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>A dataset containing the resultset generated by the command</returns>
          public static DataSet ExecuteDatasetTypedParams(OracleConnection connection, string spName, DataRow dataRow)
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
                    OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    functionReturnValue = OracleHelper.ExecuteDataset(connection, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    functionReturnValue = OracleHelper.ExecuteDataset(connection, CommandType.StoredProcedure, spName);
               }
               return functionReturnValue;
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the specified OracleTransaction
          /// using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on row values.
          /// </summary>
          /// <param name="transaction">A valid OracleTransaction object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter </param>
          public static DataSet ExecuteDatasetTypedParams(OracleTransaction transaction, string spName, DataRow dataRow)
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
                    OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    return OracleHelper.ExecuteDataset(transaction, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return OracleHelper.ExecuteDataset(transaction, CommandType.StoredProcedure, spName);
               }
          }

          #endregion "ExecuteDatasetTypedParams"

          #region "ExecuteReaderTypedParams"

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the database specified in
          /// the connection string using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <param name="connectionString">A valid connection string for a OracleConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>A OracleDataReader containing the resultset generated by the command</returns>
          public static OracleDataReader ExecuteReaderTypedParams(string connectionString, string spName, DataRow dataRow)
          {
               if (connectionString == null || connectionString.Length == 0)
                    throw new ArgumentNullException("connectionString");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");
               // If the row has values, the store procedure parameters must be initialized
               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(connectionString, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    return OracleHelper.ExecuteReader(connectionString, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return OracleHelper.ExecuteReader(connectionString, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the specified OracleConnection
          /// using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <param name="connection">A valid OracleConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>A OracleDataReader containing the resultset generated by the command</returns>
          public static OracleDataReader ExecuteReaderTypedParams(OracleConnection connection, string spName, DataRow dataRow)
          {
               if (connection == null)
                    throw new ArgumentNullException("connection");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");
               // If the row has values, the store procedure parameters must be initialized
               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    return OracleHelper.ExecuteReader(connection, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return OracleHelper.ExecuteReader(connection, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the specified OracleTransaction
          /// using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <param name="transaction">A valid OracleTransaction object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>A OracleDataReader containing the resultset generated by the command</returns>
          public static OracleDataReader ExecuteReaderTypedParams(OracleTransaction transaction, string spName, DataRow dataRow)
          {
               OracleDataReader functionReturnValue = null;
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
                    OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    functionReturnValue = OracleHelper.ExecuteReader(transaction, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    functionReturnValue = OracleHelper.ExecuteReader(transaction, CommandType.StoredProcedure, spName);
               }
               return functionReturnValue;
          }

          #endregion "ExecuteReaderTypedParams"

          #region "ExecuteScalarTypedParams"

          ///// <summary>
          ///// Execute a stored procedure via a OracleCommand (that returns a 1x1 resultset) against the database specified in
          ///// the connection string using the dataRow column values as the stored procedure's parameters values.
          ///// This method will query the database to discover the parameters for the
          ///// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          ///// </summary>
          ///// <param name="connectionString">A valid connection string for a OracleConnection</param>
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
          //          OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(connectionString, spName);
          //          // Set the parameters values
          //          AssignParameterValues(commandParameters, dataRow);
          //          return OracleHelper.ExecuteScalar(connectionString, CommandType.StoredProcedure, spName, commandParameters);
          //     }
          //     else
          //     {
          //          return OracleHelper.ExecuteScalar(connectionString, CommandType.StoredProcedure, spName);
          //     }
          //}

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a 1x1 resultset) against the specified OracleConnection
          /// using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <param name="connection">A valid OracleConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          public static object ExecuteScalarTypedParams(OracleConnection connection, string spName, DataRow dataRow)
          {
               if (connection == null)
                    throw new ArgumentNullException("connection");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");
               // If the row has values, the store procedure parameters must be initialized
               if (((dataRow != null) && dataRow.ItemArray.Length > 0))
               {
                    // Pull the parameters for this stored procedure from the parameter cache (or discover them & populate the cache)
                    OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(connection, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    return OracleHelper.ExecuteScalar(connection, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return OracleHelper.ExecuteScalar(connection, CommandType.StoredProcedure, spName);
               }
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a 1x1 resultset) against the specified OracleTransaction
          /// using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <param name="transaction">A valid OracleTransaction object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>An object containing the value in the 1x1 resultset generated by the command</returns>
          public static object ExecuteScalarTypedParams(OracleTransaction transaction, string spName, DataRow dataRow)
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
                    OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);
                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);
                    return OracleHelper.ExecuteScalar(transaction, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return OracleHelper.ExecuteScalar(transaction, CommandType.StoredProcedure, spName);
               }
          }

          #endregion "ExecuteScalarTypedParams"

          #region "ExecuteXmlReaderTypedParams"

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the specified OracleConnection
          /// using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <param name="connection">A valid OracleConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>An XmlReader containing the resultset generated by the command</returns>
          public static XmlReader ExecuteXmlReaderTypedParams(OracleConnection connection, string spName, DataRow dataRow)
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
                    OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(connection, spName);

                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);

                    functionReturnValue = OracleHelper.ExecuteXmlReader(connection, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    functionReturnValue = OracleHelper.ExecuteXmlReader(connection, CommandType.StoredProcedure, spName);
               }
               return functionReturnValue;
          }

          /// <summary>
          /// Execute a stored procedure via a OracleCommand (that returns a resultset) against the specified OracleTransaction
          /// using the dataRow column values as the stored procedure's parameters values.
          /// This method will query the database to discover the parameters for the
          /// stored procedure (the first time each stored procedure is called), and assign the values based on parameter order.
          /// </summary>
          /// <param name="transaction">A valid OracleTransaction object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="dataRow">The dataRow used to hold the stored procedure's parameter values.</param>
          /// <returns>An XmlReader containing the resultset generated by the command</returns>
          public static XmlReader ExecuteXmlReaderTypedParams(OracleTransaction transaction, string spName, DataRow dataRow)
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
                    OracleParameter[] commandParameters = OracleHelperParameterCache.GetSpParameterSet(transaction.Connection, spName);

                    // Set the parameters values
                    AssignParameterValues(commandParameters, dataRow);

                    return OracleHelper.ExecuteXmlReader(transaction, CommandType.StoredProcedure, spName, commandParameters);
               }
               else
               {
                    return OracleHelper.ExecuteXmlReader(transaction, CommandType.StoredProcedure, spName);
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
          //     // Pass through the call providing null for the set of OracleParameters
          //     return ExecuteDataTable(connectionString, commandType, commandText, (OracleParameter[])null);
          //}

          ///// <summary>
          /////
          ///// </summary>
          ///// <param name="connectionString"></param>
          ///// <param name="commandType"></param>
          ///// <param name="commandText"></param>
          ///// <param name="commandParameters"></param>
          ///// <returns></returns>
          //public static DataTable ExecuteDataTable(string connectionString, CommandType commandType, string commandText, params OracleParameter[] commandParameters)
          //{
          //     if ((connectionString == null || connectionString.Length == 0))
          //          throw new ArgumentNullException("connectionString");
          //     // Create & open a OracleConnection, and dispose of it after we are done
          //     using (OracleConnection connection = new OracleConnection(connectionString))
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
          //public static DataTable ExecuteDataTable(OracleConnection connection, CommandType commandType, string commandText, params OracleParameter[] commandParameters)
          //{
          //     if ((connection == null))
          //          throw new ArgumentNullException("connection");
          //     // Create a command and prepare it for execution
          //     OracleCommand cmd = new OracleCommand();
          //     DataTable dt = new DataTable();
          //     bool mustCloseConnection = false;
          //     PrepareCommand(cmd, connection, (OracleTransaction)null, commandType, commandText, commandParameters, ref mustCloseConnection);
          //     using (OracleDataAdapter dataAdatpter = new OracleDataAdapter(cmd))
          //     {
          //          // Create the DataAdapter & Datatable
          //          // Fill the DataSet using default values for DataTable names, etc
          //          dataAdatpter.Fill(dt);
          //          // Detach the OracleParameters from the command object, so they can be used again
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
          public static void FillDataTable(OracleConnection connection, CommandType commandType, string commandText, DataTable dataTable, string[] tableNames)
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
          public static void FillDataTable(OracleConnection connection, CommandType commandType, string commandText, DataTable dataSet, string[] tableNames, params OracleParameter[] commandParameters)
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
          private static void FillDataTable(OracleConnection connection, OracleTransaction transaction, CommandType commandType, string commandText, DataTable dataTable, string[] tableNames, params OracleParameter[] commandParameters)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((dataTable == null))
                    throw new ArgumentNullException("dataTable");
               // Create a command and prepare it for execution
               OracleCommand command = new OracleCommand();
               bool mustCloseConnection = false;
               PrepareCommand(command, connection, transaction, commandType, commandText, commandParameters, ref mustCloseConnection);
               // Create the DataAdapter & DataSet
               using (OracleDataAdapter dataAdapter = new OracleDataAdapter(command))
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
                    // Detach the OracleParameters from the command object, so they can be used again
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
          public static void FillDataTable(OracleConnection connection, CommandType commandType, StringBuilder commandText, DataTable dataTable, IEnumerable<ParameterSql> commandParameters)
          {
               FillDataTable(connection, (OracleTransaction)null, commandType, commandText, dataTable, commandParameters);
          }

          /// <summary>
          /// Fills the data table.
          /// </summary>
          /// <param name="transaction">The transaction.</param>
          /// <param name="commandType">Type of the command.</param>
          /// <param name="commandText">The command text.</param>
          /// <param name="dataTable">The data table.</param>
          /// <param name="commandParameters">The command parameters.</param>
          public static void FillDataTable(OracleTransaction transaction, CommandType commandType, StringBuilder commandText, DataTable dataTable, IEnumerable<ParameterSql> commandParameters)
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
          private static void FillDataTable(OracleConnection connection, OracleTransaction transaction, CommandType commandType, StringBuilder commandText, DataTable dataTable, IEnumerable<ParameterSql> commandParameters)
          {
               // Create a command and prepare it for execution
               using (OracleCommand command = new OracleCommand(commandText.ToString()))
               {
                    PrepareCommand(command, connection, transaction, commandType, commandParameters, true);
                    // Create the DataAdapter & DataSet
                    using (OracleDataAdapter dataAdapter = new OracleDataAdapter(command))
                    {
                         // Add the table mappings specified by the user
                         // Fill the DataSet using default values for DataTable names, etc
                         dataAdapter.Fill(dataTable);
                         // Detach the OracleParameters from the command object, so they can be used again
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
          public static void FillDataTable(OracleTransaction transaction, CommandType commandType, string commandText, DataTable dataTable, string[] tableNames)
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
          public static void FillDataTable(OracleTransaction transaction, CommandType commandType, string commandText, DataTable dataTable, string[] tableNames, params OracleParameter[] commandParameters)
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
          /// Create and prepare a OracleCommand, and call ExecuteReader with the appropriate CommandBehavior.
          /// </summary>
          /// <remarks>
          /// If we created and opened the connection, we want the connection to be closed when the DataReader is closed.
          ///
          /// If the caller provided the connection, we want to leave it to them to manage.
          /// </remarks>
          /// <param name="connection">A valid OracleConnection, on which to execute this command</param>
          /// <param name="transaction">A valid OracleTransaction, or 'null'</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParameters to be associated with the command or 'null' if no parameters are required</param>
          /// <param name="connectionOwnership">Indicates whether the connection parameter was provided by the caller, or created by OracleHelper</param>
          /// <returns>OracleDataReader containing the results of the command</returns>
          private static OracleDataReader FillDataReader(OracleConnection connection, OracleTransaction transaction, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> commandParameters, OracleConnectionOwnership connectionOwnership)
          {
               OracleDataReader dataReader;
               if ((connection == null))
                    throw new ArgumentNullException("connection");

               // Create a command and prepare it for execution
               using (OracleCommand command = new OracleCommand(commandText.ToString()))
               {
                    // Create a reader
                    PrepareCommand(command, connection, transaction, commandType, commandParameters, true);
                    // Call ExecuteReader with the appropriate CommandBehavior
                    if (connectionOwnership == OracleConnectionOwnership.External)
                         dataReader = command.ExecuteReader();
                    else
                         dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);
                    // Detach the OracleParameters from the command object, so they can be used again.
                    // HACK: There is a problem here, the output parameter values are fletched
                    // when the reader is closed, so if the parameters are detached from the command
                    // then the OracleReader can´t set its values.
                    // When this happen, the parameters can´t be used again in other command.
                    bool canClear = true;
                    foreach (OracleParameter commandParameter in command.Parameters)
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
          /// Execute a OracleCommand (that returns a resultset and takes no parameters) against the provided OracleConnection.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  OracleDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <returns>A OracleDataReader containing the resultset generated by the command</returns>
          public static OracleDataReader FillDataReader(OracleConnection connection, CommandType commandType, StringBuilder commandText)
          {
               return FillDataReader(connection, commandType, commandText, null);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset) against the specified OracleConnection
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  OracleDataReader dr = ExecuteReader(conn, CommandType.StoredProcedure, "GetOrders", new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="connection">A valid OracleConnection</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          /// <returns>A OracleDataReader containing the resultset generated by the command</returns>
          public static OracleDataReader FillDataReader(OracleConnection connection, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> commandParameters)
          {
               // Pass through the call to private overload using a null transaction value
               return FillDataReader(connection, (OracleTransaction)null, commandType, commandText, commandParameters, OracleConnectionOwnership.External);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset and takes no parameters) against the provided OracleTransaction.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///  OracleDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders");
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <returns>A OracleDataReader containing the resultset generated by the command</returns>
          public static OracleDataReader FillDataReader(OracleTransaction transaction, CommandType commandType, StringBuilder commandText)
          {
               // Pass through the call providing null for the set of OracleParameters
               return FillDataReader(transaction, commandType, commandText, null);
          }

          /// <summary>
          /// Execute a OracleCommand (that returns a resultset) against the specified OracleTransaction
          /// using the provided parameters.
          /// </summary>
          /// <remarks>
          /// e.g.:
          ///   OracleDataReader dr = ExecuteReader(trans, CommandType.StoredProcedure, "GetOrders", new OracleParameter("@prodid", 24));
          /// </remarks>
          /// <param name="transaction">A valid OracleTransaction</param>
          /// <param name="commandType">The CommandType (stored procedure, text, etc.)</param>
          /// <param name="commandText">The stored procedure name or T-Oracle command</param>
          /// <param name="commandParameters">An array of OracleParamters used to execute the command</param>
          /// <returns>A OracleDataReader containing the resultset generated by the command</returns>
          public static OracleDataReader FillDataReader(OracleTransaction transaction, CommandType commandType, StringBuilder commandText, IEnumerable<ParameterSql> commandParameters)
          {
               if ((transaction == null))
                    throw new ArgumentNullException("transaction");
               if ((transaction != null) && (transaction.Connection == null))
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
               // Pass through to private overload, indicating that the connection is owned by the caller
               return FillDataReader(transaction.Connection, transaction, commandType, commandText, commandParameters, OracleConnectionOwnership.External);
          }

          #endregion FillDataReader
     }
}
