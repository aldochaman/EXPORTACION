using MySql.Data.MySqlClient;
using System;
using System.Collections;
using System.Data;

namespace Framework.DataBase
{
     /// <summary>
     /// SqlHelperParameterCache provides functions to leverage a static cache of procedure parameters, and the ability to discover parameters for stored procedures at run-time.
     /// </summary>
     public sealed class MySqlHelperParameterCache
     {
          #region "private methods, variables, and constructors"

          /// <summary>
          /// Since this class provides only static methods, make the default constructor private to prevent
          /// instances from being created with "new MySqlHelperParameterCache()".
          /// </summary>
          private MySqlHelperParameterCache()
          {
          }

          /// <summary>
          /// The parameter cache
          /// </summary>
          private static Hashtable paramCache = Hashtable.Synchronized(new Hashtable());

          /// <summary>
          /// Resolve at run time the appropriate set of MySqlParameters for a stored procedure
          /// </summary>
          /// <param name="connection">A valid MySqlConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="includeReturnValueParameter">Whether or not to include their return value parameter</param>
          /// <param name="parameterValues">The parameter values.</param>
          /// <returns>The parameter array discovered.</returns>
          /// <exception cref="ArgumentNullException">
          /// connection
          /// or
          /// spName
          /// </exception>
          private static MySqlParameter[] DiscoverSpParameterSet(MySqlConnection connection, string spName, bool includeReturnValueParameter, params object[] parameterValues)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               MySqlCommand cmd = new MySqlCommand(spName, connection);
               cmd.CommandType = CommandType.StoredProcedure;
               MySqlParameter[] discoveredParameters = null;
               connection.Open();
               MySqlCommandBuilder.DeriveParameters(cmd);
               connection.Close();
               if (!includeReturnValueParameter)
               {
                    cmd.Parameters.RemoveAt(0);
               }

               discoveredParameters = new MySqlParameter[cmd.Parameters.Count];
               cmd.Parameters.CopyTo(discoveredParameters, 0);

               // Init the parameters with a DBNull value
               MySqlParameter discoveredParameter = null;
               foreach (MySqlParameter discoveredParameter_loopVariable in discoveredParameters)
               {
                    discoveredParameter = discoveredParameter_loopVariable;
                    discoveredParameter.Value = DBNull.Value;
               }

               return discoveredParameters;
          }

          /// <summary>
          /// Deep copy of cached MySqlParameter array
          /// </summary>
          /// <param name="originalParameters"></param>
          /// <returns></returns>
          private static MySqlParameter[] CloneParameters(MySqlParameter[] originalParameters)
          {
               int i = 0;
               int j = originalParameters.Length - 1;
               MySqlParameter[] clonedParameters = new MySqlParameter[j + 1];

               for (i = 0; i <= j; i++)
               {
                    clonedParameters[i] = (MySqlParameter)((ICloneable)originalParameters[i]).Clone();
               }

               return clonedParameters;
          }

          // CloneParameters

          #endregion "private methods, variables, and constructors"

          #region "caching functions"

          /// <summary>
          /// Add parameter array to the cache
          /// </summary>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of SqlParamters to be cached</param>
          public static void CacheParameterSet(string connectionString, string commandText, params MySqlParameter[] commandParameters)
          {
               if (connectionString == null || connectionString.Length == 0)
                    throw new ArgumentNullException("connectionString");
               if (commandText == null || commandText.Length == 0)
                    throw new ArgumentNullException("commandText");
               string hashKey = string.Format("{0}:{1}", connectionString, commandText);
               paramCache[hashKey] = commandParameters;
          }

          /// <summary>
          /// Retrieve a parameter array from the cache
          /// </summary>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <returns>An array of SqlParamters</returns>
          public static MySqlParameter[] GetCachedParameterSet(string connectionString, string commandText)
          {
               if (connectionString == null || connectionString.Length == 0)
                    throw new ArgumentNullException("connectionString");
               if (commandText == null || commandText.Length == 0)
                    throw new ArgumentNullException("commandText");

               string hashKey = string.Format("{0}:{1}", connectionString, commandText);
               MySqlParameter[] cachedParameters = (MySqlParameter[])paramCache[hashKey];

               if (cachedParameters == null)
               {
                    return null;
               }
               else
               {
                    return CloneParameters(cachedParameters);
               }
          }

          #endregion "caching functions"

          #region "Parameter Discovery Functions"

          /// <summary>
          /// Retrieves the set of MySqlParameters appropriate for the stored procedure
          /// </summary>
          /// <remarks>
          /// This method will query the database for this information, and then store it in a cache for future requests.
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <returns>An array of MySqlParameters</returns>
          public static MySqlParameter[] GetSpParameterSet(string connectionString, string spName)
          {
               return GetSpParameterSet(connectionString, spName, false);
          }

          /// <summary>
          /// Retrieves the set of MySqlParameters appropriate for the stored procedure
          /// </summary>
          /// <remarks>
          /// This method will query the database for this information, and then store it in a cache for future requests.
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a MySqlConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="includeReturnValueParameter">A bool value indicating whether the return value parameter should be included in the results</param>
          /// <returns>An array of MySqlParameters</returns>
          public static MySqlParameter[] GetSpParameterSet(string connectionString, string spName, bool includeReturnValueParameter)
          {
               //MySqlParameter[] functionReturnValue = null;
               if ((connectionString == null || connectionString.Length == 0))
               {
                    throw new ArgumentNullException(nameof(connectionString));
               }
               using (MySqlConnection connection = new MySqlConnection(connectionString))
               {
                    return GetSpParameterSetInternal(connection, spName, includeReturnValueParameter);
               }
          }

          /// <summary>
          /// Retrieves the set of MySqlParameters appropriate for the stored procedure
          /// </summary>
          /// <remarks>
          /// This method will query the database for this information, and then store it in a cache for future requests.
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <returns>An array of MySqlParameters</returns>

          public static MySqlParameter[] GetSpParameterSet(MySqlConnection connection, string spName)
          {
               return GetSpParameterSet(connection, spName, false);
          }

          /// <summary>
          /// Retrieves the set of MySqlParameters appropriate for the stored procedure
          /// </summary>
          /// <remarks>
          /// This method will query the database for this information, and then store it in a cache for future requests.
          /// </remarks>
          /// <param name="connection">A valid MySqlConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="includeReturnValueParameter">A bool value indicating whether the return value parameter should be included in the results</param>
          /// <returns>An array of MySqlParameters</returns>
          public static MySqlParameter[] GetSpParameterSet(MySqlConnection connection, string spName, bool includeReturnValueParameter)
          {
               //MySqlParameter[] functionReturnValue = null;
               if ((connection == null))
                    throw new ArgumentNullException(nameof(connection));
               using (MySqlConnection clonedConnection = (MySqlConnection)((ICloneable)connection).Clone())
               {
                    return GetSpParameterSetInternal(clonedConnection, spName, includeReturnValueParameter);
               }
          }

          /// <summary>
          /// Retrieves the set of MySqlParameters appropriate for the stored procedure
          /// </summary>
          /// <param name="connection">A valid MySqlConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="includeReturnValueParameter">A bool value indicating whether the return value parameter should be included in the results</param>
          /// <returns>An array of MySqlParameters</returns>
          private static MySqlParameter[] GetSpParameterSetInternal(MySqlConnection connection, string spName, bool includeReturnValueParameter)
          {
               if (connection == null)
                    throw new ArgumentNullException(nameof(connection));
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException(nameof(spName));
               MySqlParameter[] cachedParameters = null;
               string hashKey = null;
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException(nameof(spName));
               hashKey = string.Format("{0}:{1}{2}", connection.ConnectionString, spName, (includeReturnValueParameter == true ? ":include ReturnValue Parameter" : "").ToString());
               cachedParameters = (MySqlParameter[])paramCache[hashKey];
               if ((cachedParameters == null))
               {
                    MySqlParameter[] spParameters = DiscoverSpParameterSet(connection, spName, includeReturnValueParameter);
                    paramCache[hashKey] = spParameters;
                    cachedParameters = spParameters;
               }
               return CloneParameters(cachedParameters);
          }

          #endregion "Parameter Discovery Functions"
     }
}