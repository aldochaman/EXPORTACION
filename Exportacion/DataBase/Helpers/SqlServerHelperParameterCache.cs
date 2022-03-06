using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;

namespace Framework.DataBase
{
     /// <summary>
     /// SqlHelperParameterCache provides functions to leverage a static cache of procedure parameters, and the ability to discover parameters for stored procedures at run-time.
     /// </summary>
     public sealed class SqlServerHelperParameterCache
     {
          #region "private methods, variables, and constructors"

          /// <summary>
          /// Since this class provides only static methods, make the default constructor private to prevent
          /// instances from being created with "new SqlHelperParameterCache()".
          /// </summary>
          private SqlServerHelperParameterCache()
          {
          }

          /// <summary>
          /// The parameter cache
          /// </summary>
          private static Hashtable paramCache = Hashtable.Synchronized(new Hashtable());

          /// <summary>
          /// Resolve at run time the appropriate set of SqlParameters for a stored procedure
          /// </summary>
          /// <param name="connection">A valid SqlConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="includeReturnValueParameter">Whether or not to include their return value parameter</param>
          /// <param name="parameterValues">The parameter values.</param>
          /// <returns>The parameter array discovered.</returns>
          /// <exception cref="ArgumentNullException">
          /// connection
          /// or
          /// spName
          /// </exception>
          private static SqlParameter[] DiscoverSpParameterSet(SqlConnection connection, string spName, bool includeReturnValueParameter, params object[] parameterValues)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               SqlCommand cmd = new SqlCommand(spName, connection);
               cmd.CommandType = CommandType.StoredProcedure;
               SqlParameter[] discoveredParameters = null;
               connection.Open();
               SqlCommandBuilder.DeriveParameters(cmd);
               connection.Close();
               if (!includeReturnValueParameter)
               {
                    cmd.Parameters.RemoveAt(0);
               }

               discoveredParameters = new SqlParameter[cmd.Parameters.Count];
               cmd.Parameters.CopyTo(discoveredParameters, 0);

               // Init the parameters with a DBNull value
               SqlParameter discoveredParameter = null;
               foreach (SqlParameter discoveredParameter_loopVariable in discoveredParameters)
               {
                    discoveredParameter = discoveredParameter_loopVariable;
                    discoveredParameter.Value = DBNull.Value;
               }

               return discoveredParameters;
          }

          /// <summary>
          /// Deep copy of cached SqlParameter array
          /// </summary>
          /// <param name="originalParameters"></param>
          /// <returns></returns>
          private static SqlParameter[] CloneParameters(SqlParameter[] originalParameters)
          {
               int i = 0;
               int j = originalParameters.Length - 1;
               SqlParameter[] clonedParameters = new SqlParameter[j + 1];

               for (i = 0; i <= j; i++)
               {
                    clonedParameters[i] = (SqlParameter)((ICloneable)originalParameters[i]).Clone();
               }

               return clonedParameters;
          }

          // CloneParameters

          #endregion "private methods, variables, and constructors"

          #region "caching functions"

          /// <summary>
          /// Add parameter array to the cache
          /// </summary>
          /// <param name="connectionString">A valid connection string for a SqlConnection</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <param name="commandParameters">An array of SqlParamters to be cached</param>
          public static void CacheParameterSet(string connectionString, string commandText, params SqlParameter[] commandParameters)
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
          /// <param name="connectionString">A valid connection string for a SqlConnection</param>
          /// <param name="commandText">The stored procedure name or T-SQL command</param>
          /// <returns>An array of SqlParamters</returns>
          public static SqlParameter[] GetCachedParameterSet(string connectionString, string commandText)
          {
               if (connectionString == null || connectionString.Length == 0)
                    throw new ArgumentNullException("connectionString");
               if (commandText == null || commandText.Length == 0)
                    throw new ArgumentNullException("commandText");

               string hashKey = string.Format("{0}:{1}", connectionString, commandText);
               SqlParameter[] cachedParameters = (SqlParameter[])paramCache[hashKey];

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
          /// Retrieves the set of SqlParameters appropriate for the stored procedure
          /// </summary>
          /// <remarks>
          /// This method will query the database for this information, and then store it in a cache for future requests.
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a SqlConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <returns>An array of SqlParameters</returns>
          public static SqlParameter[] GetSpParameterSet(string connectionString, string spName)
          {
               return GetSpParameterSet(connectionString, spName, false);
          }

          /// <summary>
          /// Retrieves the set of SqlParameters appropriate for the stored procedure
          /// </summary>
          /// <remarks>
          /// This method will query the database for this information, and then store it in a cache for future requests.
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a SqlConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="includeReturnValueParameter">A bool value indicating whether the return value parameter should be included in the results</param>
          /// <returns>An array of SqlParameters</returns>
          public static SqlParameter[] GetSpParameterSet(string connectionString, string spName, bool includeReturnValueParameter)
          {
               //SqlParameter[] functionReturnValue = null;
               if ((connectionString == null || connectionString.Length == 0))
               {
                    throw new ArgumentNullException("connectionString");
               }
               using (SqlConnection connection = new SqlConnection(connectionString))
               {
                    return GetSpParameterSetInternal(connection, spName, includeReturnValueParameter);
               }
          }

          /// <summary>
          /// Retrieves the set of SqlParameters appropriate for the stored procedure
          /// </summary>
          /// <remarks>
          /// This method will query the database for this information, and then store it in a cache for future requests.
          /// </remarks>
          /// <param name="connection">A valid SqlConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <returns>An array of SqlParameters</returns>

          public static SqlParameter[] GetSpParameterSet(SqlConnection connection, string spName)
          {
               return GetSpParameterSet(connection, spName, false);
          }

          /// <summary>
          /// Retrieves the set of SqlParameters appropriate for the stored procedure
          /// </summary>
          /// <remarks>
          /// This method will query the database for this information, and then store it in a cache for future requests.
          /// </remarks>
          /// <param name="connection">A valid SqlConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="includeReturnValueParameter">A bool value indicating whether the return value parameter should be included in the results</param>
          /// <returns>An array of SqlParameters</returns>
          public static SqlParameter[] GetSpParameterSet(SqlConnection connection, string spName, bool includeReturnValueParameter)
          {
               //SqlParameter[] functionReturnValue = null;
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               using (SqlConnection clonedConnection = (SqlConnection)((ICloneable)connection).Clone())
               {
                    return GetSpParameterSetInternal(clonedConnection, spName, includeReturnValueParameter);
               }
          }

          /// <summary>
          /// Retrieves the set of SqlParameters appropriate for the stored procedure
          /// </summary>
          /// <param name="connection">A valid SqlConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="includeReturnValueParameter">A bool value indicating whether the return value parameter should be included in the results</param>
          /// <returns>An array of SqlParameters</returns>
          private static SqlParameter[] GetSpParameterSetInternal(SqlConnection connection, string spName, bool includeReturnValueParameter)
          {
               if (connection == null)
                    throw new ArgumentNullException("connection");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");
               SqlParameter[] cachedParameters = null;
               string hashKey = null;
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               hashKey = string.Format("{0}:{1}{2}", connection.ConnectionString, spName, (includeReturnValueParameter == true ? ":include ReturnValue Parameter" : "").ToString());
               cachedParameters = (SqlParameter[])paramCache[hashKey];
               if ((cachedParameters == null))
               {
                    SqlParameter[] spParameters = DiscoverSpParameterSet(connection, spName, includeReturnValueParameter);
                    paramCache[hashKey] = spParameters;
                    cachedParameters = spParameters;
               }
               return CloneParameters(cachedParameters);
          }

          #endregion "Parameter Discovery Functions"
     }
}