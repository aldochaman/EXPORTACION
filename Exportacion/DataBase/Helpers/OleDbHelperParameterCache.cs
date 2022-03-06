using System;
using System.Collections;
using System.Data;
using System.Data.OleDb;

namespace Framework.DataBase
{
     /// <summary>
     /// OleDbHelperParameterCache provides functions to leverage a static cache of procedure parameters, and the ability to discover parameters for stored procedures at run-time.
     /// </summary>
     public sealed class OleDbHelperParameterCache
     {
          #region "private methods, variables, and constructors"

          /// <summary>
          /// Since this class provides only static methods, make the default constructor private to prevent
          /// instances from being created with "new OleDbHelperParameterCache()".
          /// </summary>
          private OleDbHelperParameterCache()
          {
          }

          /// <summary>
          /// The parameter cache
          /// </summary>
          private static Hashtable paramCache = Hashtable.Synchronized(new Hashtable());

          /// <summary>
          /// Resolve at run time the appropriate set of OleDbParameters for a stored procedure
          /// </summary>
          /// <param name="connection">A valid OleDbConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="includeReturnValueParameter">Whether or not to include their return value parameter</param>
          /// <param name="parameterValues">The parameter values.</param>
          /// <returns>The parameter array discovered.</returns>
          /// <exception cref="ArgumentNullException">
          /// connection
          /// or
          /// spName
          /// </exception>
          private static OleDbParameter[] DiscoverSpParameterSet(OleDbConnection connection, string spName, bool includeReturnValueParameter, params object[] parameterValues)
          {
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               OleDbCommand cmd = new OleDbCommand(spName, connection);
               cmd.CommandType = CommandType.StoredProcedure;
               OleDbParameter[] discoveredParameters = null;
               connection.Open();
               OleDbCommandBuilder.DeriveParameters(cmd);
               connection.Close();
               if (!includeReturnValueParameter)
               {
                    cmd.Parameters.RemoveAt(0);
               }

               discoveredParameters = new OleDbParameter[cmd.Parameters.Count];
               cmd.Parameters.CopyTo(discoveredParameters, 0);

               // Init the parameters with a DBNull value
               OleDbParameter discoveredParameter = null;
               foreach (OleDbParameter discoveredParameter_loopVariable in discoveredParameters)
               {
                    discoveredParameter = discoveredParameter_loopVariable;
                    discoveredParameter.Value = DBNull.Value;
               }

               return discoveredParameters;
          }

          /// <summary>
          /// Deep copy of cached OleDbParameter array
          /// </summary>
          /// <param name="originalParameters"></param>
          /// <returns></returns>
          private static OleDbParameter[] CloneParameters(OleDbParameter[] originalParameters)
          {
               int i = 0;
               int j = originalParameters.Length - 1;
               OleDbParameter[] clonedParameters = new OleDbParameter[j + 1];

               for (i = 0; i <= j; i++)
               {
                    clonedParameters[i] = (OleDbParameter)((ICloneable)originalParameters[i]).Clone();
               }

               return clonedParameters;
          }

          // CloneParameters

          #endregion "private methods, variables, and constructors"

          #region "caching functions"

          /// <summary>
          /// Add parameter array to the cache
          /// </summary>
          /// <param name="connectionString">A valid connection string for a OleDbConnection</param>
          /// <param name="commandText">The stored procedure name or T-OleDb command</param>
          /// <param name="commandParameters">An array of OleDbParamters to be cached</param>
          public static void CacheParameterSet(string connectionString, string commandText, params OleDbParameter[] commandParameters)
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
          /// <param name="connectionString">A valid connection string for a OleDbConnection</param>
          /// <param name="commandText">The stored procedure name or T-OleDb command</param>
          /// <returns>An array of OleDbParamters</returns>
          public static OleDbParameter[] GetCachedParameterSet(string connectionString, string commandText)
          {
               if (connectionString == null || connectionString.Length == 0)
                    throw new ArgumentNullException("connectionString");
               if (commandText == null || commandText.Length == 0)
                    throw new ArgumentNullException("commandText");

               string hashKey = string.Format("{0}:{1}", connectionString, commandText);
               OleDbParameter[] cachedParameters = (OleDbParameter[])paramCache[hashKey];

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
          /// Retrieves the set of OleDbParameters appropriate for the stored procedure
          /// </summary>
          /// <remarks>
          /// This method will query the database for this information, and then store it in a cache for future requests.
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a OleDbConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <returns>An array of OleDbParameters</returns>
          public static OleDbParameter[] GetSpParameterSet(string connectionString, string spName)
          {
               return GetSpParameterSet(connectionString, spName, false);
          }

          /// <summary>
          /// Retrieves the set of OleDbParameters appropriate for the stored procedure
          /// </summary>
          /// <remarks>
          /// This method will query the database for this information, and then store it in a cache for future requests.
          /// </remarks>
          /// <param name="connectionString">A valid connection string for a OleDbConnection</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="includeReturnValueParameter">A bool value indicating whether the return value parameter should be included in the results</param>
          /// <returns>An array of OleDbParameters</returns>
          public static OleDbParameter[] GetSpParameterSet(string connectionString, string spName, bool includeReturnValueParameter)
          {
               //OleDbParameter[] functionReturnValue = null;
               if ((connectionString == null || connectionString.Length == 0))
               {
                    throw new ArgumentNullException("connectionString");
               }
               using (OleDbConnection connection = new OleDbConnection(connectionString))
               {
                    return GetSpParameterSetInternal(connection, spName, includeReturnValueParameter);
               }
          }

          /// <summary>
          /// Retrieves the set of OleDbParameters appropriate for the stored procedure
          /// </summary>
          /// <remarks>
          /// This method will query the database for this information, and then store it in a cache for future requests.
          /// </remarks>
          /// <param name="connection">A valid OleDbConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <returns>An array of OleDbParameters</returns>

          public static OleDbParameter[] GetSpParameterSet(OleDbConnection connection, string spName)
          {
               return GetSpParameterSet(connection, spName, false);
          }

          /// <summary>
          /// Retrieves the set of OleDbParameters appropriate for the stored procedure
          /// </summary>
          /// <remarks>
          /// This method will query the database for this information, and then store it in a cache for future requests.
          /// </remarks>
          /// <param name="connection">A valid OleDbConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="includeReturnValueParameter">A bool value indicating whether the return value parameter should be included in the results</param>
          /// <returns>An array of OleDbParameters</returns>
          public static OleDbParameter[] GetSpParameterSet(OleDbConnection connection, string spName, bool includeReturnValueParameter)
          {
               //OleDbParameter[] functionReturnValue = null;
               if ((connection == null))
                    throw new ArgumentNullException("connection");
               using (OleDbConnection clonedConnection = (OleDbConnection)((ICloneable)connection).Clone())
               {
                    return GetSpParameterSetInternal(clonedConnection, spName, includeReturnValueParameter);
               }
          }

          /// <summary>
          /// Retrieves the set of OleDbParameters appropriate for the stored procedure
          /// </summary>
          /// <param name="connection">A valid OleDbConnection object</param>
          /// <param name="spName">The name of the stored procedure</param>
          /// <param name="includeReturnValueParameter">A bool value indicating whether the return value parameter should be included in the results</param>
          /// <returns>An array of OleDbParameters</returns>
          private static OleDbParameter[] GetSpParameterSetInternal(OleDbConnection connection, string spName, bool includeReturnValueParameter)
          {
               if (connection == null)
                    throw new ArgumentNullException("connection");
               if (spName == null || spName.Length == 0)
                    throw new ArgumentNullException("spName");
               OleDbParameter[] cachedParameters = null;
               string hashKey = null;
               if ((spName == null || spName.Length == 0))
                    throw new ArgumentNullException("spName");
               hashKey = string.Format("{0}:{1}{2}", connection.ConnectionString, spName, (includeReturnValueParameter == true ? ":include ReturnValue Parameter" : "").ToString());
               cachedParameters = (OleDbParameter[])paramCache[hashKey];
               if ((cachedParameters == null))
               {
                    OleDbParameter[] spParameters = DiscoverSpParameterSet(connection, spName, includeReturnValueParameter);
                    paramCache[hashKey] = spParameters;
                    cachedParameters = spParameters;
               }
               return CloneParameters(cachedParameters);
          }

          #endregion "Parameter Discovery Functions"
     }
}