
using Framework.DataBase.Utilities;
using Framework.Enumerations;

using MySql.Data.MySqlClient;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Text;
using System.Threading;

namespace Framework.DataBase
{
     /// <summary>
     /// Clase para el manejo de la conexion a la base de datos
     /// </summary>
     public class HandlesConnection : IDisposable
     {
          #region Objetos de conexion

          /// <summary>
          /// The o current scope
          /// </summary>
          [ThreadStatic]
          private static HandlesConnection _oCurrentScope;

          /// <summary>
          /// The current SQL server transaction
          /// </summary>
          [ThreadStatic]
          private static IDbTransaction _CurrentSqlTransaction;

          /// <summary>
          /// The current SQL server connection
          /// </summary>
          [ThreadStatic]
          private static IDbConnection _CurrentSqlConnection;

          /// <summary>
          /// La cadena de conecion no es thread static Cadena de coenxion
          /// </summary>
          [ThreadStatic]
          private static string _StringConnection;

          /// <summary>
          /// Para el motor de la base de datos a la que se conectara
          /// </summary>
          [ThreadStatic]
          private static DatabaseEngines _DatabaseEngines;

          #endregion Objetos de conexion

          #region Variables

          /// <summary>
          /// _isDisposed
          /// </summary>
          private bool _isDisposed;

          /// <summary>
          /// Esta variable es para determinar quien fue la primera instancia. Ya que esta tendra un false como valor.
          /// Y se encargara de hacer el Dispose de la conexion
          /// </summary>
          private readonly bool _isNested;

          #endregion Variables

          #region Propiedades

          /// <summary>
          /// Gets the current scope.
          /// </summary>
          /// <value>
          /// The current scope.
          /// </value>
          public static HandlesConnection CurrentScope
          {
               get
               {
                    return _oCurrentScope;
               }
          }

          /// <summary>
          /// Gets the curren SQL server transaction.
          /// </summary>
          /// <value>
          /// The curren SQL server transaction.
          /// </value>
          public static IDbTransaction CurrenSqlTransaction
          {
               get
               {
                    return _CurrentSqlTransaction;
               }
          }

          /// <summary>
          /// Gets the current SQL serve connection.
          /// </summary>
          /// <value>
          /// The current SQL serve connection.
          /// </value>
          public static IDbConnection CurrentSqlConnection
          {
               get
               {
                    return _CurrentSqlConnection;
               }
          }

          #endregion Propiedades

          #region Constructores

          /// <summary>
          /// Initializes a new instance of the <see cref="HandlesConnection"/> class.
          /// </summary>
          /// <param name="connection">The po conexion.</param>
          public HandlesConnection(DataAccess connection) : this(connection.DataBase.StringConnection, connection.DataBase.Engine)
          {
          }

          /// <summary>
          /// Initializes a new instance of the <see cref="HandlesConnection"/> class.
          /// </summary>
          public HandlesConnection() : this(_StringConnection, _DatabaseEngines)
          {
          }

          /// <summary>
          /// Initializes a new instance of the <see cref="HandlesConnection"/> class.
          /// </summary>
          /// <param name="stringConnection">The ps cadena conexion.</param>
          /// <param name="dataBaseEngines">The penm motor.</param>
          public HandlesConnection(string stringConnection, DatabaseEngines dataBaseEngines)
          {
               _StringConnection = stringConnection;
               _DatabaseEngines = dataBaseEngines;
               if (!Object.Equals(_oCurrentScope, null) && !_oCurrentScope._isDisposed)
               {
                    //ConexionActual = _currentScope.ConexionActual;
                    _isNested = true;
                    //_bConexionAbierta = true;
               }
               else
               {
                    StartConnection();
                    Thread.BeginThreadAffinity();
                    _oCurrentScope = this;
               }
          }

          #endregion Constructores

          #region Manejo de conexiones y transacciones

          /// <summary>
          /// Inicia la conexion a la base de datos
          /// </summary>
          private static void StartConnection()
          {
               //Crear la nueba conecion de datos
               switch (_DatabaseEngines)
               {
                    case DatabaseEngines.SqlServer:
                         _CurrentSqlConnection = new SqlConnection(_StringConnection);
                         break;
                    case DatabaseEngines.MySql:
                         _CurrentSqlConnection = new MySqlConnection(_StringConnection);
                         break;
                    case DatabaseEngines.PostgressSql:
                         _CurrentSqlConnection = new NpgsqlConnection(_StringConnection);
                         break;
                    case DatabaseEngines.Oracle:
                         _CurrentSqlConnection = new OracleConnection(_StringConnection);
                         break;
                    case DatabaseEngines.Odbc:
                         _CurrentSqlConnection = new OdbcConnection(_StringConnection);
                         break;
                    case DatabaseEngines.OleDb:
                         _CurrentSqlConnection = new OleDbConnection(_StringConnection);
                         break;
               }
               if (_CurrentSqlConnection.State != ConnectionState.Open)
                    _CurrentSqlConnection.Open();
          }

          /// <summary>
          /// Metodo para heredar a la conexion una transaccion
          /// </summary>
          public static void BeginTransaction()
          {
               if (HandlesConnection.CurrentScope == null)
                    return;

               if (_CurrentSqlTransaction != null)
                    return;
               var connectionsql = HandlesConnection.CurrentSqlConnection;
               if (connectionsql.State == System.Data.ConnectionState.Closed)
               {
                    connectionsql.Open();
               }
               _CurrentSqlTransaction = connectionsql.BeginTransaction();
          }

          /// <summary>
          /// Funcion para confirmar la transaccion
          /// </summary>
          public static void CommitTransaction()
          {
               if (_CurrentSqlTransaction == null)
                    return;
               _CurrentSqlTransaction.Commit();
               _CurrentSqlTransaction = null;
          }

          /// <summary>
          /// Fncion a para deshacer la transaccion
          /// </summary>
          public static void RollbackTransaction()
          {
               try
               {
                    if (_CurrentSqlTransaction == null)
                         return;
                    _CurrentSqlTransaction.Rollback();
                    
               }
               catch
               {

               }
               finally
               {
                    _CurrentSqlTransaction = null;
               }
          }

          /// <summary>
          /// Libera lso recursos de la conexion
          /// </summary>
          public void Dispose()
          {
               if (!_isNested && !_isDisposed)
               {
                    switch (_DatabaseEngines)
                    {
                         default:
                              if (_CurrentSqlTransaction != null)
                              {                                                                    
                                   _CurrentSqlTransaction.Dispose();
                                   _CurrentSqlTransaction = null;
                              }
                              if (_CurrentSqlConnection != null)
                              {                                  
                                   _CurrentSqlConnection.Close();
                                   _CurrentSqlConnection.Dispose();
                                   _CurrentSqlConnection = null;
                              }
                              break;
                    }
                    _oCurrentScope = null;
                    GC.SuppressFinalize(this);
                    Thread.EndThreadAffinity();
                    _isDisposed = true;
               }
          }

          #endregion Manejo de conexiones y transacciones

          #region Dataset

          /// <summary>
          /// Fills the dataset.
          /// </summary>
          /// <param name="psQuery">The ps query.</param>
          /// <param name="psTabla">The ps tabla.</param>
          /// <returns></returns>
          public static DataSet FillDataset(StringBuilder psQuery, string psTabla)
          {
               DataSet ldtsResultado = new DataSet();
               switch (_DatabaseEngines)
               {
                    case DatabaseEngines.SqlServer:
                         if (_CurrentSqlTransaction == null)
                              SqlServerHelper.FillDataset((SqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString(), ldtsResultado, new string[] { psTabla });
                         else
                              SqlServerHelper.FillDataset((SqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString(), ldtsResultado, new string[] { psTabla });
                         break;

                    case DatabaseEngines.MySql:
                         if (_CurrentSqlTransaction == null)
                              MySqlHelper.FillDataset((MySqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString(), ldtsResultado, new string[] { psTabla });
                         else
                              MySqlHelper.FillDataset((MySqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString(), ldtsResultado, new string[] { psTabla });
                         break;

                    case DatabaseEngines.PostgressSql:
                         if (_CurrentSqlTransaction == null)
                              PostgresSQLHelper.FillDataset((NpgsqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString(), ldtsResultado, new string[] { psTabla });
                         else
                              PostgresSQLHelper.FillDataset((NpgsqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString(), ldtsResultado, new string[] { psTabla });
                         break;
                    case DatabaseEngines.Oracle:
                         if (_CurrentSqlTransaction == null)
                              OracleHelper.FillDataset((OracleConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString(), ldtsResultado, new string[] { psTabla });
                         else
                              OracleHelper.FillDataset((OracleTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString(), ldtsResultado, new string[] { psTabla });
                         break;
                    case DatabaseEngines.Odbc:
                         if (_CurrentSqlTransaction == null)
                              OdbcHelper.FillDataset((OdbcConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString(), ldtsResultado, new string[] { psTabla });
                         else
                              OdbcHelper.FillDataset((OdbcTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString(), ldtsResultado, new string[] { psTabla });
                         break;
                    case DatabaseEngines.OleDb:
                         if (_CurrentSqlTransaction == null)
                              OleDbHelper.FillDataset((OleDbConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString(), ldtsResultado, new string[] { psTabla });
                         else
                              OleDbHelper.FillDataset((OleDbTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString(), ldtsResultado, new string[] { psTabla });
                         break;
               }
               return ldtsResultado;
          }

          #endregion Dataset

          #region DataReader

          /// <summary>
          /// Fills the data reader.
          /// </summary>
          /// <param name="psQuery">The ps query.</param>
          /// <returns></returns>
          public static IDataReader FillDataReader(StringBuilder psQuery)
          {
               switch (_DatabaseEngines)
               {
                    case DatabaseEngines.SqlServer:
                         if (_CurrentSqlTransaction == null)
                              return SqlServerHelper.FillDataReader((SqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery);
                         else
                              return SqlServerHelper.FillDataReader((SqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery);
                    case DatabaseEngines.MySql:
                         if (_CurrentSqlTransaction == null)
                              return MySqlHelper.FillDataReader((MySqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery);
                         else
                              return MySqlHelper.FillDataReader((MySqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery);
                    case DatabaseEngines.PostgressSql:
                         if (_CurrentSqlTransaction == null)
                              return PostgresSQLHelper.FillDataReader((NpgsqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery);
                         else
                              return PostgresSQLHelper.FillDataReader((NpgsqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery);
                    case DatabaseEngines.Oracle:
                         if (_CurrentSqlTransaction == null)
                              return OracleHelper.FillDataReader((OracleConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery);
                         else
                              return OracleHelper.FillDataReader((OracleTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery);
                    case DatabaseEngines.Odbc:
                         if (_CurrentSqlTransaction == null)
                              OdbcHelper.FillDataReader((OdbcConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery);
                         else
                              OdbcHelper.FillDataReader((OdbcTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery);
                         break;
                    case DatabaseEngines.OleDb:
                         if (_CurrentSqlTransaction == null)
                              OleDbHelper.FillDataReader((OleDbConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery);
                         else
                              OleDbHelper.FillDataReader((OleDbTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery);
                         break;
               }
               return null;
          }

          /// <summary>
          /// Fills the data reader.
          /// </summary>
          /// <param name="psQuery">The ps query.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <returns></returns>
          public static IDataReader FillDataReader(StringBuilder psQuery, IEnumerable<ParameterSql> poParametros)
          {
               switch (_DatabaseEngines)
               {
                    case DatabaseEngines.SqlServer:
                         if (_CurrentSqlTransaction == null)
                              return SqlServerHelper.FillDataReader((SqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros);
                         else
                              return SqlServerHelper.FillDataReader((SqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros);

                    case DatabaseEngines.MySql:
                         if (_CurrentSqlTransaction == null)
                              return MySqlHelper.FillDataReader((MySqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros);
                         else
                              return MySqlHelper.FillDataReader((MySqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros);

                    case DatabaseEngines.PostgressSql:
                         if (_CurrentSqlTransaction == null)
                              return PostgresSQLHelper.FillDataReader((NpgsqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros);
                         else
                              return PostgresSQLHelper.FillDataReader((NpgsqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros);
                    case DatabaseEngines.Oracle:
                         if (_CurrentSqlTransaction == null)
                              return OracleHelper.FillDataReader((OracleConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros);
                         else
                              return OracleHelper.FillDataReader((OracleTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros);
                    case DatabaseEngines.Odbc:
                         if (_CurrentSqlTransaction == null)
                              OdbcHelper.FillDataReader((OdbcConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros);
                         else
                              OdbcHelper.FillDataReader((OdbcTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros);
                         break;
                    case DatabaseEngines.OleDb:
                         if (_CurrentSqlTransaction == null)
                              OleDbHelper.FillDataReader((OleDbConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros);
                         else
                              OleDbHelper.FillDataReader((OleDbTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros);
                         break;
               }
               return null;
          }

          #endregion DataReader

          #region DataTable

          /// <summary>
          /// Fills the data table.
          /// </summary>
          /// <param name="psQuery">The ps query.</param>
          /// <param name="psTabla">The ps tabla.</param>
          /// <returns></returns>
          public static DataTable FillDataTable(StringBuilder psQuery, string psTabla)
          {
               DataTable ldtResultado = new DataTable();
               switch (_DatabaseEngines)
               {
                    case DatabaseEngines.SqlServer:
                         if (_CurrentSqlTransaction == null)
                              SqlServerHelper.FillDataTable((SqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString(), ldtResultado, new string[] { psTabla });
                         else
                              SqlServerHelper.FillDataTable((SqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString(), ldtResultado, new string[] { psTabla });
                         break;

                    case DatabaseEngines.MySql:
                         if (_CurrentSqlTransaction == null)
                              MySqlHelper.FillDataTable((MySqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString(), ldtResultado, new string[] { psTabla });
                         else
                              MySqlHelper.FillDataTable((MySqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString(), ldtResultado, new string[] { psTabla });
                         break;

                    case DatabaseEngines.PostgressSql:
                         if (_CurrentSqlTransaction == null)
                              PostgresSQLHelper.FillDataTable((NpgsqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString(), ldtResultado, new string[] { psTabla });
                         else
                              PostgresSQLHelper.FillDataTable((NpgsqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString(), ldtResultado, new string[] { psTabla });
                         break;
                    case DatabaseEngines.Oracle:
                         if (_CurrentSqlTransaction == null)
                              OracleHelper.FillDataTable((OracleConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString(), ldtResultado, new string[] { psTabla });
                         else
                              OracleHelper.FillDataTable((OracleTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString(), ldtResultado, new string[] { psTabla });
                         break;
                    case DatabaseEngines.Odbc:
                         if (_CurrentSqlTransaction == null)
                              OdbcHelper.FillDataTable((OdbcConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString(), ldtResultado, new string[] { psTabla });
                         else
                              OdbcHelper.FillDataTable((OdbcTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString(), ldtResultado, new string[] { psTabla });
                         break;
                    case DatabaseEngines.OleDb:
                         if (_CurrentSqlTransaction == null)
                              OleDbHelper.FillDataTable((OleDbConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString(), ldtResultado, new string[] { psTabla });
                         else
                              OleDbHelper.FillDataTable((OleDbTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString(), ldtResultado, new string[] { psTabla });
                         break;
               }
               return ldtResultado;
          }

          /// <summary>
          /// Fills the data table.
          /// </summary>
          /// <param name="psQuery">The ps query.</param>
          /// <returns></returns>
          public static DataTable FillDataTable(StringBuilder psQuery)
          {
               return FillDataTable(psQuery, "Tabla");
          }

          /// <summary>
          /// Fills the data table.
          /// </summary>
          /// <param name="psQuery">The ps query.</param>
          /// <param name="parameters">The po parametros.</param>
          /// <returns></returns>
          public static DataTable FillDataTable(StringBuilder psQuery, IEnumerable<ParameterSql> parameters)
          {
               DataTable ldtResultado = new DataTable();
               switch (_DatabaseEngines)
               {
                    case DatabaseEngines.SqlServer:
                         if (_CurrentSqlTransaction == null)
                              SqlServerHelper.FillDataTable((SqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, ldtResultado, parameters);
                         else
                              SqlServerHelper.FillDataTable((SqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, ldtResultado, parameters);
                         break;

                    case DatabaseEngines.MySql:
                         if (_CurrentSqlTransaction == null)
                              MySqlHelper.FillDataTable((MySqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, ldtResultado, parameters);
                         else
                              MySqlHelper.FillDataTable((MySqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, ldtResultado, parameters);
                         break;

                    case DatabaseEngines.PostgressSql:
                         if (_CurrentSqlTransaction == null)
                              PostgresSQLHelper.FillDataTable((NpgsqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, ldtResultado, parameters);
                         else
                              PostgresSQLHelper.FillDataTable((NpgsqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, ldtResultado, parameters);
                         break;
                    case DatabaseEngines.Oracle:
                         if (_CurrentSqlTransaction == null)
                              OracleHelper.FillDataTable((OracleConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, ldtResultado, parameters);
                         else
                              OracleHelper.FillDataTable((OracleTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, ldtResultado, parameters);
                         break;
                    case DatabaseEngines.Odbc:
                         if (_CurrentSqlTransaction == null)
                              OdbcHelper.FillDataTable((OdbcConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, ldtResultado, parameters);
                         else
                              OdbcHelper.FillDataTable((OdbcTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, ldtResultado, parameters);
                         break;
                    case DatabaseEngines.OleDb:
                         if (_CurrentSqlTransaction == null)
                              OleDbHelper.FillDataTable((OleDbConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, ldtResultado, parameters);
                         else
                              OleDbHelper.FillDataTable((OleDbTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, ldtResultado, parameters);
                         break;
               }
               return ldtResultado;
          }

          #endregion DataTable

          #region Ejecuta comando

          /// <summary>
          /// Executes the non query.
          /// </summary>
          /// <param name="psQuery">The ps query.</param>
          /// <returns></returns>
          public static int ExecuteNonQuery(StringBuilder psQuery)
          {
               switch (_DatabaseEngines)
               {
                    case DatabaseEngines.SqlServer:
                         if (_CurrentSqlTransaction == null)
                              return SqlServerHelper.ExecuteNonQuery((SqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString());
                         else
                              return SqlServerHelper.ExecuteNonQuery((SqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString());

                    case DatabaseEngines.MySql:
                         if (_CurrentSqlTransaction == null)
                              return MySqlHelper.ExecuteNonQuery((MySqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString());
                         else
                              return MySqlHelper.ExecuteNonQuery((MySqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString());

                    case DatabaseEngines.PostgressSql:
                         if (_CurrentSqlTransaction == null)
                              return PostgresSQLHelper.ExecuteNonQuery((NpgsqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString());
                         else
                              return PostgresSQLHelper.ExecuteNonQuery((NpgsqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString());
                    case DatabaseEngines.Oracle:
                         if (_CurrentSqlTransaction == null)
                              return OracleHelper.ExecuteNonQuery((OracleConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString());
                         else
                              return OracleHelper.ExecuteNonQuery((OracleTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString());
                    case DatabaseEngines.Odbc:
                         if (_CurrentSqlTransaction == null)
                             return  OdbcHelper.ExecuteNonQuery((OdbcConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString());
                         else
                             return  OdbcHelper.ExecuteNonQuery((OdbcTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString());
                    case DatabaseEngines.OleDb:
                         if (_CurrentSqlTransaction == null)
                              OleDbHelper.ExecuteNonQuery((OleDbConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString());
                         else
                              OleDbHelper.ExecuteNonQuery((OleDbTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString());
                         break;
               }
               return 0;
          }

          /// <summary>
          /// Ejecutas the comando.
          /// </summary>
          /// <param name="psQuery">The ps query.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <param name="pbIlimitado">if set to <c>true</c> [pb ilimitado].</param>
          public static int ExecuteNonQuery(StringBuilder psQuery, IEnumerable<ParameterSql> poParametros, bool pbIlimitado)
          {
               switch (_DatabaseEngines)
               {
                    case DatabaseEngines.SqlServer:
                         if (_CurrentSqlTransaction == null)
                              return SqlServerHelper.ExecuteNonQuery((SqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros, pbIlimitado);
                         else
                              return SqlServerHelper.ExecuteNonQuery((SqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros, pbIlimitado);

                    case DatabaseEngines.MySql:
                         if (_CurrentSqlTransaction == null)
                              return MySqlHelper.ExecuteNonQuery((MySqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros, pbIlimitado);
                         else
                              return MySqlHelper.ExecuteNonQuery((MySqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros, pbIlimitado);

                    case DatabaseEngines.PostgressSql:
                         if (_CurrentSqlTransaction == null)
                              return PostgresSQLHelper.ExecuteNonQuery((NpgsqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros, pbIlimitado);
                         else
                              return PostgresSQLHelper.ExecuteNonQuery((NpgsqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros, pbIlimitado);
                    case DatabaseEngines.Oracle:
                         if (_CurrentSqlTransaction == null)
                              return OracleHelper.ExecuteNonQuery((OracleConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros, pbIlimitado);
                         else
                              return OracleHelper.ExecuteNonQuery((OracleTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros, pbIlimitado);
                    case DatabaseEngines.Odbc:
                         if (_CurrentSqlTransaction == null)
                              return OdbcHelper.ExecuteNonQuery((OdbcConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros, pbIlimitado);
                         else
                              return OdbcHelper.ExecuteNonQuery((OdbcTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros, pbIlimitado);
                    case DatabaseEngines.OleDb:
                         if (_CurrentSqlTransaction == null)
                              OleDbHelper.ExecuteNonQuery((OleDbConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros, pbIlimitado);
                         else
                              OleDbHelper.ExecuteNonQuery((OleDbTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros, pbIlimitado);
                         break;
               }
               return 0;
          }

          /// <summary>
          /// Executes the non query.
          /// </summary>
          /// <param name="psQuery">The ps query.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <returns></returns>
          public static int ExecuteNonQuery(StringBuilder psQuery, IEnumerable<ParameterSql> poParametros)
          {
               return ExecuteNonQuery(psQuery, poParametros, true);
          }

          

          #endregion Ejecuta comando

          #region Ejectua escalar

          /// <summary>
          /// Ejecutas the escalar.
          /// </summary>
          /// <param name="psQuery">The ps query.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <returns></returns>
          public static object ExecuteScalar(StringBuilder psQuery, IEnumerable<ParameterSql> poParametros)
          {
               switch (_DatabaseEngines)
               {
                    case DatabaseEngines.SqlServer:
                         if (_CurrentSqlTransaction == null)
                              return SqlServerHelper.ExecuteScalar((SqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros);
                         else
                              return SqlServerHelper.ExecuteScalar((SqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros);

                    case DatabaseEngines.MySql:
                         if (_CurrentSqlTransaction == null)
                              return MySqlHelper.ExecuteScalar((MySqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros);
                         else
                              return MySqlHelper.ExecuteScalar((MySqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros);

                    case DatabaseEngines.PostgressSql:
                         if (_CurrentSqlTransaction == null)
                              return PostgresSQLHelper.ExecuteScalar((NpgsqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros);
                         else
                              return PostgresSQLHelper.ExecuteScalar((NpgsqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros);
                    case DatabaseEngines.Oracle:
                         if (_CurrentSqlTransaction == null)
                              return OracleHelper.ExecuteScalar((OracleConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros);
                         else
                              return OracleHelper.ExecuteScalar((OracleTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros);
                    case DatabaseEngines.Odbc:
                         if (_CurrentSqlTransaction == null)
                              return OdbcHelper.ExecuteScalar((OdbcConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros);
                         else
                              return OdbcHelper.ExecuteScalar((OdbcTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros);
                    case DatabaseEngines.OleDb:
                         if (_CurrentSqlTransaction == null)
                              OleDbHelper.ExecuteScalar((OleDbConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery, poParametros);
                         else
                              OleDbHelper.ExecuteScalar((OleDbTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery, poParametros);
                         break;
               }
               return null;
          }

          /// <summary>
          /// Ejecutas the escalar.
          /// </summary>
          /// <param name="psQuery">The ps query.</param>
          /// <returns></returns>
          public static object ExecuteScalar(StringBuilder psQuery)
          {
               switch (_DatabaseEngines)
               {
                    case DatabaseEngines.SqlServer:
                         if (_CurrentSqlTransaction == null)
                              return SqlServerHelper.ExecuteScalar((SqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString());
                         else
                              return SqlServerHelper.ExecuteScalar((SqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString());

                    case DatabaseEngines.MySql:
                         if (_CurrentSqlTransaction == null)
                              return MySqlHelper.ExecuteScalar((MySqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString());
                         else
                              return MySqlHelper.ExecuteScalar((MySqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString());

                    case DatabaseEngines.PostgressSql:
                         if (_CurrentSqlTransaction == null)
                              return PostgresSQLHelper.ExecuteScalar((NpgsqlConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString());
                         else
                              return PostgresSQLHelper.ExecuteScalar((NpgsqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString());
                    case DatabaseEngines.Oracle:
                         if (_CurrentSqlTransaction == null)
                              return OracleHelper.ExecuteScalar((OracleConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString());
                         else
                              return OracleHelper.ExecuteScalar((OracleTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString());
                    case DatabaseEngines.Odbc:
                         if (_CurrentSqlTransaction == null)
                              return OdbcHelper.ExecuteScalar((OdbcConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString());
                         else
                              return OdbcHelper.ExecuteScalar((OdbcTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString());
                    case DatabaseEngines.OleDb:
                         if (_CurrentSqlTransaction == null)
                              OleDbHelper.ExecuteScalar((OleDbConnection)_CurrentSqlConnection, System.Data.CommandType.Text, psQuery.ToString());
                         else
                              OleDbHelper.ExecuteScalar((OleDbTransaction)_CurrentSqlTransaction, System.Data.CommandType.Text, psQuery.ToString());
                         break;
               }
               return null;
          }

          #endregion Ejectua escalar

          #region Store procedure

          /// <summary>
          /// Ejecutas the comando procedimiento almacenado.
          /// </summary>
          /// <param name="psNombre">The ps nombre.</param>
          /// <returns></returns>
          public static int StoreProcedureExecuteNonQuery(string psNombre)
          {
               return StoreProcedureExecuteNonQuery(psNombre, null);
          }

          /// <summary>
          /// Ejecutas the sp.
          /// </summary>
          /// <param name="psNombre">The ps nombre.</param>
          /// <param name="poParametros">The po parametros.</param>
          public static int StoreProcedureExecuteNonQuery(string psNombre, IEnumerable<ParameterSql> poParametros)
          {
               switch (_DatabaseEngines)
               {
                    case DatabaseEngines.SqlServer:
                         if (_CurrentSqlTransaction == null)
                              return SqlServerHelper.StoreProcedureExecuteNonQuery((SqlConnection)_CurrentSqlConnection, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                         else
                              return SqlServerHelper.StoreProcedureExecuteNonQuery((SqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.StoredProcedure, psNombre, poParametros);

                    case DatabaseEngines.MySql:
                         if (_CurrentSqlTransaction == null)
                              return MySqlHelper.StoreProcedureExecuteNonQuery((MySqlConnection)_CurrentSqlConnection, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                         else
                              return MySqlHelper.StoreProcedureExecuteNonQuery((MySqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.StoredProcedure, psNombre, poParametros);

                    case DatabaseEngines.PostgressSql:
                         if (_CurrentSqlTransaction == null)
                              return PostgresSQLHelper.StoreProcedureExecuteNonQuery((NpgsqlConnection)_CurrentSqlConnection, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                         else
                              return PostgresSQLHelper.StoreProcedureExecuteNonQuery((NpgsqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                    case DatabaseEngines.Oracle:
                         if (_CurrentSqlTransaction == null)
                              return OracleHelper.StoreProcedureExecuteNonQuery((OracleConnection)_CurrentSqlConnection, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                         else
                              return OracleHelper.StoreProcedureExecuteNonQuery((OracleTransaction)_CurrentSqlTransaction, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                    case DatabaseEngines.Odbc:
                         if (_CurrentSqlTransaction == null)
                              return OdbcHelper.StoreProcedureExecuteNonQuery((OdbcConnection)_CurrentSqlConnection, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                         else
                              return OdbcHelper.StoreProcedureExecuteNonQuery((OdbcTransaction)_CurrentSqlTransaction, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                    case DatabaseEngines.OleDb:
                         if (_CurrentSqlTransaction == null)
                              return OleDbHelper.StoreProcedureExecuteNonQuery((OleDbConnection)_CurrentSqlConnection, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                         else
                              return  OleDbHelper.StoreProcedureExecuteNonQuery((OleDbTransaction)_CurrentSqlTransaction, System.Data.CommandType.StoredProcedure, psNombre, poParametros);                         
               }
               return 0;
          }

          /// <summary>
          /// Ejecutas the escalar procedimiento almacenado.
          /// </summary>
          /// <param name="psNombre">The ps nombre.</param>
          /// <returns></returns>
          public static object StoreProcedureExecuteScalar(string psNombre)
          {
               return StoreProcedureExecuteScalar(psNombre, null);
          }

          /// <summary>
          /// Ejecutas the escalar procedimiento almacenado.
          /// </summary>
          /// <param name="psNombre">The ps nombre.</param>
          /// <param name="poParametros">The po parametros.</param>
          /// <returns></returns>
          public static object StoreProcedureExecuteScalar(string psNombre, IEnumerable<ParameterSql> poParametros)
          {
               switch (_DatabaseEngines)
               {
                    case DatabaseEngines.SqlServer:
                         if (_CurrentSqlTransaction == null)
                              return SqlServerHelper.StoreProcedureExecuteScalar((SqlConnection)_CurrentSqlConnection, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                         else
                              return SqlServerHelper.StoreProcedureExecuteScalar((SqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.StoredProcedure, psNombre, poParametros);

                    case DatabaseEngines.MySql:
                         if (_CurrentSqlTransaction == null)
                              return MySqlHelper.StoreProcedureExecuteScalar((MySqlConnection)_CurrentSqlConnection, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                         else
                              return MySqlHelper.StoreProcedureExecuteScalar((MySqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.StoredProcedure, psNombre, poParametros);

                    case DatabaseEngines.PostgressSql:
                         if (_CurrentSqlTransaction == null)
                              return PostgresSQLHelper.StoreProcedureExecuteScalar((NpgsqlConnection)_CurrentSqlConnection, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                         else
                              return PostgresSQLHelper.StoreProcedureExecuteScalar((NpgsqlTransaction)_CurrentSqlTransaction, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                    case DatabaseEngines.Oracle:
                         if (_CurrentSqlTransaction == null)
                              return OracleHelper.StoreProcedureExecuteScalar((OracleConnection)_CurrentSqlConnection, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                         else
                              return OracleHelper.StoreProcedureExecuteScalar((OracleTransaction)_CurrentSqlTransaction, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                    case DatabaseEngines.Odbc:
                         if (_CurrentSqlTransaction == null)
                              return OdbcHelper.StoreProcedureExecuteScalar((OdbcConnection)_CurrentSqlConnection, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                         else
                              return OdbcHelper.StoreProcedureExecuteScalar((OdbcTransaction)_CurrentSqlTransaction, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                    case DatabaseEngines.OleDb:
                         if (_CurrentSqlTransaction == null)
                              return OleDbHelper.StoreProcedureExecuteScalar((OleDbConnection)_CurrentSqlConnection, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
                         else
                              return OleDbHelper.StoreProcedureExecuteScalar((OleDbTransaction)_CurrentSqlTransaction, System.Data.CommandType.StoredProcedure, psNombre, poParametros);
               }
               return null;
          }

          #endregion Store procedure

          #region Bulkcopy

          /// <summary>
          /// Inserts the bulk copy.
          /// </summary>
          /// <param name="pdtTablaOrigen">The PDT tabla origen.</param>
          /// <param name="psTablaDestino">The ps tabla destino.</param>
          public static void InsertBulkCopy(DataTable pdtTablaOrigen, string psTablaDestino)
          {
               switch (_DatabaseEngines)
               {
                    case DatabaseEngines.SqlServer:
                    case DatabaseEngines.MySql:
                    case DatabaseEngines.PostgressSql:
                    case DatabaseEngines.Oracle:
                    case DatabaseEngines.Odbc:
                    case DatabaseEngines.OleDb:
                         InsertBuilkCopyByRows(pdtTablaOrigen, psTablaDestino);
                         break;
               }
          }
          private static void InsertBuilkCopyByRows(DataTable pdtTablaOrigen, string psTablaDestino)
          {
               StringBuilder query = new StringBuilder();
               StringBuilder columns = new StringBuilder();
               StringBuilder rows = new StringBuilder();
               List<ParameterSql> parameters;
               query.AppendFormat("Insert into {0}", psTablaDestino);
               foreach (DataColumn column in pdtTablaOrigen.Columns)
               {
                    columns.AppendFormat("{0},",column.ColumnName);
                    rows.AppendFormat("@{0},", column.ColumnName);
               }
               query.AppendFormat(" ({0}) values({1}) ", columns.Remove(columns.Length-1,1), rows.Remove(rows.Length - 1, 1));
               foreach (DataRow row in pdtTablaOrigen.Rows)
               {
                    parameters = new List<ParameterSql>();
                    foreach (DataColumn column in pdtTablaOrigen.Columns)
                    {
                         parameters.Add(new ParameterSql($"@{column.ColumnName}", row[column.ColumnName]));
                    }
                    ExecuteNonQuery(query, parameters);
               }               
          }

          private static void OriginalInsertBuilkCopy(DataTable pdtTablaOrigen, string psTablaDestino)
          {
               if (_CurrentSqlTransaction != null)
               {
                    using (SqlBulkCopy loBulkCopy = new SqlBulkCopy((SqlConnection)_CurrentSqlConnection, SqlBulkCopyOptions.Default, (SqlTransaction)_CurrentSqlTransaction))
                    {
                         loBulkCopy.BatchSize = pdtTablaOrigen.Rows.Count;
                         loBulkCopy.DestinationTableName = psTablaDestino;
                         loBulkCopy.BulkCopyTimeout = 0;
                         loBulkCopy.WriteToServer(pdtTablaOrigen);
                    }
               }
               else
               {
                    using (SqlBulkCopy loBulkCopy = new SqlBulkCopy((SqlConnection)_CurrentSqlConnection))
                    {
                         loBulkCopy.NotifyAfter = 1;
                         loBulkCopy.BatchSize = pdtTablaOrigen.Rows.Count;
                         loBulkCopy.DestinationTableName = psTablaDestino;
                         loBulkCopy.BulkCopyTimeout = 0;
                         loBulkCopy.WriteToServer(pdtTablaOrigen);
                    }
               }
          }
          #endregion Bulkcopy
     }
}