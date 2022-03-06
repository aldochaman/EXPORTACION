using Framework.Enumerations;
using MySql.Data.MySqlClient;
using Npgsql;
using System;
using System.Data.SqlClient;
using System.Text;

namespace Framework.DataBase
{
     /// <summary>
     /// DataBaseConfig
     /// </summary>
     [Serializable]
     public class DataBaseConfig
     {
          #region Properties

          /// <summary>
          /// Gets or sets the servidor vinculado.
          /// </summary>
          /// <value>
          /// The servidor vinculado.
          /// </value>
          public string LinkedServer { get; set; }

          /// <summary>
          /// Gets or sets the nombre.
          /// </summary>
          /// <value>
          /// The nombre.
          /// </value>
          public string Server { get; set; }

          /// <summary>
          ///
          /// </summary>
          public string Catalog { get; set; }

          /// <summary>
          /// Gets or sets the bitacora.
          /// </summary>
          /// <value>
          /// The bitacora.
          /// </value>
          public string LogData { get; set; }

          /// <summary>
          /// Gets or sets the collation.
          /// </summary>
          /// <value>
          /// The collation.
          /// </value>
          public string Collation { get; set; }

          /// <summary>
          /// Gets or sets the usuario.
          /// </summary>
          /// <value>
          /// The usuario.
          /// </value>
          public string User { get; set; }

          /// <summary>
          /// Gets or sets the contraseña.
          /// </summary>
          /// <value>
          /// The contraseña.
          /// </value>
          public string Password { get; set; }

          /// <summary>
          ///
          /// </summary>
          public bool FilterCompanies { get; set; }

          /// <summary>
          ///
          /// </summary>
          public string EmpoyeeTable { get; set; }

          /// <summary>
          ///
          /// </summary>
          public string AreaBranchOfficeTable { get; set; }

          /// <summary>
          ///
          /// </summary>
          public string Url { get; set; }

          /// <summary>
          ///
          /// </summary>
          public string SqlVersion { get; set; }

          /// <summary>
          ///
          /// </summary>
          public string SapUser { get; set; }

          /// <summary>
          ///
          /// </summary>
          public string SapPassword { get; set; }

          /// <summary>
          ///
          /// </summary>
          public string Company { get; set; }

          /// <summary>
          ///
          /// </summary>
          public string Ip { get; set; }

          /// <summary>
          /// Gets or sets the propietario.
          /// </summary>
          /// <value>
          /// The propietario.
          /// </value>
          public string Owner { get; set; }

          /// <summary>
          /// Gets or sets the prefijo.
          /// </summary>
          /// <value>
          /// The prefijo.
          /// </value>
          public string Prefix { get; set; }

          /// <summary>
          /// Gets or sets the posfijo object.
          /// </summary>
          /// <value>
          /// The posfijo object.
          /// </value>
          public string Postfix { get; set; }

          /// <summary>
          /// Gets or sets a value indicating whether [seguridad integrada].
          /// </summary>
          /// <value>
          ///   <c>true</c> if [seguridad integrada]; otherwise, <c>false</c>.
          /// </value>
          public bool IntegratedSecurity { get; set; }

          /// <summary>
          /// Gets or sets the nivel compatibilidad.
          /// </summary>
          /// <value>
          /// The nivel compatibilidad.
          /// </value>
          public int CompatibilityLevel { get; set; }

          /// <summary>
          /// Gets or sets the cadena conexion.
          /// </summary>
          /// <value>
          /// The cadena conexion.
          /// </value>
          public string StringConnection { get; set; }

          /// <summary>
          /// Tipo de motor de base de datos (1=Access,2=SQL)
          /// </summary>
          public Framework.Enumerations.DatabaseEngines Engine { get; set; }

          /// <summary>
          /// Prefijo para la las base datos
          /// </summary>
          public string DataBaseObjectPrefixLogData
          {
               get
               {
                    return "xsH";
               }
          }

          /// <summary>
          /// Gets or sets the parametros.
          /// </summary>
          /// <value>
          /// The parametros.
          /// </value>
          public ParametersConfiguration Parameters { get; set; }

          /// <summary>
          /// Gets or sets a value indicating whether [usa parametros SQL].
          /// </summary>
          /// <value>
          ///   <c>true</c> if [usa parametros SQL]; otherwise, <c>false</c>.
          /// </value>
          public bool UseSqlParameters { get; set; }

          /// <summary>
          /// Gets or sets a value indicating whether [existe conexion a la bd].
          /// </summary>
          /// <value>
          ///   <c>true</c> if [existe conexion bd]; otherwise, <c>false</c>.
          /// </value>
          public bool ExistsConnection { get; set; }

          #endregion Properties

          /// <summary>
          /// Initializes a new instance of the <see cref="DataBaseConfig"/> class.
          /// </summary>
          public DataBaseConfig()
          {
               Parameters = new ParametersConfiguration();
          }

          /// <summary>
          /// TableName
          /// </summary>
          /// <param name="tableName"></param>
          /// <param name="usePrefixObject"></param>
          /// <param name="isHistoryCatalog"></param>
          /// <param name="isHistoryTable"></param>
          /// <returns></returns>
          public string TableName(string tableName, bool usePrefixObject, bool isHistoryCatalog, bool isHistoryTable)
          {
               string catalogName;
               string localTableName;
               StringBuilder stringName;
               if (isHistoryCatalog)
                    catalogName = LogData;
               else
                    catalogName = Catalog;
               if (isHistoryTable)
                    localTableName = String.Format("{0}{1}{2}{3}", usePrefixObject ? Prefix : "", DataBaseObjectPrefixLogData, tableName, Convert.ToString(Postfix));
               else
               {
                    localTableName = String.Format("{0}{1}{2}", usePrefixObject ? Prefix : "", tableName, Convert.ToString(Postfix));
               }
               stringName = new StringBuilder();
               switch (Engine)
               {
                    case Framework.Enumerations.DatabaseEngines.SqlServer:
                         if (!String.IsNullOrEmpty(LinkedServer))
                              stringName.AppendFormat("[{0}].", LinkedServer);
                         stringName.AppendFormat("[{0}].[{1}].[{2}]", catalogName, Owner, localTableName);
                         break;
                    case DatabaseEngines.MySql:
                         stringName.AppendFormat("`{0}`", localTableName);
                         break;
                    case DatabaseEngines.PostgressSql:
                         stringName.AppendFormat("{0}.{1}", Owner, localTableName);
                         break;
                    default:
                         stringName.AppendFormat("[{0}].[{1}].[{2}]", catalogName, Owner, localTableName);
                         break;
               }
               return stringName.ToString();
          }

          /// <summary>
          /// Tables the name.
          /// </summary>
          /// <param name="tableName">Name of the table.</param>
          /// <param name="usePrefixObject">if set to <c>true</c> [use prefix object].</param>
          /// <param name="isHistoryCatalog">if set to <c>true</c> [is history catalog].</param>
          /// <returns></returns>
          public string TableName(string tableName, bool usePrefixObject, bool isHistoryCatalog)
          {
               return TableName(tableName, usePrefixObject, isHistoryCatalog, false);
          }

          /// <summary>
          /// Tables the name.
          /// </summary>
          /// <param name="tableName">Name of the table.</param>
          /// <param name="usePrefixObject">if set to <c>true</c> [use prefix object].</param>
          /// <returns></returns>
          public string TableName(string tableName, bool usePrefixObject)
          {
               return TableName(tableName, usePrefixObject, false, false);
          }

          /// <summary>
          /// Tables the name.
          /// </summary>
          /// <param name="tableName">Name of the table.</param>
          /// <returns></returns>
          public string TableName(string tableName)
          {
               return TableName(tableName, true, false, false);
          }

          /// <summary>
          /// CreateStringConection
          /// </summary>
          /// <returns></returns>
          public void CreateStringConection()
          {
               StringBuilder connection;
               if (!String.IsNullOrEmpty(StringConnection))
               {
                    GetValueFromStringConnection();
                    return;
               }
               connection = new StringBuilder();

               switch (Engine)
               {
                    case DatabaseEngines.SqlServer:
                         connection.AppendFormat("Persist Security Info=True");
                         connection.AppendFormat(";Initial Catalog={0}", Catalog);
                         connection.AppendFormat(";Data Source={0}", Server);
                         if (!string.IsNullOrEmpty(User) && !string.IsNullOrEmpty(Password))
                         {
                              connection.AppendFormat(";User id={0}", User);
                              connection.AppendFormat(";Password={0}", Password);
                         }
                         else
                         {
                              connection.AppendFormat(" ;Integrated Security=True ");
                         }
                         connection.AppendFormat(" ;MultipleActiveResultSets=True ");
                         break;

                    default:
                         connection.AppendFormat("Persist Security Info=True");
                         connection.AppendFormat(";Initial Catalog={0}", Catalog);
                         connection.AppendFormat(";Data Source={0}", Server);
                         if (!string.IsNullOrEmpty(User) && !string.IsNullOrEmpty(Password))
                         {
                              connection.AppendFormat(";User id={0}", User);
                              connection.AppendFormat(";Password={0}", Password);
                         }
                         else
                         {
                              connection.AppendFormat(" ;Integrated Security=True ");
                         }
                         connection.AppendFormat(" ;MultipleActiveResultSets=True ");
                         break;
               }
               StringConnection = connection.ToString();
          }

          /// <summary>
          /// Obtienes the valores cadena conexion.
          /// </summary>
          public void GetValueFromStringConnection()
          {
               SqlConnectionStringBuilder sqlConnection;
               MySqlConnectionStringBuilder mySqlConnection;
               NpgsqlConnectionStringBuilder NpsqlConnection;
               switch (Engine)
               {
                    case DatabaseEngines.SqlServer:
                         sqlConnection = new SqlConnectionStringBuilder(StringConnection);
                         if (String.IsNullOrEmpty(Catalog))
                              Catalog = sqlConnection.InitialCatalog;
                         if (String.IsNullOrEmpty(User))
                              User = sqlConnection.UserID;
                         if (String.IsNullOrEmpty(Password))
                              Password = sqlConnection.Password;
                         if (String.IsNullOrEmpty(Server))
                              Server = sqlConnection.DataSource;
                         break;
                    case DatabaseEngines.MySql:
                         mySqlConnection = new MySqlConnectionStringBuilder(StringConnection);
                         if (String.IsNullOrEmpty(Catalog))
                              Catalog = mySqlConnection.Database;
                         if (String.IsNullOrEmpty(User))
                              User = mySqlConnection.UserID;
                         if (String.IsNullOrEmpty(Password))
                              Password = mySqlConnection.Password;
                         if (String.IsNullOrEmpty(Server))
                              Server = mySqlConnection.Server;
                         break;
                    case DatabaseEngines.PostgressSql:
                         NpsqlConnection = new NpgsqlConnectionStringBuilder(StringConnection);
                         if (String.IsNullOrEmpty(Catalog))
                              Catalog = NpsqlConnection.Database;
                         if (String.IsNullOrEmpty(User))
                              User = NpsqlConnection.Username;
                         if (String.IsNullOrEmpty(Password))
                              Password = NpsqlConnection.Password;
                         if (String.IsNullOrEmpty(Server))
                              Server = NpsqlConnection.Host;
                         break;
                    default:
                         sqlConnection = new SqlConnectionStringBuilder(StringConnection);
                         if (String.IsNullOrEmpty(Catalog))
                              Catalog = sqlConnection.InitialCatalog;
                         if (String.IsNullOrEmpty(User))
                              User = sqlConnection.UserID;
                         if (String.IsNullOrEmpty(Password))
                              Password = sqlConnection.Password;
                         if (String.IsNullOrEmpty(Server))
                              Server = sqlConnection.DataSource;
                         break;
               }
          }
     }
}