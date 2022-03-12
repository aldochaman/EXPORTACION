using Framework.DataBase.Utilities;
using Framework.Enumerations;
using Framework.Extensions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
namespace Framework.DataBase
{
    /// <summary>
    /// Clase para el manejo de la conexion a la base de datos
    /// </summary>
    [Serializable]
     public class DataAccess
     {
          #region Variables

          private const string _LogTable = "XSLOGS";

          /// <summary>
          /// Determina si existe la tabla para el manejo de errores
          /// </summary>
          private bool _existsErrorInLogData;

          /// <summary>
          /// Para grabar el error
          /// _bGrabandoError
          /// </summary>
          private bool _writeError;

          /// <summary>
          /// The _o parametros SQL
          /// </summary>
          private Dictionary<string, object> _sqlParameters;

          /// <summary>
          /// The _o parameter SQL
          /// </summary>
          private List<ParameterSql> _sqlParametersList;

          
          #endregion Variables

          #region Properties
          /// <summary>
          /// DataBase
          /// </summary>
          public DataBaseConfig DataBase { get; set; }
          #endregion Properties

          #region Builders
          public DataAccess()
          {
               _sqlParameters = new Dictionary<string, object>();
               _sqlParametersList = new List<ParameterSql>();
               _writeError = false;
               _existsErrorInLogData = false;
               DataBase = new DataBaseConfig();
          }
         

          /// <summary>
          /// Initializes a new instance of the <see cref="DataAccess"/> class.
          /// </summary>
          /// <param name="dataBase">The data base.</param>
          public DataAccess(DataBaseConfig dataBase)
          {
               _sqlParameters = new Dictionary<string, object>();
               _writeError = false;
               _existsErrorInLogData = false;
               DataBase = dataBase;
               DataBase.Owner = "dbo";
               DataBase.UseSqlParameters = false;
               DataBase.CreateStringConection();
               ExistConnectionToDataBase();
               ValidateIfExistErrorsTable();
               ValidateIfExistLogErrorDataBase();
          }

          /// <summary>
          /// Initializes a new instance of the <see cref="DataAccess"/> class.
          /// </summary>
          /// <param name="server">The server.</param>
          /// <param name="dataBaseName">Name of the data base.</param>
          public DataAccess(string server, string dataBaseName)
          {
               string stringConnection = string.Format("Data Source={0};Initial Catalog={1};Integrated Security=Yes;", server, dataBaseName);
               InternalBuilder(stringConnection, "", DatabaseEngines.SqlServer);
               DataBase.Owner = "dbo";
               ValidateIfExistErrorsTable();
               ValidateIfExistLogErrorDataBase();
          }

          /// <summary>
          /// Initializes a new instance of the <see cref="DataAccess"/> class.
          /// </summary>
          /// <param name="server">The server.</param>
          /// <param name="user">The user.</param>
          /// <param name="password">The password.</param>
          public DataAccess(string server, string user, string password)
          {
               string stringConnection = string.Format("Data Source={0};Initial Catalog=Master;User ID={1};Password={2}; ", server, user, password);
               InternalBuilder(stringConnection, "", DatabaseEngines.SqlServer);
               DataBase.Owner = "dbo";
               ValidateIfExistErrorsTable();
               ValidateIfExistLogErrorDataBase();
          }

          /// <summary>
          /// Initializes a new instance of the <see cref="DataAccess"/> class.
          /// </summary>
          /// <param name="server">The server.</param>
          /// <param name="dataBaseName">Name of the data base.</param>
          /// <param name="user">The user.</param>
          /// <param name="password">The password.</param>
          public DataAccess(string server, string dataBaseName, string user, string password)
          {
               string stringConnection = string.Format("Data Source={0};Initial Catalog={1};User ID={2};Password={3}; ", server, dataBaseName, user, password);
               InternalBuilder(stringConnection, "", DatabaseEngines.SqlServer);
               DataBase.Owner = "dbo";
               ValidateIfExistErrorsTable();
               ValidateIfExistLogErrorDataBase();
          }

          /// <summary>
          /// Initializes a new instance of the <see cref="DataAccess"/> class.
          /// </summary>
          /// <param name="stringConection">The string conection.</param>
          /// <param name="logDataBDName">Name of the log data bd.</param>
          /// <param name="engine">The engine.</param>
          public DataAccess(string stringConection, string logDataBDName, Framework.Enumerations.DatabaseEngines engine)
          {
               InternalBuilder(stringConection, logDataBDName, engine);
               ValidateIfExistErrorsTable();
               ValidateIfExistLogErrorDataBase();
          }

          /// <summary>
          /// Initializes a new instance of the <see cref="DataAccess"/> class.
          /// </summary>
          /// <param name="stringConnection">The URI.</param>
          public DataAccess(string stringConnection)
          {
               InternalBuilder(stringConnection, "", Framework.Enumerations.DatabaseEngines.SqlServer);
               ValidateIfExistErrorsTable();
               ValidateIfExistLogErrorDataBase();
          }

          /// <summary>
          /// ValidateIfExistErrorsTable
          /// </summary>
          private void ValidateIfExistErrorsTable()
          {
               if (!DataBase.ExistsConnection)
               {
                    return;
               }
               try
               {
                    if (ExistsObjectInDataBase(_LogTable, "", DataBase.LogData))
                    {
                         _existsErrorInLogData = true;
                    }
                    else
                    {                         
                         CreateLogBookTableForErrors();
                         _existsErrorInLogData = true;
                    }

               }
               catch
               {
                    //TODO: si hay una excepcion al momento de isntanciar la clase, encontrar una mejor manera de manejar este error
               }
          }

          private void InitilizeColections()
          {
               _sqlParametersList = new List<ParameterSql>();
               _sqlParameters = new Dictionary<string, object>();
          }

          private void ValidateIfExistLogErrorDataBase()
          {
               if (String.IsNullOrEmpty(DataBase.LogData))
                    return;
               if (ExistsDataBase(DataBase.LogData))
                    return;
               CreateDataBase(DataBase.LogData);
          }

          /// <summary>
          /// Constructors the interno.
          /// </summary>
          /// <param name="stringConnection">The ps cad conexion.</param>
          /// <param name="psBDBitacora">The ps bd bitacora.</param>
          /// <param name="engine">The pen motor.</param>
          private void InternalBuilder(string stringConnection, string psBDBitacora, Framework.Enumerations.DatabaseEngines engine)
          {
               _writeError = false;
               _existsErrorInLogData = false;
               DataBase = new DataBaseConfig
               {
                    LogData = psBDBitacora,
                    Engine = engine,
                    UseSqlParameters = false,
                    StringConnection = stringConnection
               };
               _sqlParameters = new Dictionary<string, object>();
               if (engine == DatabaseEngines.SqlServer)
                    LoadServerVariables(stringConnection);
               ExistConnectionToDataBase();
          }

          #endregion Builders

          #region Generics

          #region GenericTable

          /// <summary>
          /// Function for get a generic table object with a query object, this object contains the query string and parameter list
          /// </summary>
          /// <param name="tableName"></param>
          /// <param name="query"></param>
          /// <returns></returns>
          public GenericTable GetGenericTable(string tableName, StringBuilder query)
          {
               GenericTable genericTable;
               using (DataTable dataTable = GetDataTable(query, tableName))
               {
                    genericTable = new GenericTable(tableName);
                    genericTable.ConvertDataTable2List2(dataTable, query.ToString());
                    return genericTable;
               }
          }

          /// <summary>
          /// Function for get a generic table object, this function use a query string and parameter list
          /// </summary>
          /// <param name="tableName"></param>
          /// <param name="query"></param>
          /// <param name="parameters"></param>
          /// <returns></returns>
          public GenericTable GetGenericTable(string tableName, StringBuilder query, IEnumerable<ParameterSql> parameters)
          {
               GenericTable genericTable;
               using (System.Data.DataTable dataTable = GetDataTable(query, parameters))
               {
                    genericTable = new GenericTable(tableName);
                    genericTable.ConvertDataTable2List2(dataTable, query.ToString());
                    return genericTable;
               }
          }

          /// <summary>
          /// Gets the generic table reader.
          /// </summary>
          /// <param name="tableName">Name of the table.</param>
          /// <param name="query">The query.</param>
          /// <returns></returns>
          public GenericTable GetGenericTableReader(string tableName, StringBuilder query)
          {
               return GetGenericTableReader(tableName, query, null);
          }

          /// <summary>
          /// Gets the generic table data record reader.
          /// </summary>
          /// <param name="query">The query.</param>
          /// <param name="IEnumParameters">The i enum parameters.</param>
          /// <returns></returns>
          public GenericTableRow GetGenericTableDataRecordReader(StringBuilder query, IEnumerable<ParameterSql> IEnumParameters)
          {
               return GetGenericTableDataRecordReader(query, IEnumParameters, "Datas");
          }

          /// <summary>
          /// Gets the generic table data record reader.
          /// </summary>
          /// <param name="query">The query.</param>
          /// <param name="table">The table.</param>
          /// <param name="IEnumParameters">The i enum parameters.</param>
          /// <returns></returns>
          public GenericTableRow GetGenericTableDataRecordReader(StringBuilder query, string table, IEnumerable<ParameterSql> IEnumParameters)
          {
               return GetGenericTableDataRecordReader(query, IEnumParameters, table);
          }

          /// <summary>
          /// Gets the generic table data record reader.
          /// </summary>
          /// <param name="query">The query.</param>
          /// <returns></returns>
          public GenericTableRow GetGenericTableDataRecordReader(StringBuilder query)
          {
               return GetGenericTableDataRecordReader(query, null, "Datas");
          }

          /// <summary>
          /// Gets the generic table data record reader.
          /// </summary>
          /// <param name="query">The query.</param>
          /// <param name="table">The table.</param>
          /// <returns></returns>
          public GenericTableRow GetGenericTableDataRecordReader(StringBuilder query, string table)
          {
               return GetGenericTableDataRecordReader(query, null, table);
          }

          /// <summary>
          /// Gets the generic table data record reader.
          /// </summary>
          /// <param name="query">The query.</param>
          /// <param name="IEnumSqlParameters">The i enum SQL parameters.</param>
          /// <param name="table">The table.</param>
          /// <returns></returns>
          private GenericTableRow GetGenericTableDataRecordReader(StringBuilder query, IEnumerable<ParameterSql> IEnumSqlParameters, string table)
          {
               try
               {
                    GenericTableRow genericTable = new GenericTableRow(table);
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         using (IDataReader dataReader = HandlesConnection.FillDataReader(query, IEnumSqlParameters))
                         {
                              genericTable.CreateGenericTableFromQuery(dataReader, query);
                              return genericTable;
                         }
                    }
               }
               finally
               {
                    InitilizeColections();
               }
          }

          /// <summary>
          /// Gets the generic table reader.
          /// </summary>
          /// <param name="tableName">Name of the table.</param>
          /// <param name="query">The query.</param>
          /// <param name="IEnumSqlParameters">The i enum SQL parameters.</param>
          /// <returns></returns>
          public GenericTable GetGenericTableReader(string tableName, StringBuilder query, IEnumerable<ParameterSql> IEnumSqlParameters)
          {
               try
               {
                    GenericTable genericTable = new GenericTable(tableName);
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         using (IDataReader dataReader = HandlesConnection.FillDataReader(query, IEnumSqlParameters))
                         {
                              genericTable.CreateGenericTableFromQuery(dataReader, query);
                              return genericTable;
                         }
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
                    throw;
               }
               finally
               {
                    InitilizeColections();
               }
          }

          #endregion GenericTable

          #region GenericTableRow

          /// <summary>
          /// Regresas the tabla generica registro.
          /// </summary>
          /// <param name="query">The ps query.</param>
          /// <param name="table">The ps tabla.</param>
          /// <returns></returns>
          public GenericTableRow ReturnGenericTableRow(StringBuilder query, string table)
          {
               GenericTableRow genericTableRow;
               using (DataTable dataTable = GetDataTable(query, table))
               {
                    genericTableRow = new GenericTableRow(table);
                    genericTableRow.ConvertDataTable2List2(dataTable, query.ToString());
                    return genericTableRow;
               }
          }

          /// <summary>
          /// Gets the generic table row.
          /// </summary>
          /// <param name="query">The query.</param>
          /// <param name="table">The table.</param>
          /// <param name="IEnumSqlParameters">The i enum SQL parameters.</param>
          /// <returns></returns>
          public GenericTableRow GetGenericTableRow(StringBuilder query, string table, IEnumerable<ParameterSql> IEnumSqlParameters)
          {
               GenericTableRow genericTable;
               using (DataTable dataTable = GetDataTable(query, IEnumSqlParameters))
               {
                    genericTable = new GenericTableRow(table);
                    genericTable.ConvertDataTable2List2(dataTable, query.ToString());
                    return genericTable;
               }
          }

          #endregion GenericTableRow

          /// <summary>
          /// Funcion que regresa una coleccion generica List de una clase usando internamente un datatable
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="query">Consulta.</param>
          /// <returns></returns>
          public List<T> GetGenericListData<T>(StringBuilder query) where T : class, new()
          {
               DataTable dataTable;

               dataTable = GetDataTable(query, "TablaGenerica");
               return dataTable.ToEnumerable<T>().ToList();
          }

          /// <summary>
          /// Regresas the lista generica datos.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="query">The ps query.</param>
          /// <param name="IEnumSqlParameters">The po parametros.</param>
          /// <returns></returns>
          public List<T> GetGenericListData<T>(StringBuilder query, IEnumerable<ParameterSql> IEnumSqlParameters) where T : class, new()
          {
               DataTable dataTable;

               dataTable = GetDataTable(query, IEnumSqlParameters);
               return dataTable.ToEnumerable<T>().ToList();
          }

          /// <summary>
          /// Regresa una coleccion HashSet en base a un campo especifico de la consulta.
          /// Esto permite tener una coleccion en base a un solo campo y sin que los campos se repitan
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="query">Consulta.</param>
          /// <param name="field">Campo.</param>
          /// <returns></returns>
          public HashSet<T> GetGenericFieldBasedCollection<T>(StringBuilder query, string field)
          {
               return GetGenericFieldBasedList<T>(query, field).ToHash<T>();
          }

          /// <summary>
          /// Regresas the coleccion generica en base a campo.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="query">The ps query.</param>
          /// <param name="field">The ps campo.</param>
          /// <param name="IEnumSqlParameters">The po parametros.</param>
          /// <returns></returns>
          public HashSet<T> GetGenericFieldBasedCollection<T>(StringBuilder query, string field, IEnumerable<ParameterSql> IEnumSqlParameters)
          {
               return GetGenericFieldBasedList<T>(query, field, IEnumSqlParameters).ToHash<T>();
          }

          /// <summary>
          /// Regresa una coleccion HashSet en base a un campo especifico de la consulta.
          /// Esto permite tener una coleccion en base a un solo campo y que los campos se repitan
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="query">Consulta.</param>
          /// <param name="field">Campo.</param>
          /// <returns></returns>
          public List<T> GetGenericFieldBasedList<T>(StringBuilder query, string field)
          {
               if (String.IsNullOrEmpty(field))
               {
                    return null;
               }
               using (DataTable dataTable = GetDataTable(query, "Tabla"))
               {
                    if (!dataTable.Columns.Contains(field))
                    {
                         return null;
                    }
                    return dataTable.AsEnumerable().Select(s => s.Field<T>(field)).ToList();
               }
          }

          /// <summary>
          /// Regresas the lista generica en base a campo.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="query">The ps query.</param>
          /// <param name="field">The ps campo.</param>
          /// <param name="IEnumSqlParameters">The po parametros.</param>
          /// <returns></returns>
          public List<T> GetGenericFieldBasedList<T>(StringBuilder query, string field, IEnumerable<ParameterSql> IEnumSqlParameters)
          {
               if (String.IsNullOrEmpty(field))
               {
                    return null;
               }
               using (DataTable dataTable = GetDataTable(query, IEnumSqlParameters))
               {
                    if (!dataTable.Columns.Contains(field))
                    {
                         return null;
                    }
                    return dataTable.AsEnumerable().Select(s => s.Field<T>(field)).ToList();
               }
          }

          /// <summary>
          /// Funcion que regresa una coleccion generica HashSet de una clase usando internamente un datatable
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="query">Consulta.</param>
          /// <returns></returns>
          public HashSet<T> GetGenericCollectionData<T>(StringBuilder query) where T : class, new()
          {
               DataTable dataTable;
               dataTable = GetDataTable(query, "TablaGenerica");
               return dataTable.ToEnumerable<T>().ToHash();
          }

          /// <summary>
          /// Regresas the coleccion generica datos2.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="query">The ps query.</param>
          /// <returns></returns>
          public HashSet<T> GetGenericCollectionDataReader<T>(StringBuilder query) where T : class, new()
          {
               return GetGenericCollectionDataReader<T>(query, null);
          }

          /// <summary>
          /// Regresas the coleccion generica datos data reader.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="query">The ps query.</param>
          /// <param name="IEnumSqlParameters">The po parametros.</param>
          /// <returns></returns>
          private HashSet<T> GetGenericCollectionDataReader<T>(StringBuilder query, IEnumerable<ParameterSql> IEnumSqlParameters) where T : class, new()
          {
               const System.Reflection.BindingFlags bindingFlags = System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance;
               T loInstanceOfT;
               HashSet<T> hashSet = new HashSet<T>();
               IEnumerable<string> columnNames;
               IEnumerable<PropertyInfo> infoProperties;
               try
               {
                    using (IDataReader dataReader = HandlesConnection.FillDataReader(query, IEnumSqlParameters))
                    {
                         //var columnNames = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();
                         columnNames = dataReader.GetSchemaTable()
                                                 .Rows
                                                 .Cast<DataRow>()
                                                 .Select(r => Convert.ToString(r["ColumnName"]).ToLower());
                         infoProperties = typeof(T).GetProperties(bindingFlags);
                         while (dataReader.Read())
                         {
                              //loInstanceOfT = Activator.CreateInstance<T>();
                              //Estes es mas rapido
                              loInstanceOfT = new T();
                              foreach (PropertyInfo loProperty in infoProperties.Where(properties => columnNames.Contains(properties.Name.ToLower())))
                              {
                                   loProperty.SetValue(loInstanceOfT, Framework.DataBase.Utilities.Extensions.ChangeType(dataReader[loProperty.Name], loProperty.PropertyType), null);
                              }
                              hashSet.Add(loInstanceOfT);
                         }
                    }
                    return hashSet;
               }
               finally
               {
                    InitilizeColections();
               }
          }

          /// <summary>
          /// Regresas the coleccion generica datos.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="query">The ps query.</param>
          /// <param name="IEnumSqlParameters">The po parametros.</param>
          /// <returns></returns>
          public HashSet<T> GetGenericCollectionData<T>(StringBuilder query, IEnumerable<ParameterSql> IEnumSqlParameters) where T : class, new()
          {
               DataTable dataTable = GetDataTable(query, IEnumSqlParameters);
               return dataTable.ToEnumerable<T>().ToHash();
          }

          #endregion Generics

          #region DataReader

          /// <summary>
          /// Gets the data reader.
          /// </summary>
          /// <param name="query">The query.</param>
          /// <returns></returns>
          public IDataReader GetDataReader(StringBuilder query)
          {
               try
               {
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         return HandlesConnection.FillDataReader(query, null);
                    }
               }
               finally
               {
                    InitilizeColections();
               }
          }

          /// <summary>
          /// Gets the data reader.
          /// </summary>
          /// <param name="query">The query.</param>
          /// <param name="IEnumSqlParameters">The i enum SQL parameters.</param>
          /// <returns></returns>
          public IDataReader GetDataReader(StringBuilder query, IEnumerable<ParameterSql> IEnumSqlParameters)
          {
               try
               {
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         return HandlesConnection.FillDataReader(query, IEnumSqlParameters);
                    }
               }
               finally
               {
                    InitilizeColections();
               }
          }

          #endregion DataReader

          #region Dataset

          /// <summary>
          /// Funcion que regresa dataset
          /// </summary>
          /// <param name="query">Consutla</param>
          /// <param name="table">NOmbre de la tabla</param>
          /// <returns></returns>
          public System.Data.DataSet GetDataSet(StringBuilder query, string table)
          {
               try
               {
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         return HandlesConnection.FillDataset(query, table);
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
                    throw;
               }
               finally
               {
                    InitilizeColections();
               }
          }

          #endregion Dataset

          #region Dataview

          /// <summary>
          /// Get data view object
          /// </summary>
          /// <param name="query"></param>
          /// <returns>Use the default table name "Table"</returns>
          public System.Data.DataView GetDataView(StringBuilder query)
          {
               DataTable dataSetResult;
               try
               {
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         dataSetResult = HandlesConnection.FillDataTable(query, "Table");
                    }
                    return new DataView(dataSetResult);
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
                    throw;
               }
               finally
               {
                    InitilizeColections();
               }
          }

          /// <summary>
          /// Get data view object with parameters for query
          /// </summary>
          /// <param name="query"></param>
          /// <param name="IEnumSqlParams"></param>
          /// <returns></returns>
          public System.Data.DataView GetDataView(StringBuilder query, IEnumerable<ParameterSql> IEnumSqlParams)
          {
               DataTable dataSetResult;
               try
               {
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         if (DataBase.UseSqlParameters)
                         {
                              CreateSqlParameters();
                              dataSetResult = HandlesConnection.FillDataTable(query, _sqlParametersList);
                         }
                         else
                         {
                              dataSetResult = HandlesConnection.FillDataTable(query, IEnumSqlParams);
                         }
                    }
                    return new DataView(dataSetResult);
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
                    throw;
               }
               finally
               {
                    InitilizeColections();
               }
          }

          /// <summary>
          /// Gets the data view.
          /// </summary>
          /// <param name="table">The table.</param>
          /// <param name="query">The query.</param>
          /// <returns></returns>
          public System.Data.DataView GetDataView(string table, StringBuilder query)
          {
               DataTable dataSetResult;
               try
               {
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         dataSetResult = HandlesConnection.FillDataTable(query, table);
                    }
                    return new DataView(dataSetResult);
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
                    throw;
               }
               finally
               {
                    InitilizeColections();
               }
          }

          #endregion Dataview

          #region Datatable

          /// <summary>
          /// Get data table from query.
          /// </summary>
          /// <param name="query">The ps query.</param>
          /// <returns>Use the default table name "Table"</returns>
          public System.Data.DataTable GetDataTable(StringBuilder query)
          {
               try
               {
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         if (DataBase.UseSqlParameters)
                         {
                              CreateSqlParameters();
                              return HandlesConnection.FillDataTable(query, _sqlParametersList);
                         }
                         return HandlesConnection.FillDataTable(query, "Table");
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
                    throw;
               }
               finally
               {
                    InitilizeColections();
               }
          }

          /// <summary>
          /// Get data table from query and custom table  name
          /// </summary>
          /// <param name="query">Consutla</param>
          /// <param name="table">NOmbre de la tabla</param>
          /// <returns></returns>
          public System.Data.DataTable GetDataTable(StringBuilder query, string table)
          {
               try
               {
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         if (DataBase.UseSqlParameters)
                         {
                              CreateSqlParameters();
                              return HandlesConnection.FillDataTable(query, _sqlParametersList);
                         }
                         return HandlesConnection.FillDataTable(query, table);
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
                    throw;
               }
               finally
               {
                    InitilizeColections();
               }
          }

          /// <summary>
          /// Get data table from query and sql parameter list
          /// </summary>
          /// <param name="query">The ps query.</param>
          /// <param name="IEnumSqlParams">The po parametros.</param>
          /// <returns></returns>
          public System.Data.DataTable GetDataTable(StringBuilder query, IEnumerable<ParameterSql> IEnumSqlParams)
          {
               try
               {
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         if (DataBase.UseSqlParameters)
                         {
                              CreateSqlParameters();
                              return HandlesConnection.FillDataTable(query, _sqlParametersList);
                         }
                         return HandlesConnection.FillDataTable(query, IEnumSqlParams);
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
                    throw;
               }
               finally
               {
                    InitilizeColections();
               }
          }


          #endregion Datatable

          #region Exec command

          /// <summary>
          /// ExecuteCommand
          /// </summary>
          /// <param name="query"></param>
          /// <param name="parameters"></param>
          /// <returns></returns>
          public int ExecuteCommand(StringBuilder query, IEnumerable<ParameterSql> parameters)
          {
               return ExecuteCommand(query, parameters, true);
          }

          /// <summary>
          /// Funcionq ue ejecuta un comando usando una lista de parametros en la bas de datos
          /// </summary>
          /// <param name="psQuery">Consulta</param>
          /// <param name="parameters">Lista de parametros</param>
          /// <param name="unlimited">Permite hacer que la conexion no termine antes de ejecutar el comando </param>
          public int ExecuteCommand(StringBuilder psQuery, IEnumerable<ParameterSql> parameters, bool unlimited)
          {
               try
               {
                    using (var loConexion = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         if (DataBase.UseSqlParameters)
                         {
                              CreateSqlParameters();
                              return HandlesConnection.ExecuteNonQuery(psQuery, _sqlParametersList, unlimited);
                         }
                         return HandlesConnection.ExecuteNonQuery(psQuery, parameters, unlimited);
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, psQuery);
                    throw;
               }
               finally
               {
                    InitilizeColections();
               }
          }

          /// <summary>
          /// ExecuteCommand
          /// </summary>
          /// <param name="query"></param>
          /// <returns></returns>

          public int ExecuteCommand(StringBuilder query)
          {
               return ExecuteCommand(query, null, true);
          }

          #endregion Exec command

          #region Execute

          /// <summary>
          /// Validas the consulta.
          /// </summary>
          /// <param name="query">The ps query.</param>
          /// <returns></returns>
          public string ValidateQuery(StringBuilder query)
          {
               StringBuilder localQuery;
               try
               {
                    localQuery = new StringBuilder();
                    localQuery.AppendFormat("SET NOEXEC ON");
                    localQuery.AppendFormat(" {0} ", query);
                    localQuery.AppendFormat("SET NOEXEC OFF");
                    return Convert.ToString(ExecuteScalar(localQuery));
               }
               catch (Exception ex)
               {
                    return ex.Message;
               }
          }

          /// <summary>
          /// Ejecutas the escalar.
          /// </summary>
          /// <param name="query">The ps query.</param>
          /// <param name="IEnmSqlParameters">The po parametros.</param>
          /// <returns></returns>
          public object ExecuteScalar(StringBuilder query, IEnumerable<ParameterSql> IEnmSqlParameters)
          {
               try
               {
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         if (DataBase.UseSqlParameters)
                         {
                              CreateSqlParameters();
                              return HandlesConnection.ExecuteScalar(query, _sqlParametersList);
                         }
                         return HandlesConnection.ExecuteScalar(query, IEnmSqlParameters);
                    }
               }
               catch (Exception ex)
               {
                    //if (ex.HResult != -2146232060 && !( ex.ToString().IndexOf("PRIMAR") > 0 ))
                    if (!(ex.ToString().IndexOf("PRIMAR") > 0))
                    {
                         //Se pone condion para evitar mandar un mensaje cuando se repita la llave duplicada
                         //esto es por la concurrencia en la bd al momento de insertar registros.
                         RecordLog(ex, query);
                         throw;
                    }
                    return null;
               }
               finally
               {
                    InitilizeColections();
               }
          }

          /// <summary>
          /// Funcion que ejecuta una consutla en la base de datos y retorna un valor
          /// </summary>
          /// <param name="query">Consulta</param>
          /// <returns></returns>
          public object ExecuteScalar(StringBuilder query)
          {
               return ExecuteScalar(query, null);
          }

          /// <summary>
          /// Executes the scalar.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="query">The query.</param>
          /// <returns></returns>
          public T ExecuteScalar<T>(StringBuilder query)
          {
               return ExecuteScalar<T>(query, null);
          }

          /// <summary>
          /// Ejecutas the escalar.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="query">The ps query.</param>
          /// <param name="IEnmSqlParameters">The po parametros.</param>
          /// <returns></returns>
          public T ExecuteScalar<T>(StringBuilder query, IEnumerable<ParameterSql> IEnmSqlParameters)
          {
               object result;
               Type nativeDataType = typeof(T);
               try
               {
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         if (DataBase.UseSqlParameters)
                         {
                              CreateSqlParameters();
                              result = HandlesConnection.ExecuteScalar(query, _sqlParametersList);
                         }
                         else
                         {
                              result = HandlesConnection.ExecuteScalar(query, IEnmSqlParameters);
                         }
                    }
                    if (Object.Equals(result, DBNull.Value) || result?.Equals("") != false)
                    {
                         return ObjectsUtilities.GetDefaultValue<T>();
                    }
                    else
                    {
                         return (T)Convert.ChangeType(result, nativeDataType);
                    }
               }
               catch (Exception ex)
               {
                    if (ex.HResult != -2146232060 && !(ex.ToString().IndexOf("PRIMAR") > 0))
                    {
                         //Se pone condion para evitar mandar un mensaje cuando se repita la llave duplicada
                         //esto es por la concurrencia en la bd al momento de insertar registros.
                         RecordLog(ex, query);
                         throw;
                    }
                    return ObjectsUtilities.GetDefaultValue<T>();
               }
               finally
               {
                    InitilizeColections();
               }
          }

          #endregion Execute

          #region Execute store procedures

          /// <summary>
          /// Funciopn que ejecuta un scalar SP
          /// </summary>
          /// <param name="storeName">Nombre del SP</param>
          /// <param name="IEnmSqlParameters">Parametros del SP</param>
          /// <returns></returns>
          public object StoreProcedureExecuteScalar(string storeName, IEnumerable<ParameterSql> IEnmSqlParameters)
          {
               try
               {
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         if (DataBase.UseSqlParameters)
                         {
                              CreateSqlParameters();
                              return HandlesConnection.StoreProcedureExecuteScalar(storeName, _sqlParametersList);
                         }
                         return HandlesConnection.StoreProcedureExecuteScalar(storeName, IEnmSqlParameters);
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, new StringBuilder(storeName));
                    throw;
               }
               finally
               {
                    InitilizeColections();
               }
          }

          /// <summary>
          /// Ejecutas the escalar procedimiento almacenado.
          /// </summary>
          /// <param name="storeName">The ps nombre.</param>
          /// <returns></returns>
          public object StoreProcedureExecuteScalar(string storeName)
          {
               return StoreProcedureExecuteScalar(storeName, null);
          }

          /// <summary>
          /// Stores the procedure execute scalar.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="storeName">Name of the store.</param>
          /// <param name="IEnmSqlParameters">The i enm SQL parameters.</param>
          /// <returns></returns>
          public T StoreProcedureExecuteScalar<T>(string storeName, IEnumerable<ParameterSql> IEnmSqlParameters)
          {
               object result;
               Type nativeDataType = typeof(T);
               try
               {
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         if (DataBase.UseSqlParameters)
                         {
                              CreateSqlParameters();
                              result = HandlesConnection.StoreProcedureExecuteScalar(storeName, _sqlParametersList);
                         }
                         else
                         {
                              result = HandlesConnection.StoreProcedureExecuteScalar(storeName, IEnmSqlParameters);
                         }
                    }
                    if (Object.Equals(result, DBNull.Value) || result?.Equals("") != false)
                    {
                         return ObjectsUtilities.GetDefaultValue<T>();
                    }
                    else
                    {
                         return (T)Convert.ChangeType(result, nativeDataType);
                    }
               }
               catch (Exception ex)
               {
                    if (ex.HResult != -2146232060 && !(ex.ToString().IndexOf("PRIMAR") > 0))
                    {
                         //Se pone condion para evitar mandar un mensaje cuando se repita la llave duplicada
                         //esto es por la concurrencia en la bd al momento de insertar registros.
                         RecordLog(ex, storeName);
                         throw;
                    }
                    return ObjectsUtilities.GetDefaultValue<T>();
               }
               finally
               {
                    InitilizeColections();
               }
          }

          /// <summary>
          /// Stores the procedure execute scalar.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="storeName">Name of the store.</param>
          /// <returns></returns>
          public T StoreProcedureExecuteScalar<T>(string storeName)
          {
               return StoreProcedureExecuteScalar<T>(storeName, null);
          }

          #endregion Execute store procedures

          #region BulkCopuy

          /// <summary>
          /// Inserts the bulk copy.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="IEnmTargetTable">The po tabla destino.</param>
          /// <param name="targetTableName">The ps tabla destino.</param>
          public void InsertBulkCopy<T>(IEnumerable<T> IEnmTargetTable, string targetTableName)
          {
               using (DataTable dataTable = IEnmTargetTable.ToDataTable())
               {
                    InsertBulkCopy(dataTable, targetTableName);
               }
          }

          /// <summary>
          /// Inserts the bulk copy.
          /// </summary>
          /// <param name="dataOriginTable">The PDS tabla origen.</param>
          /// <param name="targetTableName">The ps tabla destino.</param>
          public void InsertBulkCopy(DataSet dataOriginTable, string targetTableName)
          {
               InsertBulkCopy(dataOriginTable.Tables[0], targetTableName);
          }

          /// <summary>
          /// Inserts the bulk copy.
          /// </summary>
          /// <param name="dataOriginTable">The PDV tabla origen.</param>
          /// <param name="targetTableName">The ps tabla destino.</param>
          public void InsertBulkCopy(DataView dataOriginTable, string targetTableName)
          {
               InsertBulkCopy(dataOriginTable.ToTable(), targetTableName);
          }

          /// <summary>
          /// Inserts the bulk copy.
          /// </summary>
          /// <param name="dataOriginTable">The PDT tabla origen.</param>
          /// <param name="targetTableName">The ps tabla destino.</param>
          public void InsertBulkCopy(DataTable dataOriginTable, string targetTableName)
          {
               try
               {
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         HandlesConnection.InsertBulkCopy(dataOriginTable, targetTableName);
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, new StringBuilder(string.Format("Error InsertBuilCopy{0}", targetTableName)));
                    throw;
               }
               finally
               {
                    InitilizeColections();
               }
          }

          /// <summary>
          /// Inserts the bulk data RPT.
          /// </summary>
          /// <param name="dataOriginTable">The data origin table.</param>
          /// <param name="targetTableName">Name of the target table.</param>
          public void InsertBulkDataRpt(DataTable dataOriginTable, string targetTableName)
          {
               StringBuilder lsQueryHead;
               StringBuilder lsQueryValues;
               int liNumeroColumnas;

               liNumeroColumnas = dataOriginTable.Columns.Count;

               lsQueryHead = new StringBuilder();
               lsQueryHead.AppendFormat("INSERT INTO {0}(", TableName(targetTableName));
               foreach (DataColumn dataColumn in dataOriginTable.Columns)
               {
                    lsQueryHead.AppendFormat("{0},", dataColumn.ColumnName.ToUpper());
               }
               lsQueryHead.Remove(lsQueryHead.Length - 1, 1);
               lsQueryHead.Append(") VALUES (");

               foreach (DataRow dataRow in dataOriginTable.Rows)
               {
                    lsQueryValues = new StringBuilder();
                    lsQueryValues.AppendFormat("{0},", dataRow[0]);
                    lsQueryValues.AppendFormat("'{0}',", dataRow[1]);
                    lsQueryValues.AppendFormat("{0},", dataRow[2]);
                    lsQueryValues.AppendFormat("'{0}',", dataRow[3]);
                    for (int piIndex = 4; piIndex < liNumeroColumnas; piIndex++)
                    {
                         if (dataRow[piIndex].GetType().ToString() == "System.DBNull")
                         {
                              lsQueryValues.Append("NULL,");
                         }
                         else
                         {
                              lsQueryValues.AppendFormat("'{0}',", dataRow[piIndex]);
                         }
                    }
                    lsQueryValues.Remove(lsQueryValues.Length - 1, 1);
                    lsQueryValues.Append(")");

                    ExecuteCommand(new StringBuilder(lsQueryHead.ToString() + lsQueryValues.ToString()));
               }
          }

          #endregion BulkCopuy

          #region get/set parametros

          /// <summary>
          /// Funcion que permite guardar en xtsParametros para todo (Pais = 0)
          /// </summary>jmzszs
          /// <param name="parameterName">Nombre del Parametro</param>
          /// <param name="parameterValue">Valor del parametro</param>
          public void SetParameter(string parameterName, string parameterValue)
          {
               SetParameter(parameterName, parameterValue, 0, "", "");
          }

          /// <summary>
          /// Funcion que permite guardar en xtsParametros seleccionando el Pais
          /// </summary>
          /// <param name="parameterName">Nombre del Parametro</param>
          /// <param name="parameterValue">Valor del Parametro</param>
          /// <param name="country">Numero del Pais</param>
          public void SetParameter(string parameterName, string parameterValue, int country)
          {
               SetParameter(parameterName, parameterValue, country, "", "");
          }

          /// <summary>
          /// Sets the parameter.
          /// </summary>
          /// <param name="parameterName">Name of the parameter.</param>
          /// <param name="parameterValue">The parameter value.</param>
          /// <param name="country">The country.</param>
          /// <param name="moduleName">Name of the module.</param>
          /// <param name="description">The description.</param>
          public void SetParameter(string parameterName, string parameterValue, int country, string moduleName, string description)
          {
               SetParameter(parameterName, parameterValue, country, moduleName, description, 0);
          }

          /// <summary>
          /// Sets the parameter.
          /// </summary>
          /// <param name="parameterName">Name of the parameter.</param>
          /// <param name="parameterValue">The parameter value.</param>
          /// <param name="country">The country.</param>
          /// <param name="moduleName">Name of the module.</param>
          /// <param name="description">The description.</param>
          /// <param name="company">The company.</param>
          /// <exception cref="Exception"></exception>
          public void SetParameter(string parameterName, string parameterValue, int country, string moduleName, string description, int company)
          {
               StringBuilder query;
               try
               {
                    query = new StringBuilder();
                    query.AppendFormat("Update {0} set", TableName("xtsParametros"));
                    query.AppendFormat(" Valor='{0}'", parameterValue);
                    if (!string.IsNullOrEmpty(moduleName))
                    {
                         query.AppendFormat(",Modulo='{0}'", moduleName);
                    }
                    if (!string.IsNullOrEmpty(description))
                    {
                         query.AppendFormat(",Descripcion=''", description);
                    }
                    query.AppendFormat(" Where Pais={0}", country);
                    query.AppendFormat(" And Empresa={0}", company);
                    query.AppendFormat(" And Parametro='{0}'", parameterName);
                    if (ExecuteCommand(query) == 0)
                    {
                         query.AppendFormat("Insert into {0} (Pais,Empresa, Parametro,Valor,Modulo,Descripcion) Values(", TableName("xtsParametros"));
                         query.AppendFormat("  {0}", country);
                         query.AppendFormat(", {0}", company);
                         query.AppendFormat(",'{0}'", parameterName);
                         query.AppendFormat(",'{0}'", parameterValue);
                         query.AppendFormat(",'{0}'", moduleName);
                         query.AppendFormat(",'{0}')", description);
                         ExecuteCommand(query);
                    }
               }
               catch (Exception ex)
               {
                    throw new Exception(ex.Message);
               }
               finally
               {
                    InitilizeColections();
               }
          }

          /// <summary>
          /// Regresa el parametro para todos los paises y la empresa actual
          /// </summary>
          /// <param name="parameterName">Nombre del parametro</param>
          /// <returns>Valor del parametro</returns>
          public string GetParameter(string parameterName)
          {
               return GetParameter(parameterName, 0);
          }

          /// <summary>
          /// Regresa el parametro para el pais que se pida y empresa actual
          /// </summary>
          /// <param name="parameterName">NOmbre del parametro</param>
          /// <param name="country">Numero del pais</param>
          /// <returns>Valor del parametro</returns>
          public string GetParameter(string parameterName, int country)
          {
               return GetParameter(parameterName, country, 0);
          }

          /// <summary>
          /// Regresa el pais empresa
          /// </summary>
          /// <param name="parameterName">Nombre del parametro</param>
          /// <param name="country">Numero del pais</param>
          /// <param name="company">Numero de la empresa</param>
          /// <returns>Valor del parametro</returns>
          public string GetParameter(string parameterName, int country, int company)
          {
               StringBuilder query;
               try
               {
                    query = new StringBuilder();
                    query.AppendFormat("Select top 1 Valor From {0}", TableName("xtsParametros"));
                    query.AppendFormat(" Where");
                    query.AppendFormat("     Pais in(0,{0})", country);
                    query.AppendFormat(" And Empresa in (0,{0})", company);
                    query.AppendFormat(" And Parametro='{0}'", parameterName);
                    query.AppendFormat(" Order by Empresa desc, Pais desc");
                    return ExecuteScalar<string>(query);
               }
               catch
               {
                    throw;
               }
               finally
               {
                    InitilizeColections();
               }
          }

          #endregion get/set parametros

          #region Methods

          public string GetStructureTable(string table, bool withIdentity)
          {
               StringBuilder query;
               DataTable data;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.PostgressSql:
                         query = new StringBuilder();
                         query.AppendFormat(" SELECT c.table_schema as schema, ");
                         query.AppendFormat(" c.table_name as table, ");
                         query.AppendFormat(" c.column_name as column, ");
                         query.AppendFormat(" c.data_type as type, ");
                         query.AppendFormat(" case when c.data_type LIKE '%char%' ");
                         query.AppendFormat(" then COALESCE(character_maximum_length::text, 'N/A') ");
                         query.AppendFormat("          when c.data_type LIKE '%numeric%' ");
                         query.AppendFormat(" then  '(' || c.numeric_precision::text || ', ' || c.numeric_scale::text || ')' ");
                         query.AppendFormat(" when c.data_type LIKE '%int%' ");
                         query.AppendFormat(" then c.numeric_precision::text ");
                         query.AppendFormat(" else COALESCE(character_maximum_length::text, 'N/A') ");
                         query.AppendFormat(" end as size ");
                         query.AppendFormat(" FROM information_schema.columns c ");
                         query.AppendFormat(" WHERE c.table_schema NOT LIKE 'pg_%' ");
                         query.AppendFormat(" AND c.table_schema NOT LIKE 'information%' ");
                         query.AppendFormat(" AND c.table_name NOT LIKE 'sql_%' ");
                         query.AppendFormat(" AND c.is_updatable = 'YES' ");
                         query.AppendFormat(" AND c.table_name LIKE '{0}' ", table);
                         query.AppendFormat(" ORDER BY c.table_name, c.ordinal_position; ");
                         data = GetDataTable(query);
                         query = new StringBuilder();
                         foreach(DataRow row in data.Rows )
                         {
                              if (row.GetValue<string>("size") != "N/A")
                              {
                                   if (row.GetValue<string>("type") == "numeric")
                                        query.AppendFormat("{0} {1} {2},", row["column"], row["type"], row["size"]);
                                   else
                                        query.AppendFormat("{0} {1} ({2}),", row["column"], row["type"], row["size"]);
                              }
                              else
                              {
                                   query.AppendFormat("{0} {1} ,", row["column"], row["type"]);
                              }
                         }
                         if (query.Length > 0)
                              query.Remove(query.Length - 1, 1);
                         return query.ToString ();
                    case DatabaseEngines.MySql:
                         return "";
                         
                    default:
                         //DatabaseEngines.SqlServer 
                         query = new StringBuilder();
                         query.AppendFormat("select   SUBSTRING(o.list,0,LEN(o.list)) ");
                         query.Append(" from    sysobjects so ");
                         query.Append(" cross apply ");
                         query.Append("     (SELECT  ");
                         query.Append("         '  ['+column_name+'] ' +  ");
                         query.Append("         data_type + case data_type ");
                         query.Append("             when 'sql_variant' then '' ");
                         query.Append("             when 'text' then '' ");
                         query.Append("             when 'ntext' then '' ");
                         query.Append("             when 'image' then '' ");
                         query.Append("             when 'xml' then '' ");
                         query.Append("             when 'decimal' then '(' + cast(numeric_precision as varchar) + ', ' + cast(numeric_scale as varchar) + ')' ");
                         query.Append("             else coalesce('('+case when character_maximum_length = -1 then 'MAX' else cast(character_maximum_length as varchar) end +')','') end + ' '");
                         if (withIdentity)
                         {
                              query.Append("         + case when exists (  ");
                              query.Append("         select id from syscolumns ");
                              query.Append("         where object_name(id)=so.name ");
                              query.Append("         and name=column_name ");
                              query.Append("         and columnproperty(id,name,'IsIdentity') = 1  ");
                              query.Append("         ) then ");
                              query.Append("         'IDENTITY(' +  ");
                              query.Append("         cast(ident_seed(so.name) as varchar) + ',' +  ");
                              query.Append("         cast(ident_incr(so.name) as varchar) + ')' ");
                              query.Append("         else '' ");
                              query.Append("         end  ");
                         }
                         query.Append("         + ' ' + (case when IS_NULLABLE = 'No' then 'NOT ' else '' end ) + 'NULL ' +  ");
                         query.Append("           case when information_schema.columns.COLUMN_DEFAULT IS NOT NULL THEN 'DEFAULT '+ information_schema.columns.COLUMN_DEFAULT + ',' ELSE ',' END  ");
                         query.Append("      from information_schema.columns where table_name = so.name ");
                         query.Append("      order by ordinal_position ");
                         query.Append("     FOR XML PATH('')) o (list) ");
                         query.Append(" left join ");
                         query.Append("     information_schema.table_constraints tc ");
                         query.Append(" on  tc.Table_name       = so.Name ");
                         query.Append(" AND tc.Constraint_Type  = 'PRIMARY KEY' ");
                         query.Append(" cross apply ");
                         query.Append("     (select '[' + Column_Name + '], ' ");
                         query.Append("      FROM   information_schema.key_column_usage kcu ");
                         query.Append("      WHERE  kcu.Constraint_Name = tc.Constraint_Name ");
                         query.Append("      ORDER BY ");
                         query.Append("         ORDINAL_POSITION ");
                         query.Append("      FOR XML PATH('')) j (list) ");
                         query.Append(" where   xtype = 'U' ");
                         query.AppendFormat(" AND name    NOT IN ('dtproperties') AND name = '{0}' ", table);
                         return ExecuteScalar<string>(query);
               }                         
          }

          public DataTable GetAllDataBasePayroll()
          {
               StringBuilder query;
               DataTable data;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         query = new StringBuilder();
                         query.AppendFormat(" Select name From sysdatabases where name like 'UUPay%' order by name desc");
                         data = GetDataTable(query);

                         query = new StringBuilder();
                         query.AppendFormat(" select EmpD002,EmpD012,EmpD016 from (  ");
                         foreach (DataRow database in data.Rows)
                         {
                              query.AppendFormat(" select EmpD002,EmpD012,EmpD016  from {0}.dbo.Empresas  ", database["name"]);
                              query.AppendFormat(" UNION");
                         }
                         query.Remove(query.Length - 5, 5);
                         query.AppendFormat(" ) A  ");
                         return GetDataTable(query); 

                    default:
                         return null;
               }
          }

          public void CopyTableStructure(string originTable, string targetTable)
          {
               CopyTableStructure(originTable, targetTable, "");
          }

          public void CopyAllTableInDataBase(string origindatabase, string targetdatabase)
          {
               StringBuilder query;
               DataTable tables;
               string tableDestine;
               string tableOrigin;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         query = new StringBuilder();
                         query.AppendFormat(" Select Table_Schema,Table_Name from {0}.INFORMATION_SCHEMA.TABLES ", origindatabase);
                         query.AppendFormat(" where table_type = 'BASE TABLE' and substring(TABLE_NAME,1,1) <> '_'  ");
                         query.AppendFormat(" and not TABLE_NAME like '{0}%' ", DataBase.DataBaseObjectPrefixLogData);
                         query.AppendFormat(" and not TABLE_NAME like 'xsH%'");
                         query.AppendFormat(" and not TABLE_NAME like 'acu0%'");
                         query.AppendFormat(" and not TABLE_NAME like 'gupacu0%'");
                         query.AppendFormat(" and not TABLE_NAME in ('XSTRANSACTIONQUEUE','XSTransactionQueueConfirm','XSTransactionQueueFiles', 'XSLOGS', 'xsAttachFiles', 'gupAnual','gupNominasD','gupNominasDPersonal')");                         
                         query.AppendFormat(" order by TABLE_SCHEMA,TABLE_NAME");
                         tables = GetDataTable(query);
                         foreach (DataRow row in tables.Rows)
                         {
                              tableDestine = $"{targetdatabase}.{row["Table_Schema"]}.{row["Table_Name"]}".ToUpper();
                              tableOrigin = $"{origindatabase}.{row["Table_Schema"]}.{row["Table_Name"]}".ToUpper();
                              if (!ExistsObjectInDataBase(row["Table_Name"].ToString(), "", targetdatabase))
                                   CopyTableStructure(tableOrigin, tableDestine);
                         }
                         break;

                    default:
                         break;
               }
          }

          public void CopyForeingKeys(string origindatabase, string targetdatabase)
          {
               StringBuilder query;
               DataTable tables;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:

                         query = new StringBuilder();
                         query.AppendFormat(" SELECT N' if not exists (Select Name From {0}.dbo.SysObjects where Name = '''+ replace(replace( QUOTENAME(fk.name),']',''),'[','')+''')", targetdatabase);
                         query.AppendFormat(" begin ALTER TABLE '  ");
                         query.AppendFormat(" + '{0}.' + QUOTENAME(cs.name) + '.' + QUOTENAME(ct.name) ", targetdatabase);
                         query.AppendFormat(" + ' ADD CONSTRAINT ' + QUOTENAME(fk.name) ");
                         query.AppendFormat(" + ' FOREIGN KEY (' + STUFF((SELECT ',' + QUOTENAME(c.name) ");
                         query.AppendFormat(" FROM {0}.sys.columns AS c ", origindatabase);
                         query.AppendFormat(" INNER JOIN {0}.sys.foreign_key_columns AS fkc ", origindatabase);
                         query.AppendFormat(" ON fkc.parent_column_id = c.column_id ");
                         query.AppendFormat(" AND fkc.parent_object_id = c.[object_id] ");
                         query.AppendFormat(" WHERE fkc.constraint_object_id = fk.[object_id] ");
                         query.AppendFormat(" ORDER BY fkc.constraint_column_id ");
                         query.AppendFormat(" FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)'), 1, 1, N'') ");
                         query.AppendFormat(" + ') REFERENCES {0}.' + QUOTENAME(rs.name) + '.' + QUOTENAME(rt.name) ", targetdatabase);
                         query.AppendFormat(" + '(' + STUFF((SELECT ',' + QUOTENAME(c.name) ");
                         query.AppendFormat(" FROM {0}.sys.columns AS c ", origindatabase);
                         query.AppendFormat(" INNER JOIN {0}.sys.foreign_key_columns AS fkc ", origindatabase);
                         query.AppendFormat(" ON fkc.referenced_column_id = c.column_id ");
                         query.AppendFormat(" AND fkc.referenced_object_id = c.[object_id] ");
                         query.AppendFormat(" WHERE fkc.constraint_object_id = fk.[object_id] ");
                         query.AppendFormat(" ORDER BY fkc.constraint_column_id ");
                         query.AppendFormat(" FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)'), 1, 1, N'') +'); end;' reference");
                         query.AppendFormat(" FROM {0}.sys.foreign_keys AS fk ", origindatabase);
                         query.AppendFormat(" INNER JOIN {0}.sys.tables AS rt ", origindatabase);
                         query.AppendFormat(" ON fk.referenced_object_id = rt.[object_id] ");
                         query.AppendFormat(" INNER JOIN {0}.sys.schemas AS rs ", origindatabase);
                         query.AppendFormat(" ON rt.[schema_id] = rs.[schema_id] ");
                         query.AppendFormat(" INNER JOIN {0}.sys.tables AS ct ", origindatabase);
                         query.AppendFormat(" ON fk.parent_object_id = ct.[object_id] ");
                         query.AppendFormat(" INNER JOIN {0}.sys.schemas AS cs ", origindatabase);
                         query.AppendFormat(" ON ct.[schema_id] = cs.[schema_id] ");
                         query.AppendFormat(" WHERE rt.is_ms_shipped = 0 AND ct.is_ms_shipped = 0;  ");

                         tables = GetDataTable(query);
                         foreach (DataRow row in tables.Rows)
                         {
                              ExecuteCommand(new StringBuilder(row["reference"].ToString()));
                         }
                         break;

                    default:
                         break;
               }
          }

          /// <summary>
          /// Copies the table structure.
          /// </summary>
          /// <param name="originTable">The origin table.</param>
          /// <param name="targetTable">The target table.</param>
          public void CopyTableStructure(string originTable, string targetTable, string columnfilter)
          {
               StringBuilder query;
               string[] objectdatabase;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         objectdatabase = originTable.Split('.');
                         query = new StringBuilder();
                         query.AppendFormat("select   'create table {0} (' + SUBSTRING(o.list,0,LEN(o.list)) + ');' + CASE WHEN tc.Constraint_Name IS NULL THEN '' ELSE 'ALTER TABLE {0} ADD PRIMARY KEY ' + ' (' + LEFT(j.List, Len(j.List)-1) + ')' END ", targetTable);
                         if (objectdatabase.Length == 3)
                              query.AppendFormat(" from   {0}.dbo.sysobjects so ", objectdatabase[0]);
                         else
                              query.AppendFormat(" from    sysobjects so ");
                         query.AppendFormat(" cross apply ");
                         query.AppendFormat("     (SELECT  ");
                         query.AppendFormat("         '  ['+column_name+'] ' +  ");
                         query.AppendFormat("         data_type + case data_type ");
                         query.AppendFormat("             when 'sql_variant' then '' ");
                         query.AppendFormat("             when 'text' then '' ");
                         query.AppendFormat("             when 'ntext' then '' ");
                         query.AppendFormat("             when 'image' then '' ");
                         query.AppendFormat("             when 'xml' then '' ");
                         query.AppendFormat("             when 'decimal' then '(' + cast(numeric_precision as varchar) + ', ' + cast(numeric_scale as varchar) + ')' ");
                         query.AppendFormat("             else coalesce('('+case when character_maximum_length = -1 then 'MAX' else cast(character_maximum_length as varchar) end +')','') end + ' ' + ");
                         query.AppendFormat("         case when exists (  ");
                         if (objectdatabase.Length == 3)
                              query.AppendFormat("         select id from {0}.dbo.syscolumns ", objectdatabase[0]);
                         else
                              query.AppendFormat("         select id from syscolumns ");
                         query.AppendFormat("         where object_name(id)=so.name ");
                         query.AppendFormat("         and name=column_name ");
                         query.AppendFormat("         and columnproperty(id,name,'IsIdentity') = 1  ");
                         query.AppendFormat("         ) then ");
                         query.AppendFormat("         'IDENTITY(' +  ");
                         query.AppendFormat("         cast(ident_seed(so.name) as varchar) + ',' +  ");
                         query.AppendFormat("         cast(ident_incr(so.name) as varchar) + ')' ");
                         query.AppendFormat("         else '' ");
                         query.AppendFormat("         end + ' ' + ");
                         query.AppendFormat("          (case when IS_NULLABLE = 'No' then 'NOT ' else '' end ) + 'NULL ' +  ");

                         if (objectdatabase.Length == 3)
                         {
                              query.AppendFormat("           case when {0}.information_schema.columns.COLUMN_DEFAULT IS NOT NULL THEN 'DEFAULT '+ {0}.information_schema.columns.COLUMN_DEFAULT + ',' ELSE ',' END  ", objectdatabase[0]);
                              query.AppendFormat("      from {0}.information_schema.columns where table_name = so.name ", objectdatabase[0]);
                         }
                         else
                         {
                              query.AppendFormat("           case when information_schema.columns.COLUMN_DEFAULT IS NOT NULL THEN 'DEFAULT '+ information_schema.columns.COLUMN_DEFAULT + ',' ELSE ',' END  ");
                              query.AppendFormat("      from information_schema.columns where table_name = so.name ");
                         }
                         if (!string.IsNullOrEmpty(columnfilter))
                              query.AppendFormat(" AND column_name  in ({0})", columnfilter);
                         query.AppendFormat("      order by ordinal_position ");
                         query.AppendFormat("     FOR XML PATH('')) o (list) ");
                         query.AppendFormat(" left join ");
                         if (objectdatabase.Length == 3)
                              query.AppendFormat("     {0}.information_schema.table_constraints tc ", objectdatabase[0]);
                         else
                              query.AppendFormat("     information_schema.table_constraints tc ");
                         query.AppendFormat(" on  tc.Table_name       = so.Name ");
                         query.AppendFormat(" AND tc.Constraint_Type  = 'PRIMARY KEY' ");
                         query.AppendFormat(" cross apply ");
                         query.AppendFormat("     (select '[' + Column_Name + '], ' ");
                         if (objectdatabase.Length == 3)
                              query.AppendFormat("      FROM   {0}.information_schema.key_column_usage kcu ", objectdatabase[0]);
                         else
                              query.AppendFormat("      FROM   information_schema.key_column_usage kcu ");
                         query.AppendFormat("      WHERE  kcu.Constraint_Name = tc.Constraint_Name ");
                         query.AppendFormat("      ORDER BY ");
                         query.AppendFormat("         ORDINAL_POSITION ");
                         query.AppendFormat("      FOR XML PATH('')) j (list) ");
                         query.AppendFormat(" where   xtype = 'U' ");
                         if (objectdatabase.Length == 3)
                              query.AppendFormat(" AND name    NOT IN ('dtproperties') AND name = '{0}' ", objectdatabase[2]);
                         else
                              query.AppendFormat(" AND name    NOT IN ('dtproperties') AND name = '{0}' ", originTable);
                         ExecuteCommand(new StringBuilder(ExecuteScalar<string>(query)));
                         break;

                    default:
                         break;
               }
          }

          /// <summary>
          /// Gets the column names.
          /// </summary>
          /// <param name="tableName">Name of the table.</param>
          /// <param name="aliasTable">The alias table.</param>
          /// <returns></returns>
          public string GetColumnNames(string tableName, string aliasTable = "")
          {
               StringBuilder columnNames;
               StringBuilder query;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         columnNames = new StringBuilder();
                         query = new StringBuilder();                         
                         query.AppendFormat("Select Column_Name From Information_Schema.Columns Where Table_Name = @TableName ORDER BY ORDINAL_POSITION");
                         using (GenericTable genericTable = GetGenericTableReader(tableName, query, new List<ParameterSql>() { new ParameterSql("@TableName", tableName) }))
                         {
                              foreach (Row column in genericTable.Rows)
                              {
                                   if (!string.IsNullOrEmpty(aliasTable))
                                        columnNames.AppendFormat("{0}.", aliasTable);
                                   columnNames.AppendFormat("{0},", column["Column_Name"]);
                              }
                              columnNames.Remove(columnNames.Length - 1, 1);
                              return columnNames.ToString();
                         }
                    default:
                         return "";
               }
          }

          /// <summary>
          /// Gets the columns.
          /// </summary>
          /// <param name="tableName">Name of the table.</param>
          /// <returns></returns>
          public DataTable GetColumns(string tableName)
          {
               StringBuilder query;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         query = new StringBuilder();
                         query.AppendFormat("Select Column_Name Columna From Information_Schema.Columns Where Table_Name = @TableName ORDER BY ORDINAL_POSITION");
                         return GetDataTable(query, new List<ParameterSql>() { new ParameterSql("@TableName", tableName) });
                    default:
                         return null;
               }
          }

          /// <summary>
          /// Tables the name.
          /// </summary>
          /// <param name="tableName">Name of the table.</param>
          /// <param name="containPrefix">if set to <c>true</c> [contain prefix].</param>
          /// <param name="catalogContainsHistory">if set to <c>true</c> [catalog contains history].</param>
          /// <param name="isHistoryTable">if set to <c>true</c> [is history table].</param>
          /// <returns></returns>
          public string TableName(string tableName, bool containPrefix, bool catalogContainsHistory, bool isHistoryTable)
          {
               return DataBase.TableName(tableName, containPrefix, catalogContainsHistory, isHistoryTable);
          }

          /// <summary>
          /// Tables the name.
          /// </summary>
          /// <param name="tableName">Name of the table.</param>
          /// <param name="containPrefix">if set to <c>true</c> [contain prefix].</param>
          /// <param name="catalogContainsHistory">if set to <c>true</c> [catalog contains history].</param>
          /// <returns></returns>
          public string TableName(string tableName, bool containPrefix, bool catalogContainsHistory)
          {
               return DataBase.TableName(tableName, containPrefix, catalogContainsHistory, false);
          }

          /// <summary>
          /// Tables the name.
          /// </summary>
          /// <param name="tableName">Name of the table.</param>
          /// <param name="containPrefix">if set to <c>true</c> [contain prefix].</param>
          /// <returns></returns>
          public string TableName(string tableName, bool containPrefix)
          {
               return DataBase.TableName(tableName, containPrefix, false, false);
          }

          /// <summary>
          /// Tables the name.
          /// </summary>
          /// <param name="tableName">Name of the table.</param>
          /// <returns></returns>
          public string TableName(string tableName)
          {
               return DataBase.TableName(tableName, true, false, false);
          }

          /// <summary>
          /// Gets the data from table.
          /// </summary>
          /// <param name="tableName">Name of the table.</param>
          /// <param name="containPrefix">if set to <c>true</c> [contain prefix].</param>
          /// <param name="filter">The filter.</param>
          /// <returns></returns>
          public DataView GetDataFromTable(string tableName, bool containPrefix, string filter)
          {
               StringBuilder query = new StringBuilder();
              query.AppendFormat ("select * from {0}", TableName(tableName, containPrefix));
               if (!String.IsNullOrEmpty(filter))
                    query.AppendFormat(" where {0}", filter);
               return GetDataView(query);
          }

          /// <summary>
          /// Functions the is null.
          /// </summary>
          /// <returns></returns>
          public string FunctionIsNull()
          {
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         return "ISNULL";
                    case DatabaseEngines.MySql:
                         return "IFNULL";
                    default:
                         return "ISNULL";
               }
          }

          /// <summary>
          /// Functions the get date.
          /// </summary>
          /// <returns></returns>
          public string FunctionGetDate()
          {
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         return "GETUTCDATE()";
                    case DatabaseEngines.MySql:
                         return "UTC_TIMESTAMP()";
                    default:
                         return "GETUTCDATE()";
               }
          }
          public string FunctionSCOPEIDENTITY(string key = "")
          {
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         return "SCOPE_IDENTITY()";
                    case DatabaseEngines.MySql:
                         return "LAST_INSERT_ID()";
                    case DatabaseEngines.PostgressSql:
                         return $"currval('{DataBase.Catalog}_{key}_seq')";
                    default:
                         return "SCOPE_IDENTITY()";

               }
          }

          /// <summary>
          /// FunctionConcatena
          /// </summary>
          /// <returns></returns>
          public string FunctionConcatena()
          {
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         return "+";

                    default:
                         return "+";
               }
          }

          /// <summary>
          /// FunctionCreateTable
          /// </summary>
          /// <returns></returns>
          public string FunctionCreateTable()
          {
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         return "CREATE TABLE";

                    default:
                         return "CREATE TABLE";
               }
          }

          /// <summary>
          /// DataTypeDate
          /// </summary>
          /// <returns></returns>
          public string DataTypeDate()
          {
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         return "DATETIME";

                    default:
                         return "DATETIME";
               }
          }

          /// <summary>
          /// CurrencyDataType
          /// </summary>
          /// <returns></returns>
          public string CurrencyDataType()
          {
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         return "MONEY";

                    default:
                         return "MONEY";
               }
          }

          /// <summary>
          /// BinaryDataType
          /// </summary>
          /// <returns></returns>
          public string BinaryDataType()
          {
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         return "VARBINARY(MAX)";

                    default:
                         return "VARBINARY(MAX)";
               }
          }

          /// <summary>
          /// VarcharMaxDataType
          /// </summary>
          /// <returns></returns>
          public string VarcharMaxDataType()
          {
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         return "VARCHAR(MAX)";

                    default:
                         return "VARCHAR(MAX)";
               }
          }

          /// <summary>
          /// CreatePrimaryKey
          /// </summary>
          /// <param name="keyName"></param>
          /// <param name="listKeys"></param>
          /// <returns></returns>
          public string CreatePrimaryKey(string keyName, params string[] listKeys)
          {
               string orderQuery;
               StringBuilder query;
               query = new StringBuilder();
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         orderQuery = "ASC";
                         query.AppendFormat(" CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED ", keyName);
                         break;
                    case DatabaseEngines.MySql:
                         orderQuery = "";
                         query.AppendFormat(" PRIMARY KEY  ", keyName);
                         break;
                    default:
                         orderQuery = "ASC";
                         query.AppendFormat(" CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED ", keyName);
                         break;
               }
               query.AppendFormat(" ( ");
               foreach (string lsLLave in listKeys)
               {
                    query.AppendFormat("    {0} {1},", lsLLave, orderQuery);
               }
               query.Remove(query.Length - 1, 1);
               query.AppendFormat(" ) ");
               return query.ToString();
          }

          /// <summary>
          /// Limpias the coleccion parametros.
          /// </summary>
          public void CleanParametersCollection()
          {
               _sqlParameters = new Dictionary<string, object>();
          }

          /// <summary>
          /// Agregars the parametro.
          /// </summary>
          /// <param name="parameterName">The ps parametro.</param>
          /// <param name="parameterValue">The po valor.</param>
          public void AddParameter(string parameterName, object parameterValue)
          {
               _sqlParameters[parameterName] = parameterValue;
          }

          /// <summary>
          /// Creas the pametros SQL.
          /// </summary>
          private void CreateSqlParameters()
          {
               _sqlParametersList = new List<ParameterSql>(_sqlParameters.Count);
               foreach (KeyValuePair<string, object> parameter in _sqlParameters)
               {
                    _sqlParametersList.Add(new ParameterSql(parameter.Key, parameter.Value));
               }
          }

          /// <summary>
          /// Existes the conexion ala base datos.
          /// </summary>
          public void ExistConnectionToDataBase()
          {
               try
               {
                    using (var connection = new HandlesConnection(DataBase.StringConnection, DataBase.Engine))
                    {
                         DataBase.ExistsConnection = true;
                    }
               }
               catch
               {
                    DataBase.ExistsConnection = false;
                    throw;
               }
          }

          /// <summary>
          /// Regresas the base datos.
          /// </summary>
          /// <returns></returns>
          public List<string> GetDataBasesNames()
          {
               StringBuilder query;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         query = new StringBuilder();
                         query.AppendFormat(" Select name From sysdatabases");
                         return GetGenericFieldBasedList<string>(query, "name");

                    default:
                         return new List<string>();
               }
          }

          /// <summary>
          /// Regresas the tablas de la base datos.
          /// </summary>
          /// <returns></returns>
          public List<string> GetTableNamesFromDatabase()
          {
               StringBuilder query;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         query = new StringBuilder();
                         query.AppendFormat(" Select name From sysobjects Where xtype = 'U' Order by name");
                         return GetGenericFieldBasedList<string>(query, "name");

                    default:
                         return new List<string>();
               }
          }

          /// <summary>
          /// Funcion que determina si existe una bd datos en el servidor
          /// </summary>
          /// <param name="dataBaseName">Nombre de la bd</param>
          /// <returns></returns>
          public bool ExistsDataBase(string dataBaseName)
          {
               StringBuilder query;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         query = new StringBuilder();
                         query.AppendFormat ("SELECT name FROM sysdatabases where name = @DataBase");
                         return dataBaseName.ToUpper().Trim() == ExecuteScalar<string>(query, new List<ParameterSql>() {new ParameterSql ("@DataBase", dataBaseName) }).Trim().ToUpper();

                    default:
                         return false;
               }
          }

          /// <summary>
          /// ExistsDataBaseSchema
          /// </summary>
          /// <param name="dataBaseName"></param>
          /// <param name="schemaName"></param>
          /// <returns></returns>
          public bool ExistsDataBaseSchema(string dataBaseName, string schemaName)
          {
               StringBuilder query;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         query = new StringBuilder();
                         query.AppendFormat ("select name from {0}.sys.schemas where name = @Schema", dataBaseName);                         
                         return schemaName.ToUpper().Trim() == ExecuteScalar<string>(query, new List<ParameterSql>() { new ParameterSql("@DataBase", schemaName) }).Trim().ToUpper();

                    default:
                         return false;
               }
          }

          /// <summary>
          /// GetSpaceUsedInDataBase
          /// </summary>
          /// <returns></returns>
          public DataTable GetSpaceUsedInDataBase()
          {
               StringBuilder query;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         query = new StringBuilder();
                         query.AppendFormat("EXEC sp_spaceused");
                         return GetDataTable(query);

                    default:
                         return new DataTable();
               }
          }

          /// <summary>
          /// Creas the base de datos.
          /// </summary>
          /// <param name="dataBaseName">The ps base datos.</param>
          public void CreateDataBase(string dataBaseName)
          {
               StringBuilder query;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         query = new StringBuilder();
                         query.AppendFormat(" CREATE DATABASE {0} ON PRIMARY(", dataBaseName);
                         query.AppendFormat(" NAME = '{0}', ", dataBaseName);
                         query.AppendFormat(" FILENAME = '{0}\\{1}.mdf', ", PhysicalDataBasePath(), dataBaseName);
                         query.AppendFormat(" SIZE = 5MB , MAXSIZE = UNLIMITED, FILEGROWTH = 1024KB) ");
                         query.AppendFormat(" LOG ON (NAME ='{0}_log', ", dataBaseName);
                         query.AppendFormat(" FILENAME = '{0}\\{1}_log.ldf', ", PhysicalLogDataBasePath(), dataBaseName);
                         query.AppendFormat(" SIZE = 1MB, MAXSIZE = 2048GB , FILEGROWTH = 10% )");
                         ExecuteCommand(query);
                         break;

                    default:
                         break;
               }
          }

          /// <summary>
          /// Rutas the fisica base datos.
          /// </summary>
          /// <returns></returns>
          public string PhysicalDataBasePath()
          {
               StringBuilder query;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         query = new StringBuilder();
                         query.AppendFormat("SELECT mf.physical_name ");
                         query.AppendFormat(" FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS divfs");
                         query.AppendFormat(" JOIN sys.master_files AS mf ON mf.database_id = divfs.database_id AND mf.file_id = divfs.file_id");
                         query.AppendFormat(" where  DB_NAME(mf.database_id) = '{0}'", DataBase.Catalog);
                         query.AppendFormat(" and type_desc = 'ROWS'");
                         return Path.GetDirectoryName(ExecuteScalar<string>(query));

                    default:
                         return "";
               }
          }

          /// <summary>
          /// Rutas the fisica log base datos.
          /// </summary>
          /// <returns></returns>
          public string PhysicalLogDataBasePath()
          {
               StringBuilder query;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         query = new StringBuilder();
                         query.AppendFormat("SELECT mf.physical_name ");
                         query.AppendFormat(" FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS divfs ");
                         query.AppendFormat(" JOIN sys.master_files AS mf ON mf.database_id = divfs.database_id AND mf.file_id = divfs.file_id");
                         query.AppendFormat(" where  DB_NAME(mf.database_id) = '{0}'", DataBase.Catalog);
                         query.AppendFormat(" and type_desc = 'LOG'");
                         return Path.GetDirectoryName(ExecuteScalar<string>(query));

                    default:
                         return "";
               }
          }

          /// <summary>
          /// ExistsPrimaryRestrictionInTable
          /// </summary>
          /// <param name="objectName"></param>
          /// <returns></returns>
          public bool ExistsPrimaryRestrictionInTable(string objectName)
          {
               return ExistsPrimaryRestrictionInTable(objectName, "");
          }

          /// <summary>
          /// Verifica que exista un key constraint en alguna tabla
          /// </summary>
          /// <param name="objectName">The ps objeto.</param>
          /// <param name="dataBaseName">The ps base datos.</param>
          /// <returns></returns>
          public bool ExistsPrimaryRestrictionInTable(string objectName, string dataBaseName)
          {
               StringBuilder query;
               try
               {
                    switch (DataBase.Engine)
                    {
                         case DatabaseEngines.SqlServer:
                              query = new StringBuilder();
                              if (!String.IsNullOrEmpty(dataBaseName))
                              {
                                  query.AppendFormat ("select CONSTRAINT_NAME from [{0}].[INFORMATION_SCHEMA].[TABLE_CONSTRAINTS] where table_name = @TableName", dataBaseName);
                              }
                              else
                              {
                                  query.AppendFormat ("select CONSTRAINT_NAME from [INFORMATION_SCHEMA].[TABLE_CONSTRAINTS] where table_name = @TableName");
                              }
                              return String.IsNullOrEmpty(ExecuteScalar<string>(query, new List<ParameterSql>() { new ParameterSql("@TableName", objectName) }));

                         default:
                              return false;
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex);
                    return false;
               }
          }

          /// <summary>
          /// Funcion que deterina si existe un objeto en la bd
          /// </summary>
          /// <param name="objectName">Nombre del objeto</param>
          /// <param name="type">Tipo del objeto</param>
          /// <param name="dataBaseName">NOmbre de la bd, cuando no es la bd principal del servidor</param>
          /// <returns></returns>
          public bool ExistsObjectInDataBase(string objectName, string type = "", string dataBaseName = "")
          {
               string value;
               bool isTemporal;
               StringBuilder query;
               List<ParameterSql> parameters;
               query = new StringBuilder();
               try
               {
                    if (string.IsNullOrEmpty(objectName))
                         return false;
                    switch (DataBase.Engine)
                    {
                         case DatabaseEngines.SqlServer:
                              parameters = new List<ParameterSql>();
                              isTemporal = false;
                              isTemporal = objectName.Substring(0, Math.Min(1, objectName.Length)) == "#";
                              if (!isTemporal)
                              {
                                   if (!String.IsNullOrEmpty(dataBaseName))
                                       query.AppendFormat ("Select Name From [{0}].[dbo].[SysObjects]  ", dataBaseName);
                                   else
                                       query.AppendFormat ("Select Name From [{0}].[dbo].[SysObjects]  ", DataBase.Catalog);
                                   query.AppendFormat (" where Name = @Object");
                                   parameters.Add(new ParameterSql("@Object", string.Format("{0}{1}", DataBase.Prefix, objectName)));
                              }
                              else
                              {
                                  query.AppendFormat ("Select Count(*) From {0}", objectName);
                              }
                              if (!String.IsNullOrEmpty(type))
                              {
                                   query.AppendFormat(" And type = @Type", type);
                                   parameters.Add(new ParameterSql("@Type", type));
                              }
                              value = ExecuteScalar<string>(query, parameters);
                              if (!isTemporal)
                              {
                                   return value != null && objectName.ToUpper().Trim() == Convert.ToString(value).Trim().ToUpper();
                              }
                              else
                              {
                                   return true;
                              }
                         case DatabaseEngines.MySql:
                              parameters = new List<ParameterSql>
                              {
                                   new ParameterSql("@Object", string.Format("{0}{1}", DataBase.Prefix, objectName).ToUpper())
                              };
                              query = new StringBuilder();
                              query.AppendFormat("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES where upper(TABLE_SCHEMA) = '{0}' and upper(TABLE_NAME) = @Object ", DataBase.Catalog.ToUpper());
                              if (ExecuteScalar<int>(query, parameters) > 0)
                                   return true;
                              query = new StringBuilder();
                              query.AppendFormat("SELECT count(*) FROM INFORMATION_SCHEMA.COLUMNS where upper(TABLE_SCHEMA) = '{0}' and upper(COLUMN_NAME) =@Object ", DataBase.Catalog.ToUpper());
                              if (ExecuteScalar<int>(query, parameters) > 0)
                                   return true;
                              return false;
                         case DatabaseEngines.PostgressSql:
                              parameters = new List<ParameterSql>
                              {
                                   new ParameterSql("@Object", string.Format("{0}{1}", DataBase.Prefix, objectName).ToUpper())
                              };
                              query = new StringBuilder();
                              query.AppendFormat("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES where upper(table_catalog) = '{0}' and upper(table_schema) = '{1}'  and upper(table_name) = @Object ", DataBase.Catalog.ToUpper(), DataBase.Owner.ToUpper());
                              if (ExecuteScalar<int>(query, parameters) > 0)
                                   return true;
                              query = new StringBuilder();
                              query.AppendFormat("SELECT count(*) FROM INFORMATION_SCHEMA.COLUMNS where upper(table_catalog) = '{0}' and upper(table_schema) = '{1}' and upper(column_name) =@Object ", DataBase.Catalog.ToUpper(), DataBase.Owner.ToUpper());
                              if (ExecuteScalar<int>(query, parameters) > 0)
                                   return true;
                              return false;
                         case DatabaseEngines.OleDb:
                              try
                              {
                                   query = new StringBuilder();
                                   query.AppendFormat("Select count(*) From {0} ", objectName);
                                   if (ExecuteScalar<int>(query) >= 0)
                                        return true;
                              }
                              catch
                              {
                                   return false;
                              }
                              
                              return false;
                         default:
                              return false;
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
                    throw;
               }
               finally
               {
                    _sqlParametersList = new List<ParameterSql>();
               }
          }

          /// <summary>
          /// ExistsObjectInDataBase2
          /// </summary>
          /// <param name="objectName"></param>
          /// <param name="type"></param>
          /// <param name="dataBaseName"></param>
          /// <returns></returns>
          public bool ExistsObjectInDataBase2(string objectName, string type = "", string dataBaseName = "")
          {
               StringBuilder query;
               object value;
               bool isTemporal;
               StringBuilder filter;
               List<ParameterSql> parameters;
               query = new StringBuilder();
               try
               {
                    if (String.IsNullOrEmpty(objectName))
                         return false;
                    switch (DataBase.Engine)
                    {
                         case DatabaseEngines.SqlServer:
                              query = new StringBuilder();
                              filter = null;
                              isTemporal = false;

                              isTemporal = objectName.Substring(0, Math.Min(1, objectName.Length)) == "#";
                              if (!isTemporal)
                              {
                                   if (!String.IsNullOrEmpty(dataBaseName))
                                        query.AppendFormat("Select Name From [{1}].[dbo].[SysObjects]  Where Name='{2}{0}'", objectName, dataBaseName, DataBase.Prefix);
                                   else
                                        query.AppendFormat("Select Name From [{1}].[dbo].[SysObjects]  Where Name='{2}{0}'", objectName, DataBase.Catalog, DataBase.Prefix);
                              }
                              else
                              {
                                   query.AppendFormat("Select Count(*) From {0}", objectName);
                              }
                              if (!String.IsNullOrEmpty(type))
                              {
                                   filter = new StringBuilder();
                                   filter.AppendFormat(" and type ='{0}' ", type);
                                   query.AppendFormat(filter.ToString());
                              }

                              value = ExecuteScalar(query);
                              if (!isTemporal)
                              {
                                   return objectName.ToUpper().Trim() == Convert.ToString(value).Trim().ToUpper();
                              }
                              else
                              {
                                   return true;
                              }
                         case DatabaseEngines.MySql:
                              parameters = new List<ParameterSql>
                              {
                                   new ParameterSql("@Object", string.Format("{0}{1}", DataBase.Prefix, objectName).ToUpper())
                              };
                              query = new StringBuilder();
                              query.AppendFormat("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES where upper(TABLE_SCHEMA) = '{0}' and upper(TABLE_NAME) = @Object ", DataBase.Catalog.ToUpper());
                              if (ExecuteScalar<int>(query, parameters) > 0)
                                   return true;
                              query = new StringBuilder();
                              query.AppendFormat("SELECT count(*) FROM INFORMATION_SCHEMA.COLUMNS where upper(TABLE_SCHEMA) = '{0}' and upper(COLUMN_NAME) =@Object ", DataBase.Catalog.ToUpper());
                              return ExecuteScalar<int>(query, parameters) > 0;
                         case DatabaseEngines.PostgressSql:
                              parameters = new List<ParameterSql>
                              {
                                   new ParameterSql("@Object", string.Format("{0}{1}", DataBase.Prefix, objectName).ToUpper())
                              };
                              query = new StringBuilder();
                              query.AppendFormat("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES where upper(table_catalog) = '{0}' and upper(table_schema) = '{1}'  and upper(table_name) = @Object ", DataBase.Catalog.ToUpper(), DataBase.Owner.ToUpper());
                              if (ExecuteScalar<int>(query, parameters) > 0)
                                   return true;
                              query = new StringBuilder();
                              query.AppendFormat("SELECT count(*) FROM INFORMATION_SCHEMA.COLUMNS where upper(table_catalog) = '{0}' and upper(table_schema) = '{1}' and upper(column_name) =@Object ", DataBase.Catalog.ToUpper(), DataBase.Owner.ToUpper());
                              return ExecuteScalar<int>(query, parameters) > 0;
                         default:
                              return false;
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
                    throw;
               }
          }

          /// <summary>
          /// Grabas the log.
          /// </summary>
          /// <param name="exception">The po exception.</param>
          /// <param name="query">The ps consulta.</param>
          public void RecordLog(Exception exception, String query)
          {
               RecordLog(exception, new StringBuilder(query));
          }

          /// <summary>
          /// Fucion que graba en la tbala de errores del sistema
          /// </summary>
          /// <param name="ex">The po exception.</param>
          /// <param name="query">The ps consulta.</param>
          public void RecordLog(Exception ex, StringBuilder query)
          {
               //Logger.Fatal(ex, query.ToString());               
          }

          /// <summary>
          /// Grabas the log.
          /// </summary>
          /// <param name="exception">The po exception.</param>
          public void RecordLog(Exception exception)
          {
               RecordLog(exception, new StringBuilder());
          }

          /// <summary>
          /// Grabas the log.
          /// </summary>
          /// <param name="exception">The po exception.</param>
          /// <param name="queryList">The ps consulta.</param>
          public void RecordLog(Exception exception, List<StringBuilder> queryList)
          {
               foreach (StringBuilder lsQuery in queryList)
               {
                    RecordLog(exception, lsQuery);
               }
          }

          /// <summary>
          /// Insertas the error.
          /// </summary>
          /// <param name="exception">The po exception.</param>
          /// <param name="query">The ps consulta.</param>
          private void InsertLog(Exception exception, StringBuilder query)
          {
               StringBuilder query2 = new StringBuilder();
               try
               {
                    switch (DataBase.Engine)
                    {
                         case DatabaseEngines.SqlServer:
                              _sqlParameters = new Dictionary<string, object>();
                              query2.AppendFormat("Insert into {0}", TableName(_LogTable));
                              query2.AppendFormat("(Type,Message,Stack,Query) ");
                              query2.AppendFormat("values(@Type,@Message,@Stack,@Query)");
                              AddParameter("@Type", exception.GetType().ToString());
                              AddParameter("@Message", exception.Message);
                              AddParameter("@Stack", exception.ToString());
                              AddParameter("@Query", query.ToString());
                              CreateSqlParameters();
                              ExecuteCommand(query2, _sqlParametersList);
                              break;

                         default:
                              break;
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query2);
               }
          }

          /// <summary>
          /// Creas the tablaxts bitacora errores.
          /// </summary>
          private void CreateLogBookTableForErrors()
          {
               StringBuilder query;
               if (_existsErrorInLogData)
               {
                    return;
               }
               query = new StringBuilder();
               try
               {
                    switch (DataBase.Engine)
                    {
                         case DatabaseEngines.SqlServer:
                              query.AppendFormat("CREATE TABLE {0}(", TableName(_LogTable));
                              query.AppendFormat(" [ID] bigint IDENTITY(1,1) NOT NULL,");
                              query.AppendFormat(" [Type][varchar](100) not NULL,");
                              query.AppendFormat(" [DateLog] datetime not NULL DEFAULT (getdate()),");
                              query.AppendFormat(" [Message] [varchar](max) NULL,");
                              query.AppendFormat(" [Stack] [varchar](max) NULL,");
                              query.AppendFormat(" [Query] [varchar](max) NULL,");
                              query.AppendFormat(" [Reporter][bit] not null default(0),");
                              query.AppendFormat(" PRIMARY KEY (ID Asc))");
                              ExecuteCommand(query);
                              break;
                         case DatabaseEngines.MySql:
                              query.AppendFormat("CREATE TABLE {0}(", TableName(_LogTable));
                              query.AppendFormat(" `ID` bigint NOT NULL AUTO_INCREMENT,");
                              query.AppendFormat(" `Type` varchar(100) not NULL,");
                              query.AppendFormat(" `DateLog` datetime not NULL DEFAULT NOW(),");
                              query.AppendFormat(" `Message` LONGTEXT NULL,");
                              query.AppendFormat(" `Stack` LONGTEXT NULL,");
                              query.AppendFormat(" `Query` LONGTEXT NULL,");
                              query.AppendFormat(" `Reporter` INT(1) not null default 0,");
                              query.AppendFormat(" PRIMARY KEY (`ID`))");
                              ExecuteCommand(query);
                              break;
                         case DatabaseEngines.PostgressSql:
                              query.AppendFormat("CREATE TABLE {0}(", TableName(_LogTable));
                              query.AppendFormat(" ID BIGSERIAL,", _LogTable);
                              query.AppendFormat(" Type character(100) not NULL,");
                              query.AppendFormat(" DateLog  timestamp with time zone DEFAULT now(),");
                              query.AppendFormat(" Message text NULL,");
                              query.AppendFormat(" Stack text NULL,");
                              query.AppendFormat(" Query text NULL,");
                              query.AppendFormat(" Reporter bit not null default '0',");
                              query.AppendFormat(" PRIMARY KEY (ID))");
                              ExecuteCommand(query);
                              break;
                         default:
                              break;
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
               }
          }

          /// <summary>
          /// Creates the view.
          /// </summary>
          /// <param name="viewName">Name of the view.</param>
          /// <param name="queryView">The query view.</param>
          public void CreateView(string viewName, StringBuilder queryView)
          {
               CreateView(viewName, queryView, true);
          }

          /// <summary>
          /// Creates the view.
          /// </summary>
          /// <param name="viewName">Name of the view.</param>
          /// <param name="queryView">The query view.</param>
          /// <param name="deleteView">if set to <c>true</c> [delete view].</param>
          public void CreateView(string viewName, StringBuilder queryView, bool deleteView)
          {
               StringBuilder query = new StringBuilder();
               try
               {
                    if (deleteView)
                    {
                         if (!DropView(viewName))
                         {
                              return;
                         }
                    }
                    switch (DataBase.Engine)
                    {
                         case DatabaseEngines.SqlServer:
                              query = new StringBuilder();
                              query.AppendFormat (" USE [{0}]", DataBase.Catalog);
                              ExecuteCommand(query);
                              query = new StringBuilder();
                              query.AppendFormat ("Create View [dbo].[{0}] as {1};", viewName, queryView);
                              ExecuteCommand(query);
                              break;

                         default:
                              break;
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
                    throw;
               }
          }

          /// <summary>
          /// Deletes the table.
          /// </summary>
          /// <param name="tableName">Name of the table.</param>
          /// <returns></returns>
          public bool DeleteTable(string tableName)
          {
               StringBuilder query = new StringBuilder();
               try
               {
                    if (!ExistsObjectInDataBase2(tableName))
                         return false;
                    switch (DataBase.Engine)
                    {
                         case DatabaseEngines.SqlServer:
                             query.AppendFormat ("DROP TABLE {0}", TableName(tableName));
                              ExecuteCommand(query);
                              return true;

                         default:
                              return false;
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
                    return false;
               }
          }

          /// <summary>
          /// Deletes the view.
          /// </summary>
          /// <param name="viewName">Name of the view.</param>
          /// <returns></returns>
          public bool DropView(string viewName)
          {
               StringBuilder query = new StringBuilder();
               try
               {
                    if (!ExistsObjectInDataBase2(viewName))
                         return true;
                    switch (DataBase.Engine)
                    {
                         case DatabaseEngines.SqlServer:
                              query = new StringBuilder();
                              query.AppendFormat (" USE [{0}]", DataBase.Catalog);
                              ExecuteCommand(query);
                              query = new StringBuilder();
                              query.AppendFormat ("DROP VIEW [dbo].[{1}]", DataBase.Catalog, viewName);
                              ExecuteCommand(query);
                              return true;

                         default:
                              return false;
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
                    return false;
               }
          }

          /// <summary>
          /// Obteners the propietario tabla.
          /// </summary>
          /// <param name="tableName">The ps objeto.</param>
          /// <returns></returns>
          public string GetTableOwner(string tableName)
          {
               StringBuilder query = new StringBuilder();
               try
               {
                    switch (DataBase.Engine)
                    {
                         case DatabaseEngines.SqlServer:
                             query.AppendFormat ("SELECT su.name FROM ");
                             query.AppendFormat (" sysobjects so JOIN sysusers su");
                             query.AppendFormat (" on so.uid = su.uid");
                             query.AppendFormat (" where so.name like '{0}'", tableName);
                              return ExecuteScalar<string>(query);

                         default:
                              return "";
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
                    throw;
               }
          }

          /// <summary>
          /// Existses the field in table.
          /// </summary>
          /// <param name="tableName">Name of the table.</param>
          /// <param name="fieldName">Name of the field.</param>
          /// <returns></returns>
          public bool ExistsFieldInTable(string tableName, string fieldName)
          {
               return ExistsFieldInTable(tableName, fieldName, false, "");
          }

          /// <summary>
          /// Existses the field in table.
          /// </summary>
          /// <param name="tableName">Name of the table.</param>
          /// <param name="fieldName">Name of the field.</param>
          /// <param name="isAzure">if set to <c>true</c> [is azure].</param>
          /// <returns></returns>
          public bool ExistsFieldInTable(string tableName, string fieldName, bool isAzure)
          {
               return ExistsFieldInTable(tableName, fieldName, isAzure, "");
          }

          /// <summary>
          /// Funcion que dertemirna si existe un campo en la tabla
          /// </summary>
          /// <param name="tableName">NOmbre de la tabla</param>
          /// <param name="fieldName">Nombre del campo</param>
          /// <param name="isAzure"></param>
          /// /// <param name="dataBaseName"></param>
          /// <returns></returns>
          public bool ExistsFieldInTable(string tableName, string fieldName, bool isAzure, string dataBaseName)
          {
               StringBuilder query = new StringBuilder();
               try
               {
                    switch (DataBase.Engine)
                    {
                         case DatabaseEngines.SqlServer:
                              if (isAzure)
                              {
                                   if (String.IsNullOrEmpty(dataBaseName))
                                   {
                                        query = new StringBuilder();
                                        //version windows azure
                                        query.AppendFormat("SELECT    count (sys.columns.name) Columna ");
                                        query.AppendFormat(" FROM sys.columns");
                                        query.AppendFormat(" INNER JOIN sys.objects ");
                                        query.AppendFormat(" ON  (sys.columns.object_id = sys.objects.object_id) ");
                                        query.AppendFormat(" LEFT OUTER JOIN sys.index_columns ");
                                        query.AppendFormat(" ON  (sys.columns.object_id = sys.index_columns.object_id  ");
                                        query.AppendFormat(" AND sys.columns.column_id = sys.index_columns.column_id  ");
                                        query.AppendFormat(" AND (sys.index_columns.index_id <= 1)) ");
                                        query.AppendFormat(" INNER JOIN sys.types ");
                                        query.AppendFormat(" ON  (sys.columns.system_type_id = sys.types.system_type_id  ");
                                        query.AppendFormat(" AND sys.types.system_type_id = sys.types.user_type_id) ");
                                        //Se hizo un upper para que la función no sea sensible a mayusculas y minusculas
                                        query.AppendFormat(" WHERE      UPPER(sys.objects.name) = '{0}' ", tableName.ToUpper());
                                        query.AppendFormat(" AND UPPER(sys.columns.name) = '{0}'", fieldName.ToUpper());
                                   }
                                   else
                                   {
                                        query = new StringBuilder();
                                        //version windows azure
                                        query.AppendFormat("SELECT    count ({0}.sys.columns.name) Columna ", dataBaseName);
                                        query.AppendFormat(" FROM {0}.sys.columns", dataBaseName);
                                        query.AppendFormat(" INNER JOIN {0}.sys.objects ", dataBaseName);
                                        query.AppendFormat(" ON  ({0}.sys.columns.object_id = {0}.sys.objects.object_id) ", dataBaseName);
                                        query.AppendFormat(" LEFT OUTER JOIN {0}.sys.index_columns ", dataBaseName);
                                        query.AppendFormat(" ON  ({0}.sys.columns.object_id = {0}.sys.index_columns.object_id  ", dataBaseName);
                                        query.AppendFormat(" AND {0}.sys.columns.column_id = {0}.sys.index_columns.column_id  ", dataBaseName);
                                        query.AppendFormat(" AND ({0}.sys.index_columns.index_id <= 1)) ", dataBaseName);
                                        query.AppendFormat(" INNER JOIN {0}.sys.types ", dataBaseName);
                                        query.AppendFormat(" ON  ({0}.sys.columns.system_type_id = {0}.sys.types.system_type_id  ", dataBaseName);
                                        query.AppendFormat(" AND {0}.sys.types.system_type_id = {0}.sys.types.user_type_id) ", dataBaseName);
                                        //Se hizo un upper para que la función no sea sensible a mayusculas y minusculas
                                        query.AppendFormat(" WHERE      UPPER({1}.sys.objects.name) = '{0}' ", tableName.ToUpper(), dataBaseName);
                                        query.AppendFormat(" AND UPPER({1}.sys.columns.name) = '{0}'", fieldName.ToUpper(), dataBaseName);
                                   }
                              }
                              else
                              {
                                   if (String.IsNullOrEmpty(dataBaseName))
                                   {
                                        query = new StringBuilder();
                                        query.AppendFormat("SELECT     count (syscolumns.name) Columna");
                                        query.AppendFormat(" FROM   syscolumns INNER JOIN");
                                        query.AppendFormat(" sysobjects ON ");
                                        query.AppendFormat(" (syscolumns.id = sysobjects.id) LEFT OUTER JOIN");
                                        query.AppendFormat(" sysindexkeys ON ");
                                        query.AppendFormat(" (syscolumns.id = sysindexkeys.id ");
                                        query.AppendFormat(" AND syscolumns.colid = sysindexkeys.colid ");
                                        query.AppendFormat(" AND (sysindexkeys.indid <= 1)) INNER JOIN");
                                        query.AppendFormat(" systypes ON ");
                                        query.AppendFormat(" (syscolumns.xtype = systypes.xtype ");
                                        query.AppendFormat(" AND systypes.xtype = systypes.xusertype)");
                                        query.AppendFormat(" WHERE     ");
                                        //Se hizo un upper para que la función no sea sensible a mayusculas y minusculas
                                        query.AppendFormat(" UPPER(sysobjects.name) = '{0}' ", tableName.ToUpper());
                                        query.AppendFormat(" AND UPPER(syscolumns.name) = '{0}'", fieldName.ToUpper());
                                   }
                                   else
                                   {
                                        query = new StringBuilder();
                                        query.AppendFormat("SELECT     count ({0}.dbo.syscolumns.name) Columna", dataBaseName);
                                        query.AppendFormat(" FROM   {0}.dbo.syscolumns INNER JOIN", dataBaseName);
                                        query.AppendFormat(" {0}.dbo.sysobjects ON ", dataBaseName);
                                        query.AppendFormat(" ({0}.dbo.syscolumns.id = {0}.dbo.sysobjects.id) LEFT OUTER JOIN", dataBaseName);
                                        query.AppendFormat(" {0}.dbo.sysindexkeys ON ", dataBaseName);
                                        query.AppendFormat(" ({0}.dbo.syscolumns.id = {0}.dbo.sysindexkeys.id ", dataBaseName);
                                        query.AppendFormat(" AND {0}.dbo.syscolumns.colid = {0}.dbo.sysindexkeys.colid ", dataBaseName);
                                        query.AppendFormat(" AND ({0}.dbo.sysindexkeys.indid <= 1)) INNER JOIN", dataBaseName);
                                        query.AppendFormat(" {0}.dbo.systypes ON ", dataBaseName);
                                        query.AppendFormat(" ({0}.dbo.syscolumns.xtype = {0}.dbo.systypes.xtype ", dataBaseName);
                                        query.AppendFormat(" AND {0}.dbo.systypes.xtype = {0}.dbo.systypes.xusertype)", dataBaseName);
                                        query.AppendFormat(" WHERE     ");
                                        //Se hizo un upper para que la función no sea sensible a mayusculas y minusculas
                                        query.AppendFormat(" UPPER({1}.dbo.sysobjects.name) = '{0}' ", tableName.ToUpper(), dataBaseName);
                                        query.AppendFormat(" AND UPPER({1}.dbo.syscolumns.name) = '{0}'", fieldName.ToUpper(), dataBaseName);
                                   }
                              }

                              return ExecuteScalar<int>(query) > 0;

                         default:
                              return false;
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
                    throw;
               }
          }

          /// <summary>
          /// ExistsRow
          /// </summary>
          /// <param name="tableName"></param>
          /// <param name="fieldName"></param>
          /// <param name="value"></param>
          /// <param name="specialFilter"></param>
          /// <returns></returns>
          public bool ExistsRow(string tableName, string fieldName, string value, ref string specialFilter)
          {

               StringBuilder query = new StringBuilder();
               if (tableName.Trim()?.Length == 0 || fieldName.Trim()?.Length == 0 || value.Trim()?.Length == 0)
               {
                    return false;
               }

               try
               {
                    switch (DataBase.Engine)
                    {
                         case DatabaseEngines.SqlServer:
                             query.AppendFormat ("SELECT {0} FROM [{1}].[{2}].[{3}] WHERE {4}={5} {6}", fieldName, DataBase.Catalog.ToUpper(), DataBase.Owner, tableName.ToUpper(), value, specialFilter);
                              return !String.IsNullOrEmpty(ExecuteScalar<string>(query));

                         default:
                              return false;
                    }
               }
               catch (Exception ex)
               {
                    RecordLog(ex, query);
                    throw;
               }
          }

          /// <summary>
          /// Regresas the schema tabla.
          /// </summary>
          /// <param name="tableName">The ps tabla.</param>
          /// <returns></returns>
          public string GetTableSchema(string tableName)
          {
               StringBuilder query = new StringBuilder();
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         query.AppendFormat(" Select Table_Schema from INFORMATION_SCHEMA.TABLES ");
                         query.AppendFormat(" where Table_Name = '{0}'", tableName);
                         return ExecuteScalar<string>(query);

                    default:
                         return "";
               }
          }

          /// <summary>
          /// Gets all tables generic table.
          /// </summary>
          /// <returns></returns>
          public GenericTable GetAllTablesGenericTable()
          {
               StringBuilder query = new StringBuilder();
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                        query.AppendFormat (" Select Table_Schema,Table_Name from INFORMATION_SCHEMA.TABLES ");
                        query.AppendFormat (" where table_type = 'BASE TABLE' and substring(TABLE_NAME,1,1) <> '_' and TABLE_NAME <> 'sysdiagrams'");
                        query.AppendFormat (" order by TABLE_SCHEMA,TABLE_NAME");
                         return GetGenericTableReader("Tables", query);

                    default:
                         return new GenericTable("Tables");
               }
          }

          /// <summary>
          /// Regresas the todaslas tablasdela bd.
          /// </summary>
          /// <returns></returns>
          public DataTable GetAllTablesInDataBase()
          {
               StringBuilder query = new StringBuilder();
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                        query.AppendFormat (" Select Table_Schema,Table_Name from INFORMATION_SCHEMA.TABLES ", DataBase.Catalog);
                        query.AppendFormat (" where table_type = 'BASE TABLE' and substring(TABLE_NAME,1,1) <> '_' ");
                        query.AppendFormat (" order by TABLE_SCHEMA,TABLE_NAME");
                         return GetDataTable(query);

                    default:
                         return new DataTable();
               }
          }

          public DataTable GetAllTablesPayrollSystemInDataBase(string database)
          {
               StringBuilder query = new StringBuilder();
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                        query.AppendFormat (" Select Table_Schema,Table_Name from {0}.INFORMATION_SCHEMA.TABLES ", database);
                        query.AppendFormat (" where table_type = 'BASE TABLE' and substring(TABLE_NAME,1,1) <> '_'  ");
                        query.AppendFormat (" and not TABLE_NAME like '{0}%' ", DataBase.DataBaseObjectPrefixLogData);
                        query.AppendFormat (" and (table_name like 'XTS%' or table_name like 'XTF%')");
                        query.AppendFormat (" order by TABLE_SCHEMA,TABLE_NAME");

                         return GetDataTable(query);

                    default:
                         return new DataTable();
               }
          }

          /// <summary>
          /// Armas the consulta definicio columanasdela tabla segun bd.
          /// </summary>
          /// <param name="psPropietario">The ps propietario.</param>
          /// <param name="psTabla">The ps tabla.</param>
          /// <returns></returns>
          private StringBuilder GetTableDefinitionQuery(string psPropietario, string psTabla)
          {
               StringBuilder query;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         query = new StringBuilder();
                         query.AppendFormat(" SELECT C.Name Name ,ST.Name DataType ,STNativo.Name NativeType  ,C.max_Length Length  ,C.precision Precision  ,C.column_id FieldId ");
                         query.AppendFormat(" ,CASE IsNull(PKC.Columna,'NULA') when 'NULA' then 0 else 1 END PrimaryKey  ,case C.Is_Nullable when 0 then 1 when 1 then 0 end IsRequiredInDataBase    ");
                         query.AppendFormat(" ,CASE ST.Name ");
                         query.AppendFormat("    WHEN 'int' THEN 'int'");
                         query.AppendFormat("    WHEN 'bit' THEN 'bool'");
                         query.AppendFormat("    WHEN 'binary' THEN 'int'");
                         query.AppendFormat("    WHEN 'varbinary' THEN 'int[]'");
                         query.AppendFormat("    WHEN 'image' THEN 'int[]'");
                         query.AppendFormat("    WHEN 'char' THEN 'string'");
                         query.AppendFormat("    WHEN 'nchar' THEN 'string'");
                         query.AppendFormat("    WHEN 'bigint' THEN 'long'");
                         query.AppendFormat("    WHEN 'smallint' THEN 'int'");
                         query.AppendFormat("    WHEN 'tinyint' THEN 'int'");
                         query.AppendFormat("    WHEN 'ntext' THEN 'string'");
                         query.AppendFormat("    WHEN 'varchar' THEN 'string'");
                         query.AppendFormat("    WHEN 'nvarchar' THEN 'string'");
                         query.AppendFormat("    WHEN 'text' THEN 'string'");
                         query.AppendFormat("    WHEN 'datetime' THEN 'DateTime'");
                         query.AppendFormat("    WHEN 'time' THEN 'DateTime'");
                         query.AppendFormat("    WHEN 'datetime2' THEN 'DateTime'");
                         query.AppendFormat("    WHEN 'datetimeoffset' THEN 'DateTimeOffset'");
                         query.AppendFormat("    WHEN 'smalldatetime' THEN 'DateTime'");
                         query.AppendFormat("    WHEN 'decimal' THEN 'double'");
                         query.AppendFormat("    WHEN 'numeric' THEN 'double'");
                         query.AppendFormat("    WHEN 'smallmoney' THEN 'double'");
                         query.AppendFormat("    WHEN 'money' THEN 'double'");
                         query.AppendFormat("    WHEN 'real' THEN 'double'");
                         query.AppendFormat("    WHEN 'float' THEN 'double'");
                         query.AppendFormat("    WHEN 'sql_variant' THEN 'object'");
                         query.AppendFormat(" ELSE 'string'");
                         query.AppendFormat(" END CSharpType");
                         query.AppendFormat(" ,CASE ST.Name ");
                         query.AppendFormat("    WHEN 'datetime' THEN 1");
                         query.AppendFormat("    WHEN 'time' THEN 1");
                         query.AppendFormat("    WHEN 'datetime2' THEN 1");
                         query.AppendFormat("    WHEN 'datetimeoffset' THEN 1");
                         query.AppendFormat("    WHEN 'smalldatetime' THEN 0");
                         query.AppendFormat(" ELSE 0");
                         query.AppendFormat(" END IncludeHours");
                         query.AppendFormat(" , replace(replace(isnull(CO.definition,'NULL'),'(',''),')','') DefaultValue ,SEP.value Description");
                         query.AppendFormat("  ,C.is_identity EsIdentity");
                         query.AppendFormat("  ,0 Virtual");
                         query.AppendFormat(" FROM sys.columns C ");

                         query.AppendFormat("    INNER JOIN sys.objects O ON (C.object_id = O.object_id) ");
                         query.AppendFormat("    INNER JOIN sys.types ST ON (C.system_type_id = ST.system_type_id AND C.user_type_id = ST.user_type_id) ");
                         query.AppendFormat("    INNER JOIN sys.types STNativo ON (C.system_type_id = STNativo.system_type_id  AND STNativo.system_type_id = STNativo.user_type_id) ");
                         query.AppendFormat(" LEFT JOIN (		");
                         query.AppendFormat(" select c.name as Columna ");
                         query.AppendFormat(" from   sys.indexes i ");
                         query.AppendFormat(" join   sys.objects o  ON i.object_id = o.object_id ");
                         query.AppendFormat(" join   sys.objects pk ON i.name = pk.name AND pk.parent_object_id = i.object_id AND pk.type = 'PK' ");
                         query.AppendFormat(" join   sys.index_columns ik on i.object_id = ik.object_id and i.index_id = ik.index_id ");
                         query.AppendFormat(" join   sys.columns c ON ik.object_id = c.object_id AND ik.column_id = c.column_id INNER JOIN sys.schemas z ON o.schema_id = Z.schema_id ");
                         query.AppendFormat(" where  o.name = '{0}' and z.name = '{1}') ", psTabla, psPropietario);
                         query.AppendFormat(" PKC on PKC.Columna=C.Name ");
                         query.AppendFormat(" Left join sys.default_constraints CO on (C.default_object_id = CO.object_id)");
                         query.AppendFormat(" left join  sys.extended_properties SEP on(C.object_id = SEP.major_id and  C.column_id = SEP.minor_id and SEP.name = 'MS_Description')");
                         query.AppendFormat(" INNER JOIN sys.schemas z ON o.schema_id = Z.schema_id ");
                         query.AppendFormat(" WHERE O.name = '{0}' and z.name = '{1}' ", psTabla, psPropietario);
                         return query;

                    default:
                         return new StringBuilder();
               }
          }

          /// <summary>
          /// GetExecQueries
          /// </summary>
          /// <returns></returns>
          public DataTable GetExecQueries()
          {
               StringBuilder query = new StringBuilder();
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                        query.AppendFormat ("SELECT(SELECT SUBSTRING(text, statement_start_offset / 2, (CASE WHEN statement_end_offset = -1 then LEN(CONVERT(nvarchar(max), text)) * 2 ELSE statement_end_offset end - statement_start_offset) / 2) FROM sys.dm_exec_sql_text(req.sql_handle)) AS query_text, ");
                        query.AppendFormat (" req.session_id, ");
                        query.AppendFormat (" req.status, ");
                        query.AppendFormat (" req.command, ");
                        query.AppendFormat (" req.cpu_time, ");
                        query.AppendFormat (" req.total_elapsed_time ");
                        query.AppendFormat (" ,   right(convert(varchar,  ");
                        query.AppendFormat ("             dateadd(ms, datediff(ms, P.last_batch, GETUTCDATE()), '1900-01-01'),  ");
                        query.AppendFormat ("             121), 12) as 'batch_duration' ");
                        query.AppendFormat (" ,   P.program_name ");
                        query.AppendFormat (" ,   P.hostname ");
                        query.AppendFormat (" ,   P.loginame ");
                        query.AppendFormat (" FROM sys.dm_exec_requests req ");
                        query.AppendFormat (" INNER JOIN master.dbo.sysprocesses P on req.session_id=P.spid ");
                        query.AppendFormat (" WHERE P.spid > 50 ");
                        query.AppendFormat ("         and      P.status not in ('background', 'sleeping') ");
                        query.AppendFormat ("         and      P.cmd not in ('AWAITING COMMAND' ");
                        query.AppendFormat ("                            ,'MIRROR HANDLER' ");
                        query.AppendFormat ("                            ,'LAZY WRITER' ");
                        query.AppendFormat ("                            ,'CHECKPOINT SLEEP' ");
                        query.AppendFormat ("                            ,'RA MANAGER') ");
                        query.AppendFormat (" AND p.spid != @@SPID "); //exclude this query
                        query.AppendFormat (" AND loginame != '' ");
                        query.AppendFormat (" order by batch_duration desc");
                         return GetDataTable(query);

                    default:
                         return new DataTable();
               }
          }

          /// <summary>
          /// GetQueryHistory
          /// </summary>
          /// <returns></returns>
          public DataTable GetQueryHistory()
          {
               StringBuilder query = new StringBuilder();
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                        query.AppendFormat ("SELECT ");
                        query.AppendFormat (" ( SELECT SUBSTRING(text,statement_start_offset/2,");
                        query.AppendFormat ("   (CASE WHEN statement_end_offset = -1 then LEN(CONVERT(nvarchar(max), text)) * 2 ELSE statement_end_offset end -statement_start_offset)/2 ) FROM sys.dm_exec_sql_text(sql_handle) ) AS query_text ");
                        query.AppendFormat (" , total_elapsed_time ");
                        query.AppendFormat (" , qs.execution_count AS NumberOfExecs ");
                        query.AppendFormat (" , (total_elapsed_time/execution_count)/1000 AS [Avg Exec Time in ms] ");
                        query.AppendFormat (" , max_elapsed_time/1000 AS [MaxExecTime in ms] ");
                        query.AppendFormat (" , min_elapsed_time/1000 AS [MinExecTime in ms] ");
                        query.AppendFormat (" , (total_worker_time/execution_count)/1000 AS [Avg CPU Time in ms] ");
                        query.AppendFormat (" , (total_logical_writes+total_logical_Reads)/execution_count AS [Avg Logical IOs] ");
                        query.AppendFormat (" , max_logical_reads AS MaxLogicalReads ");
                        query.AppendFormat (" , min_logical_reads AS MinLogicalReads ");
                        query.AppendFormat (" , max_logical_writes AS MaxLogicalWrites ");
                        query.AppendFormat (" , min_logical_writes AS MinLogicalWrites ");
                        query.AppendFormat (" FROM sys.dm_exec_query_stats qs ");
                        query.AppendFormat (" where max_elapsed_time/1000 > 0 ");
                        query.AppendFormat (" ORDER BY total_elapsed_time desc,[Avg Exec Time in ms] DESC;");
                         return GetDataTable(query);

                    default:
                         return new DataTable();
               }
          }

          /// <summary>
          /// GetFragmentIndexes
          /// </summary>
          /// <returns></returns>
          public DataTable GetFragmentIndexes(string psbaseDatos)
          {
               StringBuilder query = new StringBuilder();
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                        query.AppendFormat ("  select  ");
                        query.AppendFormat ("  object_name(ips.object_id) as Tabla");
                        query.AppendFormat (" , i.name as Indice");
                        query.AppendFormat (" , ips.page_count as PageCount");
                        query.AppendFormat (" , ips.page_count * 8 / 1024 as IndexSizeMB");
                        query.AppendFormat (" , ips.fragment_count as FragCount");
                        query.AppendFormat (" , ips.avg_fragmentation_in_percent as AvgFrag");
                        query.AppendFormat (" , ips.index_type_desc as IndexType");
                        query.AppendFormat (" from sys.dm_db_index_physical_stats(db_id(), NULL, NULL, NULL, NULL)   ips");
                        query.AppendFormat (" join sys.indexes i");
                        query.AppendFormat (" on ips.object_id = i.object_id");
                        query.AppendFormat (" and ips.index_id = i.index_id");
                        query.AppendFormat (" where i.index_id <> 0");
                        query.AppendFormat (" and ips.page_count > 0");
                        query.AppendFormat (" and db_name() = '{0}'", psbaseDatos);
                        query.AppendFormat (" order by FragCount desc");
                         return GetDataTable(query);

                    default:
                         return new DataTable();
               }
          }

          /// <summary>
          /// Regresas the definicio columanasdela tabla.
          /// </summary>
          /// <param name="owner">The ps propietario.</param>
          /// <param name="tableName">The ps tabla.</param>
          /// <returns></returns>
          public HashSet<ColumnDefinition> GetColumnDefinitionFromTable(string owner, string tableName)
          {
               return GetGenericCollectionData<ColumnDefinition>(GetTableDefinitionQuery(owner, tableName));
          }

          /// <summary>
          /// Funciopn que recura el nombre del servidor de la cadena de conexion
          /// </summary>
          /// <param name="connectionString"></param>
          private void LoadServerVariables(string connectionString)
          {
               SqlConnectionStringBuilder query = new SqlConnectionStringBuilder(connectionString);
               DataBase.Server = query.DataSource;
               DataBase.Catalog = query.InitialCatalog;
          }

          /// <summary>
          /// GetColumnDefinitionFromTable
          /// </summary>
          /// <param name="dataBaseName"></param>
          /// <param name="owner"></param>
          /// <param name="tableName"></param>
          /// <returns></returns>
          public DataTable GetColumnDefinitionFromTable(string dataBaseName, string owner, string tableName)
          {
               StringBuilder query = new StringBuilder();
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                        query.AppendFormat (" Select c.COLUMN_NAME,c.DATA_TYPE, isnull(c.Column_default,'')Column_default, isnull(c.character_maximum_length,0)character_maximum_length,isnull( c.numeric_precision,0)numeric_precision, c.is_nullable");
                        query.AppendFormat (", CASE WHEN pk.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 'PK' when pk.CONSTRAINT_TYPE = 'FOREIGN KEY' THEN 'FK' ELSE '' END AS KeyType");
                        query.AppendFormat (" FROM INFORMATION_SCHEMA.COLUMNS c");
                        query.AppendFormat (" LEFT JOIN (");
                        query.AppendFormat (" SELECT ku.TABLE_CATALOG, ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME, CONSTRAINT_TYPE");
                        query.AppendFormat (" FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc");
                        query.AppendFormat (" INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku");
                        query.AppendFormat (" ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME");
                        query.AppendFormat (" )   pk");
                        query.AppendFormat (" ON  c.TABLE_CATALOG = pk.TABLE_CATALOG");
                        query.AppendFormat (" AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA");
                        query.AppendFormat (" AND c.TABLE_NAME = pk.TABLE_NAME");
                        query.AppendFormat (" AND c.COLUMN_NAME = pk.COLUMN_NAME");

                         if (string.IsNullOrEmpty(dataBaseName))
                             query.AppendFormat (" where C.TABLE_CATALOG = '{0}'", DataBase.Catalog);
                         else
                             query.AppendFormat (" where C.TABLE_CATALOG = '{0}'", dataBaseName);
                         if (string.IsNullOrEmpty(owner))
                             query.AppendFormat (" and C.TABLE_SCHEMA = '{0}'", DataBase.Owner);
                         else
                             query.AppendFormat (" and C.TABLE_SCHEMA = '{0}'", owner);
                        query.AppendFormat (" and C.table_name = '{0}'", tableName);
                        query.AppendFormat (" ORDER BY c.TABLE_SCHEMA,c.TABLE_NAME, c.ORDINAL_POSITION");
                         return GetDataTable(query);

                    default:
                         return new DataTable();
               }
          }

          /// <summary>
          /// GetTableOrViewList
          /// </summary>
          /// <param name="dataBaseName"></param>
          /// <param name="type"></param>
          /// <returns></returns>
          public DataTable GetTableOrViewList(string dataBaseName, string type)
          {
               StringBuilder query = new StringBuilder();
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                        query.AppendFormat ("SELECT TABLE_SCHEMA,table_name FROM {0}.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = '{1}' order by TABLE_SCHEMA,table_name", dataBaseName, type);
                         return GetDataTable(query);

                    default:
                         return new DataTable();
               }
          }

          /// <summary>
          /// GetFunctionsOrProceduresList
          /// </summary>
          /// <param name="dataBaseName"></param>
          /// <param name="type"></param>
          /// <returns></returns>
          public DataTable GetFunctionsOrProceduresList(string dataBaseName, string type)
          {
               StringBuilder query = new StringBuilder();
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                        query.AppendFormat ("SELECT specific_schema,specific_name FROM {0}.INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE='{1}' order by specific_schema,specific_name", dataBaseName, type);
                         return GetDataTable(query);

                    default:
                         return new DataTable();
               }
          }

          /// <summary>
          /// SelectLog
          /// </summary>
          /// <returns></returns>
          public DataTable SelectLog()
          {
               StringBuilder query = new StringBuilder();
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                        query.AppendFormat ("Select * from {0} order by id desc", TableName(_LogTable));
                         return GetDataTable(query);

                    default:
                         return new DataTable();
               }
          }

          /// <summary>
          /// GetTableSize
          /// </summary>
          /// <returns></returns>
          public DataTable GetTableSize()
          {
               StringBuilder query = new StringBuilder();
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                        query.AppendFormat ("SELECT     X.[name],");
                        query.AppendFormat (" REPLACE(CONVERT(varchar, CONVERT(money, X.[rows]), 1), '.00', '')        AS[rows],");
                        query.AppendFormat (" REPLACE(CONVERT(varchar, CONVERT(money, X.[reserved]), 1), '.00', '')    AS[reserved],");
                        query.AppendFormat (" REPLACE(CONVERT(varchar, CONVERT(money, X.[data]), 1), '.00', '')        AS[data],");
                        query.AppendFormat (" REPLACE(CONVERT(varchar, CONVERT(money, X.[index_size]), 1), '.00', '')  AS[index_size],");
                        query.AppendFormat (" REPLACE(CONVERT(varchar, CONVERT(money, X.[unused]), 1), '.00', '')      AS[unused]");
                        query.AppendFormat (" FROM");
                        query.AppendFormat (" (SELECT     CAST(object_name(id) AS varchar(50))        AS[name],");
                        query.AppendFormat (" SUM(CASE WHEN indid < 2 THEN CONVERT(bigint, [rows]) END)        AS[rows],");
                        query.AppendFormat (" SUM(CONVERT(bigint, reserved)) * 8        AS reserved,");
                        query.AppendFormat (" SUM(CONVERT(bigint, dpages)) * 8        AS data,");
                        query.AppendFormat (" SUM(CONVERT(bigint, used) - CONVERT(bigint, dpages)) * 8        AS index_size,");
                        query.AppendFormat (" SUM(CONVERT(bigint, reserved) - CONVERT(bigint, used)) * 8        AS unused");
                        query.AppendFormat (" FROM sysindexes WITH(NOLOCK)");
                        query.AppendFormat (" WHERE sysindexes.indid IN(0, 1, 255)");
                        query.AppendFormat (" AND sysindexes.id > 100");
                        query.AppendFormat (" AND object_name(sysindexes.id) <> 'dtproperties'");
                        query.AppendFormat (" GROUP BY sysindexes.id WITH ROLLUP");
                        query.AppendFormat (" ) AS X");
                        query.AppendFormat (" WHERE X.[name] is not null");
                        query.AppendFormat (" ORDER BY X.[rows] DESC");

                         return GetDataTable(query);

                    default:
                         return new DataTable();
               }
          }

          /// <summary>
          /// GetSugestedIndexes
          /// </summary>
          /// <returns></returns>
          public DataTable GetSugestedIndexes()
          {
               StringBuilder query = new StringBuilder();
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                        query.AppendFormat (" SELECT Db_name( mid.database_id )                                                                                          AS DatabaseID,");
                        query.AppendFormat (" CONVERT ( DECIMAL(28, 1), migs.avg_total_user_cost * migs.avg_user_impact * ( migs.user_seeks + migs.user_scans ) ) AS improvement_measure,");
                        query.AppendFormat (" 'CREATE INDEX missing_index_'");
                        query.AppendFormat (" + CONVERT (VARCHAR, mig.index_group_handle)");
                        query.AppendFormat (" + '_' + CONVERT (VARCHAR, mid.index_handle)");
                        query.AppendFormat (" + ' ON ' + mid.statement + ' ('");
                        query.AppendFormat (" + Isnull(mid.equality_columns, '') + CASE WHEN mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL THEN ',' ELSE '' END");
                        query.AppendFormat (" + Isnull(mid.inequality_columns, '') + ')'");
                        query.AppendFormat (" + Isnull(' INCLUDE (' + mid.included_columns + ')', '')                                                             AS create_index_statement,");
                        query.AppendFormat (" migs.user_seeks,");
                        query.AppendFormat (" migs.user_scans,");
                        query.AppendFormat (" Isnull( CONVERT ( INT, ( ");
                        query.AppendFormat ("                                         SELECT Sum( rows )");
                        query.AppendFormat ("                                           FROM   sys.PARTITIONS s_p");
                        query.AppendFormat ("                          WHERE  mid.object_id = s_p.object_id AND");
                        query.AppendFormat ("                s_p.index_id = 1  ");
                        query.AppendFormat (" ) ), 0 )                                                                                    AS estimated_table_rows,");
                        query.AppendFormat (" sus.user_updates + sus.system_updates                                                                               AS rows_updated");
                        query.AppendFormat (" FROM   sys.DM_DB_MISSING_INDEX_GROUPS mig");
                        query.AppendFormat (" INNER JOIN sys.DM_DB_MISSING_INDEX_GROUP_STATS migs");
                        query.AppendFormat (" ON migs.group_handle = mig.index_group_handle");
                        query.AppendFormat (" INNER JOIN sys.DM_DB_MISSING_INDEX_DETAILS mid");
                        query.AppendFormat (" ON mig.index_handle = mid.index_handle");
                        query.AppendFormat (" LEFT JOIN sys.DM_DB_INDEX_USAGE_STATS sus");
                        query.AppendFormat (" ON sus.index_id = 1 ");
                        query.AppendFormat (" AND");
                        query.AppendFormat (" sus.object_id = mid.object_id AND");
                        query.AppendFormat (" sus.database_id = mid.database_id");
                        query.AppendFormat (" WHERE  mid.database_id = Db_id( ) AND");
                        query.AppendFormat (" CONVERT ( DECIMAL(28, 1), migs.avg_total_user_cost * migs.avg_user_impact * ( migs.user_seeks + migs.user_scans ) ) > 10 ");
                         return GetDataTable(query);

                    default:
                         return new DataTable();
               }
          }

          /// <summary>
          /// DinamicSelect
          /// </summary>
          /// <param name="dataBaseName"></param>
          /// <param name="owner"></param>
          /// <param name="tableName"></param>
          /// <returns></returns>
          public StringBuilder DinamicSelect(string dataBaseName, string owner, string tableName)
          {
               StringBuilder query = new StringBuilder();
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                        query.AppendFormat (" Select c.COLUMN_NAME");
                        query.AppendFormat (" FROM {0}.INFORMATION_SCHEMA.COLUMNS c", dataBaseName);
                        query.AppendFormat (" where C.TABLE_CATALOG = '{0}'", dataBaseName);
                        query.AppendFormat (" and C.TABLE_SCHEMA = '{0}'", owner);
                        query.AppendFormat (" and C.table_name = '{0}'", tableName);
                        query.AppendFormat (" ORDER BY c.TABLE_SCHEMA,c.TABLE_NAME, c.ORDINAL_POSITION");
                         query = new StringBuilder();
                         query.AppendFormat("SELECT TOP(10) ");
                         using (var table = GetGenericTableReader("table", query))
                         {
                              foreach (Row rowColumn in table.Rows)
                              {
                                   query.AppendFormat("[{0}],", Convert.ToString(rowColumn["COLUMN_NAME"]).ToUpper());
                              }
                         }
                         query.Remove(query.Length - 1, 1);
                         query.AppendFormat(" FROM {0}", TableName(tableName));
                         return query;

                    default:
                         return new StringBuilder();
               }
          }

          /// <summary>
          /// GetTablesThatNotHaveLog
          /// </summary>
          /// <returns></returns>
          public DataTable GetTablesThatNotHaveLog()
          {
               StringBuilder query = new StringBuilder();
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                        query.AppendFormat ("Select Table_Schema,Table_Name from [{0}].INFORMATION_SCHEMA.TABLES  ", DataBase.Catalog);
                        query.AppendFormat (" where table_type = 'BASE TABLE' and substring(TABLE_NAME,1,1) <> '_' ");
                        query.AppendFormat (" and not (TABLE_SCHEMA+'.'+TABLE_NAME )  COLLATE Modern_Spanish_CI_AS ");
                        query.AppendFormat (" in (Select TABLE_SCHEMA+'.'+ replace(TABLE_NAME,'{0}','') from ", DataBase.DataBaseObjectPrefixLogData);
                         if (!String.IsNullOrEmpty(DataBase.LogData))
                             query.AppendFormat (" [{0}].INFORMATION_SCHEMA.TABLES ", DataBase.LogData);
                         else
                             query.AppendFormat (" [{0}].INFORMATION_SCHEMA.TABLES ", DataBase.Catalog);
                        query.AppendFormat (" where table_type = 'BASE TABLE' ");
                        query.AppendFormat ("   and substring(TABLE_NAME,1,1) <> '_' ");
                        query.AppendFormat ("   and TABLE_NAME like '{0}[_]%' )", DataBase.DataBaseObjectPrefixLogData.Replace("_", ""));
                         //lsQuery.AppendFormat("   and not TABLE_NAME in (SELECT OBJECT_NAME(OBJECT_ID) AS TableName FROM  SYS.COLUMNS WHERE is_identity = 1) ");
                        query.AppendFormat (" order by TABLE_SCHEMA,TABLE_NAME");
                         return GetDataTable(query);

                    default:
                         return new DataTable();
               }
          }

          /// <summary>
          /// CreateQueryForHistoryTable
          /// </summary>
          /// <param name="owner"></param>
          /// <param name="tableName"></param>
          /// <param name="tableDefinition"></param>
          /// <returns></returns>
          public void CreateQueryForHistoryTable(string owner, string tableName, HashSet<ColumnDefinition> tableDefinition)
          {
               StringBuilder query = new StringBuilder();
               string serverName;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                        query.AppendFormat ("Select  ");
                        query.AppendFormat (" IDENTITY(INT, 1, 1) AS _Folio");
                        query.AppendFormat (", convert(varchar(50),'MASTER') _USUABREVIACION");
                        query.AppendFormat (", 0 _USUCLAVE");
                        query.AppendFormat (", GETUTCDATE() _FECHA");
                        query.AppendFormat (", 1 _MOVIMIENTO");
                        query.AppendFormat (", '' XMLCAMPO");
                         foreach (ColumnDefinition loColumna in tableDefinition)
                         {
                              if (!loColumna.EsIdentity)
                                  query.AppendFormat (", [{0}].[{1}].[{2}].[{3}] ", DataBase.Catalog, owner, tableName, loColumna.Name.ToUpper());
                              else
                                  query.AppendFormat (", cast([{0}].[{1}].[{2}].[{3}] as int) {3} ", DataBase.Catalog, owner, tableName, loColumna.Name.ToUpper());
                         }
                         if (String.IsNullOrEmpty(DataBase.LogData))
                             query.AppendFormat (" into [{2}].[{3}].[{0}{1}]", DataBase.DataBaseObjectPrefixLogData, tableName, DataBase.Catalog, owner);
                         else
                             query.AppendFormat (" into [{0}].[{3}].[{2}{1}]", DataBase.LogData, tableName, DataBase.DataBaseObjectPrefixLogData, owner);
                        query.AppendFormat (" From [{1}].[{2}].[{0}];", tableName, DataBase.Catalog, owner);
                         if (String.IsNullOrEmpty(DataBase.LogData))
                              serverName = DataBase.Catalog;
                         else
                              serverName = DataBase.LogData;
                        query.AppendFormat (" Alter table [{2}].[{3}].[{1}{0}] Alter Column _FOLIO int not null;",
                              tableName, DataBase.DataBaseObjectPrefixLogData, serverName, owner);
                        query.AppendFormat (" Alter table [{2}].[{3}].[{1}{0}] Alter Column _USUABREVIACION varchar(50) not null;",
                              tableName, DataBase.DataBaseObjectPrefixLogData, serverName, owner);
                        query.AppendFormat (" Alter table [{2}].[{3}].[{1}{0}] Alter Column _USUCLAVE int not null;",
                              tableName, DataBase.DataBaseObjectPrefixLogData, serverName, owner);
                        query.AppendFormat (" Alter table [{2}].[{3}].[{1}{0}] Alter Column _FECHA datetime not null;",
                              tableName, DataBase.DataBaseObjectPrefixLogData, serverName, owner);
                        query.AppendFormat (" Alter table [{2}].[{3}].[{1}{0}] Alter Column _MOVIMIENTO int not null;",
                              tableName, DataBase.DataBaseObjectPrefixLogData, serverName, owner);
                        query.AppendFormat (" Alter table [{2}].[{3}].[{1}{0}] Alter Column XMLCAMPO xml null;",
                              tableName, DataBase.DataBaseObjectPrefixLogData, serverName, owner);
                        query.AppendFormat (" Alter table [{2}].[{3}].[{1}{0}] Add  Constraint [DF_{1}{0}_FECHA]  DEFAULT (getdate()) FOR [_FECHA];",
                              tableName, DataBase.DataBaseObjectPrefixLogData, serverName, owner);

                         foreach (ColumnDefinition loColumna in tableDefinition)
                         {
                              if (loColumna.EsIdentity)
                                  query.AppendFormat (" Alter table [{0}].[{1}].[{2}{3}] Alter Column {4} int not null;", serverName, owner, DataBase.DataBaseObjectPrefixLogData, tableName, loColumna.Name.ToUpper());
                         }

                        query.AppendFormat (" Alter table [{2}].[{3}].[{1}{0}] ADD CONSTRAINT [PK_{1}{0}] PRIMARY KEY CLUSTERED( ", tableName, DataBase.DataBaseObjectPrefixLogData, serverName, owner);
                        query.AppendFormat (" [_FOLIO] ASC,");
                        query.AppendFormat (" [_USUABREVIACION] ASC,");
                        query.AppendFormat (" [_USUCLAVE] ASC,");
                        query.AppendFormat (" [_FECHA] ASC,");
                        query.AppendFormat (" [_MOVIMIENTO] ASC");
                         foreach (ColumnDefinition loColumna in tableDefinition)
                         {
                              if (loColumna.PrimaryKey == 1)
                                  query.AppendFormat (", [{0}] ASC", loColumna.Name.ToUpper());
                         }
                        query.AppendFormat (" );");
                         ExecuteCommand(query);
                         break;

                    default:
                         break;
               }
          }

          /// <summary>
          /// Validates the size of the primary key.
          /// </summary>
          /// <param name="owner">The owner.</param>
          /// <param name="tableName">Name of the table.</param>
          /// <param name="tableDefinition">The table definition.</param>
          /// <returns></returns>
          public bool ValidatePrimaryKeySize(string owner, string tableName, HashSet<ColumnDefinition> tableDefinition)
          {
               StringBuilder query = new StringBuilder();
               string serverName;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         if (String.IsNullOrEmpty(DataBase.LogData))
                              serverName = DataBase.Catalog;
                         else
                              serverName = DataBase.Catalog;
                        query.AppendFormat ("SELECT ISNULL(SUM(max_length),0)AS TotalIndexKeySize");
                        query.AppendFormat (" FROM {0}.sys.columns", serverName);
                        query.AppendFormat (" WHERE ");
                        query.AppendFormat (" NAME IN (");
                        query.AppendFormat (" '_FOLIO', '_USUABREVIACION', '_USUCLAVE', '_FECHA', '_MOVIMIENTO' ");
                         foreach (ColumnDefinition loColumna in tableDefinition)
                         {
                              if (loColumna.PrimaryKey == 1)
                                  query.AppendFormat (", '{0}'", loColumna.Name.ToUpper());
                         }
                        query.AppendFormat (" )");
                        query.AppendFormat (" AND object_id = OBJECT_ID('{2}.{3}.{1}{0}')",
                              tableName, DataBase.DataBaseObjectPrefixLogData, serverName, owner);
                         return ExecuteScalar<int>(query) < 128;

                    default:
                         return false;
               }
          }

          /// <summary>
          /// CreateQueryForCreateHistoryTableIndexes
          /// </summary>
          /// <param name="owner"></param>
          /// <param name="tableName"></param>
          /// <returns></returns>
          public void CreateQueryForCreateHistoryTableIndexes(string owner, string tableName)
          {
               StringBuilder query = new StringBuilder();
               string serverName;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         if (String.IsNullOrEmpty(DataBase.Catalog))
                              serverName = DataBase.Catalog;
                         else
                              serverName = DataBase.LogData;
                         // Create primary index.
                         query.AppendFormat(" CREATE PRIMARY XML INDEX idx_XMLCAMpo{1}{0} on [{2}].[{3}].[{1}{0}](XMLCAMPO)",
                          tableName, DataBase.DataBaseObjectPrefixLogData, serverName, owner);
                         // Create secondary indexes (PATH, VALUE, PROPERTY).
                         query.AppendFormat(" CREATE XML INDEX idx_XMLCAMpo{1}{0}_PATH ON [{2}].[{3}].[{1}{0}](XMLCAMPO)",
                               tableName, DataBase.DataBaseObjectPrefixLogData, serverName, owner);
                         query.AppendFormat(" USING XML INDEX idx_XMLCAMpo{1}{0}",
                               tableName, DataBase.DataBaseObjectPrefixLogData);
                         query.AppendFormat(" FOR PATH;");
                         query.AppendFormat(" CREATE XML INDEX idx_XMLCAMpo{1}{0}_VALUE ON [{2}].[{3}].[{1}{0}](XMLCAMPO)",
                               tableName, DataBase.DataBaseObjectPrefixLogData, serverName, owner);
                         query.AppendFormat(" USING XML INDEX idx_XMLCAMpo{1}{0}", tableName, DataBase.DataBaseObjectPrefixLogData);
                         query.AppendFormat(" FOR VALUE;");
                         query.AppendFormat(" CREATE XML INDEX idx_XMLCAMpo{1}{0}_PROPERTY ON [{2}].[{3}].[{1}{0}](XMLCAMPO)",
                               tableName, DataBase.DataBaseObjectPrefixLogData, serverName, owner);
                         query.AppendFormat(" USING XML INDEX idx_XMLCAMpo{1}{0}",
                               tableName, DataBase.DataBaseObjectPrefixLogData);
                         query.AppendFormat(" FOR PROPERTY;");

                         ExecuteCommand(query);
                         break;

                    default:

                         break;
               }
          }

          public bool CanCreateSociety(string id)
          {
               List<ParameterSql> parameters;
               StringBuilder query = new StringBuilder();
               query.AppendFormat(" Select CreateSociety from {0} where UserId = @Id", TableName("Users"));
               parameters = new List<ParameterSql>
               {
                    new ParameterSql("@Id", id)
               };
               return ExecuteScalar<string>(query, parameters).ToBoolean();
          }
         
          public int GetUserNumber(int company,string uuIdUser)
          {
               List<ParameterSql> parameters;
               StringBuilder query = new StringBuilder();
               query.AppendFormat(" Select  distinct usuClave from {0} where uuid = @Uuid and usuEmpresa = @Empresa", TableName("xtsUsuarios"));
               parameters = new List<ParameterSql>
               {
                    new ParameterSql("@Uuid", uuIdUser),
                    new ParameterSql("@Empresa", company)
               };
               return ExecuteScalar<int>(query, parameters);
          }

          public int GetCompanyDefault(string uuIdUser)
          {
               List<ParameterSql> parameters;
               StringBuilder query = new StringBuilder();
               query.AppendFormat(" Select  TOP 1 usuEmpresa from {0} where uuid = @Uuid and usuStatus = 1 order by usuEmpresa asc", TableName("xtsUsuarios"));
               parameters = new List<ParameterSql>
               {
                    new ParameterSql("@Uuid", uuIdUser)
               };
               return ExecuteScalar<int>(query, parameters);
          }

          public int GetPayrollTypeDefault(int company, string uuIdUser)
          {
               List<ParameterSql> parameters;
               //StringBuilder query = new StringBuilder();
               //query.AppendFormat("  Select  TOP 1 TipD002 from {0} where TipD000 = ", TableName("TIPOSEMPD"));
               //query.AppendFormat(" (Select  TOP 1 usuEmpresa from {0} where uuid = @Uuid and usuEmpresa = @usuEmpresa) ", TableName("xtsUsuarios"));
               //query.AppendFormat(" and TipD001 = (Select  TOP 1 usuTipoNomina from {0} where uuid = @Uuid and UsuEmpresa = ", TableName("xtsUsuarios"));
               //query.AppendFormat(" (Select  TOP 1 usuEmpresa from {0} where uuid = @Uuid and usuEmpresa = @usuEmpresa))", TableName("xtsUsuarios"));
               parameters = new List<ParameterSql>
               {
                    new ParameterSql("@Uuid", uuIdUser),
                    new ParameterSql("@usuEmpresa", company)
               };
               //int value = ExecuteScalar<int>(query, parameters);
               //if(value == 0)
               //{
               StringBuilder query = new StringBuilder();
               query.AppendFormat(" Select  usuTipoNomina from {0} where uuid = @Uuid and UsuEmpresa =  @usuEmpresa", TableName("xtsUsuarios"));
               int value = ExecuteScalar<int>(query, parameters);
               //}
               return value;
          }
          public int GetLanguageDefault(int company,string uuIdUser)
          {
               List<ParameterSql> parameters;
               StringBuilder query = new StringBuilder();
               query.AppendFormat(" Select  TOP 1 usuIdioma from {0} where uuid = @Uuid and usuEmpresa = @usuEmpresa", TableName("xtsUsuarios"));
               parameters = new List<ParameterSql>
               {
                    new ParameterSql("@Uuid", uuIdUser),
                    new ParameterSql("@usuEmpresa", company)
               };
               return ExecuteScalar<int>(query, parameters);
          }
          public string GetStringConnection(string InitialCatalog)
          {
               SqlConnectionStringBuilder sqlConnection;
               switch (DataBase.Engine)
               {
                    case DatabaseEngines.SqlServer:
                         sqlConnection = new SqlConnectionStringBuilder(DataBase.StringConnection);
                         return string.Format("Data Source={0};Initial Catalog={1};User ID={2};Password={3}", sqlConnection.DataSource, InitialCatalog, sqlConnection.UserID, sqlConnection.Password);
                    default:
                         sqlConnection = new SqlConnectionStringBuilder(DataBase.StringConnection);
                         return string.Format("Data Source={0};Initial Catalog={1};User ID={2};Password={3}", sqlConnection.DataSource, InitialCatalog, sqlConnection.UserID, sqlConnection.Password);
               }
          }

          #endregion Methods
     }
}