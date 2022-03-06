using Framework.DataBase.Utilities;
using Framework.Extensions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Framework.DataBase
{
    /// <summary>
    /// Clase para  manejar dinamicamente tablas del sistema. El proposito de esta tabla generica es poder serializarla en stateserver de asp.net
    /// </summary>
    [Serializable]
     public class GenericTable : IDisposable
     {
          #region Variables

          /// <summary>
          /// Lista de las columnas de la tabla
          /// </summary>
          protected HashSet<string> _columnNames;

          /// <summary>
          /// The column types
          /// </summary>
          protected Dictionary<string, Type> _columnTypes;

          /// <summary>
          /// The column expresion
          /// </summary>
          protected Dictionary<string, string> _columnExpresion;

          /// <summary>
          /// Registros de la tabla
          /// </summary>
          protected HashSet<Row> _rows;

          /// <summary>
          /// Nombre de la tabla
          /// </summary>
          protected string _tableName;

          /// <summary>
          /// The is disposed
          /// </summary>
          protected bool _isDisposed;

          /// <summary>
          /// Total de filas de la tabla
          /// </summary>
          protected int _totalRows;

          /// <summary>
          /// Total de columnas de la tabla
          /// </summary>
          protected int _totalColumns;

          /// <summary>
          /// Consulta que genero el registro
          /// </summary>
          protected string _querySource;

          /// <summary>
          /// Numero de fila
          /// </summary>
          protected int _indexRow;

          #endregion Variables

          #region Properties

          /// <summary>
          /// Gets the indexrow.
          /// </summary>
          /// <value>
          /// The indexrow.
          /// </value>
          public int Indexrow
          {
               get
               {
                    return _indexRow;
               }
          }

          /// <summary>
          /// Lista de las columnas de la tabla
          /// </summary>
          public HashSet<Row> Rows
          {
               get
               {
                    return _rows;
               }
          }

          /// <summary>
          /// Lista con losbombre de la columna
          /// </summary>
          public HashSet<string> ColumnNames
          {
               get
               {
                    return _columnNames;
               }
          }

          public Dictionary<string, Type> ColumnTypes
          {
               get
               {
                    return _columnTypes;
               }
          }

          public Dictionary<string, string> ColumnExpresion
          {
               get
               {
                    return _columnExpresion;
               }
          }

          /// <summary>
          /// NOmbre de la tabla
          /// </summary>
          public string TableName
          {
               get
               {
                    return _tableName;
               }
          }

          /// <summary>
          /// Total de filas
          /// </summary>
          public int TotalRows
          {
               get
               {
                    return _totalRows;
               }
          }

          /// <summary>
          /// Total de columnas
          /// </summary>
          public int TotalColumns
          {
               get
               {
                    return _totalColumns;
               }
          }

          /// <summary>
          /// Consulta origen
          /// </summary>
          public string QuerySource
          {
               get
               {
                    return _querySource;
               }
          }

          /// <summary>
          /// Acceder a los registros de la tabla
          /// </summary>
          /// <param name="row">Numero de la fila</param>
          /// <param name="column">NUmero de la colimna</param>
          /// <returns></returns>
          public object this[int row, int column]
          {
               get
               {
                    return _rows.ElementAt(row).RowValue[_columnNames.ElementAt(column)];
               }
               set
               {
                    _rows.ElementAt(row).RowValue[_columnNames.ElementAt(column)] = value;
               }
          }

          /// <summary>
          /// Acceder a los registros de la tabla
          /// </summary>
          /// <param name="row">Numero de fila</param>
          /// <param name="column">NOmbre columna</param>
          /// <returns></returns>
          public object this[int row, string column]
          {
               get
               {
                    return _rows.ElementAt(row).RowValue[column.ToLower()];
               }
               set
               {
                    _rows.ElementAt(row).RowValue[column.ToLower()] = value;
               }
          }

          #endregion Properties

          #region Builders

          private void InternalBuilder()
          {
               _rows = new HashSet<Row>();
               _columnNames = new HashSet<string>();
               _columnExpresion = new Dictionary<string, string>();
               _columnTypes = new Dictionary<string, Type>();
          }

          /// <summary>
          /// Crea una instancia de la TablaGenreica
          /// </summary>
          /// <param name="tableName">NOmbre de la tabla</param>
          public GenericTable(string tableName)
          {
               _tableName = tableName;
               InternalBuilder();
          }

          /// <summary>
          /// Initializes a new instance of the <see cref="GenericTable"/> class.
          /// </summary>
          /// <param name="dataTable">The po tabla.</param>
          /// <param name="tableName">The ps nombre tabla.</param>
          /// <param name="query">The ps query.</param>
          public GenericTable(DataTable dataTable, string tableName, string query)
          {
               _tableName = tableName;
               InternalBuilder();
               ConvertDataTable2List2(dataTable, query);
          }

          /// <summary>
          /// Crea una nueva instancia de la TablaGenerica
          /// </summary>
          public GenericTable()
          {
               _tableName = "TablaGenerica";
               InternalBuilder();
          }

          /// <summary>
          /// Crea una nueva instancia de la tabla generica con un data table
          /// </summary>
          /// <param name="dataTable">Tabla con valores</param>
          public GenericTable(DataTable dataTable)
          {
               _tableName = "TablaGenerica";
               InternalBuilder();
               ConvertDataTable2List2(dataTable, "");
          }

          #endregion Builders

          #region Functions

          /// <summary>
          /// Regresas the valor.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="indexRow">The pi columna.</param>
          /// <param name="columnName">The ps columna.</param>
          /// <returns></returns>
          public T GetValue<T>(int indexRow, string columnName)
          {
               Object poValor = _rows.ElementAt(indexRow).RowValue[columnName.ToLower()];
               if (Convert.IsDBNull(poValor) || poValor == null)
                    return ObjectsUtilities.GetDefaultValue<T>();
               return (T)Convert.ChangeType(poValor, typeof(T));
          }

          /// <summary>
          /// Regresas the valor.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="columnName">The ps columna.</param>
          /// <returns></returns>
          public T GetValue<T>(string columnName)
          {
               Object poValor = _rows.ElementAt(_indexRow).RowValue[columnName.ToLower()];
               if (Convert.IsDBNull(poValor) || String.IsNullOrEmpty(Convert.ToString(poValor).Trim()))
                    return ObjectsUtilities.GetDefaultValue<T>();
               return (T)Convert.ChangeType(poValor, typeof(T));
          }

          public object GetValue(string columnName)
          {
               return _rows.ElementAt(_indexRow).RowValue[columnName.ToLower()];
          }
          public Row GetCurrentRow()
          {
               return _rows.ElementAt(_indexRow);
          }
          /// <summary>
          /// Regresa el tipo de dato en la columna
          /// </summary>
          /// <param name="indexColumn"></param>
          /// <returns></returns>
          public Type TipoDatoColumna(int indexColumn)
          {
               return _rows.ElementAt(0).RowValue[_columnNames.ElementAt(indexColumn)].GetType();
          }

          /// <summary>
          /// Tipoes the dato columna.
          /// </summary>
          /// <param name="columnName">The ps columna.</param>
          /// <returns></returns>
          public Type TipoDatoColumna(string columnName)
          {
               return _rows.ElementAt(0).RowValue[columnName].GetType();
          }

          /// <summary>
          /// Funcion que convierte un DataTable en TablaGenerica
          /// </summary>
          /// <param name="dataTable"></param>
          /// <param name="query"></param>
          protected internal void ConvertDataTable2List2(DataTable dataTable, string query)
          {
               Row typedRow;
               string columnName;
               _querySource = query;
               _totalColumns = dataTable.Columns.Count;
               _totalRows = dataTable.Rows.Count;
               foreach (DataRow loFila in dataTable.Rows)
               {
                    typedRow = new Row();
                    foreach (DataColumn loColumna in loFila.Table.Columns)
                    {
                         columnName = loColumna.ColumnName.ToLower();
                         _columnNames.Add(columnName);
                         if (!_columnTypes.ContainsKey(columnName.ToLower()))
                              _columnTypes.Add(columnName, loColumna.DataType);
                         if (!_columnExpresion.ContainsKey(columnName.ToLower()))
                              _columnExpresion.Add(columnName, loColumna.Expression);
                         typedRow.RowValue[columnName] = loFila[loColumna.ColumnName];
                    }
                    _rows.Add(typedRow);
               }
               _columnNames.TrimExcess();
               _rows.TrimExcess();
          }

          /// <summary>
          /// Creates the generic table from query.
          /// </summary>
          /// <param name="data">The data.</param>
          /// <param name="query">The query.</param>
          protected internal void CreateGenericTableFromQuery(IDataReader data, StringBuilder query)
          {
               IEnumerable<string> columnNames;
               DataTable columns;
               Row fila;
               _querySource = query.ToString();
               columnNames = data.GetSchemaTable()
                                       .Rows
                                       .Cast<DataRow>()
                                       .Select(propertie => Convert.ToString(propertie["ColumnName"]).ToLower());
               columns = data.GetSchemaTable();

               while (data.Read())
               {
                    fila = new Row();
                    foreach (string column in columnNames)
                    {
                         _columnNames.Add(column.ToLower());
                         if (!_columnTypes.ContainsKey(column.ToLower()))
                              _columnTypes.Add(column.ToLower(), data.GetFieldType(data.GetOrdinal(column)));
                         if (!_columnExpresion.ContainsKey(column.ToLower()))
                              _columnExpresion.Add(column.ToLower(), "");
                         fila.RowValue[column] = data[column];
                    }
                    _rows.Add(fila);
               }
               _totalColumns = columnNames.Count();
               _totalRows = _rows.Count;
          }

          /// <summary>
          /// Funcion que determina si contiene la columna
          /// </summary>
          /// <param name="columnName">Nombre de la columna</param>
          /// <returns></returns>
          public bool ContainsColumn(string columnName)
          {
               return _columnNames.Contains(columnName.ToLower());
          }

          /// <summary>
          /// Sorts the specified Campos ordenar.
          /// Multiple Field Sorting by Field Names Using Linq
          /// Los campos se ordenan de la manera siguiente
          /// Ejemplos
          ///
          /// 1. Campo0,Campo1,Campo2,... se tomar pode default un ordeamiento ASC
          /// 2. Campo0~asc,Campos1~desc,... se puede indecar el timpo de ordenamiento para el campo
          ///
          /// Si no se expecifica el ordenamiento se tomar por defaul ASC
          /// Si el campo escrito no existe no se toma en cuenta en el ordenamiento
          /// </summary>
          /// <param name="fieldsToOrder">Cadena con los campos a ordenar.</param>
          public void Sort(string fieldsToOrder)
          {
               Tuple<string, string> tuple;
               if (String.IsNullOrEmpty(fieldsToOrder))
               {
                    return;
               }
               var sortExpresions = new List<Tuple<string, string>>();
               foreach (string field in fieldsToOrder.Split(','))
               {
                    tuple = CreateFieldToOrder(field);
                    if (!Object.Equals(tuple, null))
                    {
                         sortExpresions.Add(tuple);
                    }
               }
               MultipleSort(sortExpresions);
          }

          /// <summary>
          /// AddColumn
          /// </summary>
          /// <param name="columnName">The ps nombre.</param>
          public void AddColumn(string columnName)
          {
               _columnNames.Add(columnName);
               _totalColumns = _columnNames.Count;
          }

          /// <summary>
          /// AddRow
          /// </summary>
          /// <param name="row"></param>
          public void AddRow(Row row)
          {
               _rows.Add(row);
               _totalRows = _rows.Count;
          }

          /// <summary>
          /// CreateFieldToOrder
          /// </summary>
          /// <param name="fieldName">Campos.</param>
          /// <returns></returns>
          private Tuple<string, string> CreateFieldToOrder(string fieldName)
          {
               string realField;
               string order;
               if (!fieldName.Contains('~'))
               {
                    realField = fieldName;
                    order = "asc";
               }
               else
               {
                    realField = fieldName.Split('~')[0];
                    order = fieldName.Split('~')[1];
               }
               if (!ContainsColumn(realField))
               {
                    return null;
               }
               if (!string.Equals(order, "asc", StringComparison.OrdinalIgnoreCase) && !string.Equals(order, "desc", StringComparison.OrdinalIgnoreCase))
               {
                    order = "asc";
               }
               return new Tuple<string, string>(realField, order.ToLower());
          }

          /// <summary>
          /// Multiples the sort.
          /// </summary>
          /// <param name="sortExpressions">Expresion de Ordenamiento.</param>
          private void MultipleSort(List<Tuple<string, string>> sortExpressions)
          {
               // No sorting needed
               if ((sortExpressions == null) || (sortExpressions.Count == 0))
               {
                    //Si no tiene nada que ordenas lo regresamos
                    return;
               }
               //Obtenemos toda la informacion que se va a ordenar
               IEnumerable<Row> query = from item in _rows
                                        select item;
               //Permite almancenar la lista ordenana
               IOrderedEnumerable<Row> sortQuery = null;

               for (int indexTuple = 0; indexTuple < sortExpressions.Count; indexTuple++)
               {
                    //Hacemos un ciclo por indice, ya que es alterado por Linq
                    var index = indexTuple;
                    //Recuperamos el valor del campo por el cual vamos a ordenar.
                    //Recuperamos el objeto por el que se ordena por el nombre del campo
                    //Esto es una expresion lamda
                    //Encapsulamos el metodo para que tiene un parametro Fila y regresamos un nuevo objeto
                    //que usaremos como expreiosn para
                    Func<Row, object> expression = loItem => loItem.RowValue[sortExpressions[index].Item1];
                    //Generic con reflection
                    //Func<AccesoDatos, object> loExpresion = loItem => loItem.GetType().GetProperty(poSortExpressions[loIndex].Item1);
                    //Objeto.Empresa
                    //loExpression ()

                    //Seleccionamos el tipo de ordenamiento
                    if (sortExpressions[index].Item2 == "asc")
                    {
                         sortQuery = (index == 0) ?
                                              //Ordenamos la se cuenta por la expresion
                                              query.OrderBy(expression)
                                              //Realiza la clasificaicon porterior al ordenamiento anterior
                                              : sortQuery.ThenBy(expression);
                    }
                    else
                    {
                         sortQuery = (index == 0) ?
                                              //Ordenamos la se cuenta por la expresion
                                              query.OrderByDescending(expression)
                                              //Realiza la clasificaicon posterior al ordenamiento anterior
                                              : sortQuery.ThenByDescending(expression);
                    }
               }
               //Una vez ordenado la lista por los campos la reasignamos al origen de datos
               _rows = sortQuery.ToHash();
          }

          #endregion Functions

          #region Disposable

          /// <summary>
          /// Clears this instance.
          /// </summary>
          public void Clear()
          {
               _rows.Clear();
               _columnNames.Clear();
               _columnExpresion.Clear();
               _columnTypes.Clear();
          }

          /// <summary>
          /// Implementación de IDisposable. No se sobreescribe.
          /// </summary>
          public void Dispose()
          {
               Dispose(true);
               // GC.SupressFinalize quita de la cola de finalización al objeto.
               GC.SuppressFinalize(this);
          }

          /// <summary>
          /// Limpia los recursos manejados y no manejados.
          /// </summary>
          /// <param name="isDiposing">
          /// Si es true, el método es llamado directamente o indirectamente
          /// desde el código del usuario.
          /// Si es false, el método es llamado por el finalizador
          /// y sólo los recursos no manejados son finalizados.
          /// </param>
          protected virtual void Dispose(bool isDiposing)
          {
               // Preguntamos si Dispose ya fue llamado.
               if (!_isDisposed)
               {
                    if (isDiposing)
                    {
                         //Llamamos al Dispose de todos los RECURSOS MANEJADOS.
                    }
                    // Acá finalizamos correctamente los RECURSOS NO MANEJADOS
                    if (!Object.Equals(_columnNames, null))
                    {
                         _columnNames.Clear();
                         _columnNames = null;
                    }
                    if (!Object.Equals(_columnTypes, null))
                    {
                         _columnTypes.Clear();
                         _columnTypes = null;
                    }
                    if (!Object.Equals(_columnExpresion, null))
                    {
                         _columnExpresion.Clear();
                         _columnExpresion = null;
                    }
                    if (!Object.Equals(_columnNames, null))
                    {
                         _columnNames.Clear();
                         _columnNames = null;
                    }
                    if (!Object.Equals(_rows, null))
                    {
                         _rows.Clear();
                         _rows = null;
                    }
               }
               _isDisposed = true;
          }

          /// <summary>
          /// Destructor de la instancia
          /// </summary>
          ~GenericTable()
          {
               Dispose(false);
          }

          #endregion Disposable
     }
}