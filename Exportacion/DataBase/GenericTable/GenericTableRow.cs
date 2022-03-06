using Framework.DataBase.Utilities;
using Framework.Enumerations;
using Framework.Extensions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Framework.DataBase
{
    /// <summary>
    /// Clase para manejar operaciones mas complejas sobre una TablaGenerica
    /// </summary>
    [Serializable]
     public class GenericTableRow : GenericTable
     {
          #region Variables

          /// <summary>
          /// Lista original de los registros
          /// </summary>
          protected HashSet<Row> _backupOriginalRows;

          #endregion Variables

          #region Properties

          /// <summary>
          /// Campos de busqueda
          /// </summary>
          public FieldSearch[] FindFields { get; set; }

          /// <summary>
          /// Modo de edicion de los registros
          /// </summary>
          public EditionModes EditionMode { get; private set; }

          #endregion Properties

          #region Builders

          /// <summary>
          /// Initializes a new instance of the <see cref="GenericTableRow"/> class.
          /// </summary>
          public GenericTableRow() : base()
          {
          }

          /// <summary>
          /// Initializes a new instance of the <see cref="GenericTableRow"/> class.
          /// </summary>
          /// <param name="tableName"></param>
          public GenericTableRow(string tableName) : base(tableName)
          {
          }

          /// <summary>
          /// Initializes a new instance of the <see cref="GenericTableRow"/> class.
          /// </summary>
          /// <param name="dataTable">The po tabla.</param>
          /// <param name="tableName">The ps nombre tabla.</param>
          /// <param name="query">The ps query.</param>
          public GenericTableRow(DataTable dataTable, string tableName, string query) : base(dataTable, tableName, query)
          {
          }

          #endregion Builders

          #region Metodos

        

          /// <summary>
          /// Filters this instance.
          /// </summary>
          public void Filter()
          {
               Filter("", "", false, Operations.Equal);
          }

          /// <summary>
          /// Filters the specified field.
          /// </summary>
          /// <param name="field">The field.</param>
          public void Filter(string field)
          {
               Filter(field, "", false, Operations.Equal);
          }

          /// <summary>
          /// Filters the specified field.
          /// </summary>
          /// <param name="field">The field.</param>
          /// <param name="value">The value.</param>
          public void Filter(string field, string value)
          {
               Filter(field, value, false, Operations.Equal);
          }

          /// <summary>
          /// Filters the specified field.
          /// </summary>
          /// <param name="field">The field.</param>
          /// <param name="value">The value.</param>
          /// <param name="isNested">if set to <c>true</c> [is nested].</param>
          public void Filter(string field, string value, bool isNested)
          {
               Filter(field, value, isNested, Operations.Equal);
          }

          /// <summary>
          /// Filter
          /// </summary>
          /// <param name="field"></param>
          /// <param name="value"></param>
          /// <param name="isNested"></param>
          /// <param name="operators"></param>
          public void Filter(string field, string value, bool isNested, Operations operators)
          {
               if (!String.IsNullOrEmpty(field) && !String.IsNullOrEmpty(value))
               {
                    if (Object.Equals(_backupOriginalRows, null))
                    {
                         //Si _oRegistroFiltro es nulo nunca se ha ejecutado un filtro.
                         //Se hace un respaldo de los datos originales
                         _backupOriginalRows = new HashSet<Row>(_rows);
                    }
                    else if (!isNested)
                    {
                         //Solo si se quiere no se ejecuta un filtro aninado
                         _rows = new HashSet<Row>(_backupOriginalRows);
                    }

                    //Se hace filtro sobre el campo y el valor especifico
                    switch (operators)
                    {
                         case Operations.Equal:
                              var filter = from row in _rows
                                           where string.Equals(row.RowValue.GetValue<string>(field), value, StringComparison.OrdinalIgnoreCase)
                                           select row;
                              _rows = filter.ToHash();
                              break;

                         case Operations.Different:
                              var filterD = from row in _rows
                                            where !string.Equals(row.RowValue.GetValue<string>(field), value, StringComparison.OrdinalIgnoreCase)
                                            select row;
                              _rows = filterD.ToHash();
                              break;

                         case Operations.LessThan:
                              var filterMe = from row in _rows
                                             where row.RowValue.GetValue<double>(field) < Convert.ToDouble(value)
                                             select row;
                              _rows = filterMe.ToHash();
                              break;

                         case Operations.GreaterThan:
                              var filterMa = from row in _rows
                                             where row.RowValue.GetValue<double>(field) > Convert.ToDouble(value)
                                             select row;
                              _rows = filterMa.ToHash();
                              break;

                         case Operations.LessThanOrEqual:
                              var filterMeI = from row in _rows
                                              where row.RowValue.GetValue<double>(field) <= Convert.ToDouble(value)
                                              select row;
                              _rows = filterMeI.ToHash();
                              break;

                         case Operations.GreaterThanOrEqual:
                              var filterMaI = from row in _rows
                                              where row.RowValue.GetValue<double>(field) >= Convert.ToDouble(value)
                                              select row;
                              _rows = filterMaI.ToHash();
                              break;

                         case Operations.InsideOf:
                              var filterIn = from row in _rows.Where(m => value.Contains(m.RowValue.GetValue<string>(field)))
                                             select row;
                              _rows = filterIn.ToHash();
                              break;

                         default:
                              var filterDe = from row in _rows
                                             where string.Equals(row.RowValue.GetValue<string>(field), value, StringComparison.OrdinalIgnoreCase)
                                             select row;
                              _rows = filterDe.ToHash();
                              break;
                    }
                    _totalRows = _rows.Count;
                    _indexRow = 0;
                    return;
               }
               if (!Object.Equals(_backupOriginalRows, null))
               {
                    _rows = new HashSet<Row>(_backupOriginalRows);
                    _totalRows = _rows.Count;
                    _backupOriginalRows.Clear();
                    _backupOriginalRows = null;
               }
          }

          /// <summary>
          /// Sort
          /// </summary>
          /// <param name="field"></param>
          /// <param name="psAscDes"></param>
          public void Sort(string field, string psAscDes = "ASC")
          {
               if (String.IsNullOrEmpty(field) || String.IsNullOrEmpty(psAscDes.Trim()))
               {
                    return;
               }
               switch (psAscDes.Trim())
               {
                    case "DESC":
                         var sortDes = from row in _rows
                                       orderby row.RowValue.GetValue<string>(field) descending
                                       select row;
                         _rows = sortDes.ToHash();
                         break;

                    case "ASC":
                         var sortAsc = from row in _rows
                                       orderby row.RowValue.GetValue<string>(field) ascending
                                       select row;
                         _rows = sortAsc.ToHash();
                         break;

                    default:
                         var sort = from row in _rows
                                    orderby row.RowValue.GetValue<string>(field) ascending
                                    select row;
                         _rows = sort.ToHash();
                         break;
               }
          }

          /// <summary>
          /// MoveFirst
          /// </summary>
          public void MoveFirst()
          {
               _indexRow = 0;
          }

          /// <summary>
          /// MovePrevious
          /// </summary>
          public void MovePrevious()
          {
               _indexRow -= 1;
          }

          /// <summary>
          /// MoveNext
          /// </summary>
          public void MoveNext()
          {
               _indexRow += 1;
          }

          /// <summary>
          /// MoveLast
          /// </summary>
          public void MoveLast()
          {
               if (Object.Equals(_backupOriginalRows, null))
                    _indexRow = _backupOriginalRows.Count - 1;
               else
                    _indexRow = _rows.Count - 1;
          }
          
          /// <summary>
          /// Close
          /// </summary>
          public void Close()
          {
               if (!Object.Equals(_rows, null))
               {
                    _rows = null;
                    _columnNames = null;
               }
               if (!Object.Equals(_backupOriginalRows, null))
               {
                    _backupOriginalRows = null;
               }
               FindFields = null;
          }

          #endregion Metodos

          /// <summary>
          /// Contains
          /// </summary>
          /// <param name="fieldName"></param>
          /// <returns></returns>

          public bool Contains(string fieldName)
          {
               if (ColumnNames.Contains(fieldName))
               {
                    return true;
               }
               return false;
          }

          /// <summary>
          /// indexer
          /// </summary>
          /// <param name="fieldName"></param>
          /// <returns></returns>
          public object this[string fieldName]
          {
               get
               {
                    if (Object.Equals(_backupOriginalRows, null))
                    {
                         var find = (from m in _rows
                                     where m.RowValue[fieldName].ToString() == fieldName
                                     select m).ToList();

                         return this[_indexRow, fieldName]; //Cdv_Dat.Table.Rows[Ci_Fila][Campo];
                    }
                    else
                    {
                         return this[_indexRow, fieldName];
                    }
               }
               set
               {
                    //Puede actualizar ya los valores
                    //Cdv_Dat.Table.Rows[Ci_Fila][Campo] = value;
                    this[_indexRow, fieldName] = value;
                    if (EditionMode == EditionModes.AdEditNone)
                    {
                         EditionMode = EditionModes.AdEditInProgress;
                    }
               }
          }

          /// <summary>
          /// Find
          /// </summary>
          public void Find()
          {
              
          }

          /// <summary>
          /// Find
          /// </summary>
          /// <param name="charValue"></param>
          /// <returns></returns>
          public int Find(char charValue)
          {
               return Convert.ToInt32(_rows.Where(p => p.RowValue.ToString() == charValue.ToString()));
          }

         
     }
}