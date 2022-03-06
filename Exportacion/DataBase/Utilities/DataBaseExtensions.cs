using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml.Linq;

namespace Framework.DataBase.Utilities
{
     /// <summary>
     /// Extensions
     /// </summary>
     public static class Extensions
     {
          /// <summary>
          /// Basic data types
          /// </summary>
          private static Type[] _oDataTypes = new[]
          {
               typeof(byte),
               typeof(sbyte),
               typeof(short),
               typeof(ushort),
               typeof(int),
               typeof(uint),
               typeof(long),
               typeof(ulong),
               typeof(float),
               typeof(double),
               typeof(decimal),
               typeof(bool),
               typeof(char),
               typeof(Guid),
               typeof(DateTime),
               typeof(DateTimeOffset),
               typeof(byte[]),
               typeof(string)
          };

          /// <summary>
          /// To the data table.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="poData">The data.</param>
          /// <returns></returns>
          public static DataTable ToDataTable<T>(this IEnumerable<T> poData)
          {
               //Excluir las propiedades publicas que viene de la clase padre
               IEnumerable<PropertyDescriptor> loProperties = from loProperty in TypeDescriptor.GetProperties(typeof(T)).Cast<PropertyDescriptor>()
                                                              where IsBasicType(loProperty.PropertyType) &&
                                                                    loProperty.Name != "NombreTabla" &&
                                                                    loProperty.Name != "EOF" &&
                                                                    loProperty.Name != "AgregaHistorial" &&
                                                                    loProperty.Name != "Propietario"
                                                              select loProperty;
               return GetDataTable(poData, loProperties);
          }

          /// <summary>
          /// To the data table.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="poData">The data.</param>
          /// <param name="poExpression">The expression.</param>
          /// <returns></returns>
          public static DataTable ToDataTable<T>(this IEnumerable<T> poData, Func<PropertyDescriptor, bool> poExpression)
          {
               IEnumerable<PropertyDescriptor> loProperties = TypeDescriptor.GetProperties(typeof(T))
                                                                            .Cast<PropertyDescriptor>()
                                                                            .Where(poExpression);
               return GetDataTable(poData, loProperties);
          }

          /// <summary>
          /// To the listof.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="poDataTable">The dt.</param>
          /// <returns></returns>
          public static IEnumerable<T> ToEnumerable<T>(this DataTable poDataTable) where T : class, new()
          {
               T loInstanceOfT;
               const BindingFlags loFlags = BindingFlags.Public | BindingFlags.Instance;
               IEnumerable<string> loColumnNames;
               IEnumerable<PropertyInfo> loObjectProperties;
               loColumnNames = poDataTable.Columns
                                          .Cast<DataColumn>()
                                          .Select(c => c.ColumnName.ToLower());
               loObjectProperties = typeof(T).GetProperties(loFlags);

               IEnumerable<T> loTargetList = poDataTable.AsEnumerable().Select(loDataRow =>
               {
                    //var instanceOfT = Activator.CreateInstance<T>();
                    loInstanceOfT = new T();
                    foreach (PropertyInfo loProperty in loObjectProperties.Where(loProperties => loColumnNames.Contains(loProperties.Name.ToLower()) && loDataRow[loProperties.Name] != DBNull.Value))
                    {
                         loProperty.SetValue(loInstanceOfT, ChangeType(loDataRow[loProperty.Name], loProperty.PropertyType), null);
                    }
                    return loInstanceOfT;
               });

               return loTargetList;
          }

          /// <summary>
          /// To the hash set.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="poEnumerable">The po enumerable.</param>
          /// <returns></returns>
          public static HashSet<T> ToHash<T>(this IEnumerable<T> poEnumerable)
          {
               HashSet<T> loHashSet = new HashSet<T>();
               foreach (T loItem in poEnumerable)
               {
                    loHashSet.Add(loItem);
               }
               return loHashSet;
          }

          /// <summary>
          /// Changes the type.
          /// </summary>
          /// <param name="poData">The po data.</param>
          /// <param name="poType">Type of the po.</param>
          /// <returns></returns>
          public static object ChangeType(object poData, Type poType)
          {
               if (Object.Equals(poData, System.DBNull.Value))
                    return null;
               return Convert.ChangeType(poData, poType);
          }

          /// <summary>
          /// To the XML.
          /// </summary>
          /// <param name="poData">The dt.</param>
          /// <param name="psRootName">Name of the root.</param>
          /// <returns></returns>
          public static XDocument ToXml(this DataTable poData, string psRootName)
          {
               XDocument loXDocument = new XDocument
               {
                    Declaration = new XDeclaration("1.0", "utf-8", "")
               };
               loXDocument.Add(new XElement(psRootName));
               foreach (DataRow loRow in poData.Rows)
               {
                    XElement loElement = new XElement(poData.TableName);
                    foreach (DataColumn loColumn in poData.Columns)
                    {
                         loElement.Add(new XElement(loColumn.ColumnName, loRow[loColumn].ToString().Trim(' ')));
                    }
                    if (loXDocument.Root != null)
                         loXDocument.Root.Add(loElement);
               }
               return loXDocument;
          }

          /// <summary>
          /// Gets the property.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <returns></returns>
          public static IEnumerable<PropertyDescriptor> GetProperty<T>()
          {
               return from Lo_Property in TypeDescriptor.GetProperties(typeof(T)).Cast<PropertyDescriptor>()
                      where IsBasicType(Lo_Property.PropertyType)
                      select Lo_Property;
          }

          /// <summary>
          /// Gets the data table.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="psPrefijoColumnas">The ps prefijo columnas.</param>
          /// <param name="psCambio">The ps cambio.</param>
          /// <returns></returns>
          public static DataTable GetDataTable<T>(string psPrefijoColumnas, string psCambio)
          {
               DataTable table = new DataTable();
               IEnumerable<PropertyDescriptor> Lo_Properties;
               string name;
               Lo_Properties = GetProperty<T>();
               foreach (PropertyDescriptor Lo_Property in Lo_Properties)
               {
                    if (string.IsNullOrEmpty(psCambio) || string.IsNullOrEmpty(psPrefijoColumnas))
                         name = Lo_Property.Name;
                    else
                         name = Lo_Property.Name.Replace(psCambio, psPrefijoColumnas);
                    if (Lo_Property.PropertyType == null)
                         table.Columns.Add(name, Nullable.GetUnderlyingType(Lo_Property.PropertyType));
                    else
                         table.Columns.Add(name, Lo_Property.PropertyType);
               }
               return table;
          }

          ///// <summary>
          ///// Regresas the fecha SQL.
          ///// </summary>
          ///// <param name="pdFecha">The pd fecha.</param>
          ///// <param name="pbIncluyeHHmmss">if set to <c>true</c> [pb incluye h HMMSS].</param>
          ///// <param name="psCaracterSeparado">The ps caracter separado.</param>
          ///// <returns>System.String.</returns>
          //public static string RegresaFechaSQL(this DateTime pdFecha, bool pbIncluyeHHmmss = false, string psCaracterSeparado = "")
          //{
          //     StringBuilder lsFechaSQL;
          //     lsFechaSQL = new StringBuilder();
          //     lsFechaSQL.AppendFormat("'{0}{3}{1}{3}{2}", pdFecha.Year, (pdFecha.Month).ToString().PadLeft(2, '0'), pdFecha.Day.ToString().PadLeft(2, '0'), psCaracterSeparado);
          //     if (pbIncluyeHHmmss == true)
          //     {
          //          lsFechaSQL.AppendFormat(" {0}:{1}:{2}'", pdFecha.Hour.ToString().PadLeft(2, '0'), pdFecha.Minute.ToString().PadLeft(2, '0'), pdFecha.Second.ToString().PadLeft(2, '0'));
          //     }
          //     else
          //     {
          //          lsFechaSQL.AppendFormat("'");
          //     }
          //     return lsFechaSQL.ToString();
          //}

          #region Private methods

          /// <summary>
          /// Determines whether [is basic type] [the specified type].
          /// </summary>
          /// <param name="poType">The type.</param>
          /// <returns></returns>
          private static bool IsBasicType(Type poType)
          {
               poType = Nullable.GetUnderlyingType(poType) ?? poType;
               return poType.IsEnum || _oDataTypes.Contains(poType);
          }

          /// <summary>
          /// Gets the data table.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="poData">The data.</param>
          /// <param name="poMappedProperties">The mapped properties.</param>
          /// <returns></returns>
          private static DataTable GetDataTable<T>(this IEnumerable<T> poData, IEnumerable<PropertyDescriptor> poMappedProperties)
          {
               DataTable loTable = new DataTable();
               // columns
               foreach (PropertyDescriptor loProperty in poMappedProperties)
               {
                    loTable.Columns.Add(loProperty.Name, Nullable.GetUnderlyingType(loProperty.PropertyType) ?? loProperty.PropertyType);
               }
               // row values
               foreach (T loItem in poData)
               {
                    DataRow loRow = loTable.NewRow();
                    foreach (PropertyDescriptor loProperty in poMappedProperties)
                    {
                         object value = loProperty.GetValue(loItem) ?? DBNull.Value;
                         loRow[loProperty.Name] = value;
                    }
                    loTable.Rows.Add(loRow);
               }
               return loTable;
          }

          /// <summary>
          /// DataSetToTableList
          /// </summary>
          /// <param name="ds"></param>
          /// <returns></returns>
          private static List<List<Dictionary<string, object>>> DataSetToTableList(System.Data.DataSet ds)
          {
               List<List<Dictionary<string, object>>> tables = new List<List<Dictionary<string, object>>>();

               foreach (System.Data.DataTable dt in ds.Tables)
               {
                    List<Dictionary<string, object>> rows = new List<Dictionary<string, object>>();
                    foreach (System.Data.DataRow dr in dt.Rows)
                    {
                         Dictionary<string, object> row = new Dictionary<string, object>();
                         foreach (System.Data.DataColumn dc in dt.Columns)
                         {
                              row.Add(dc.ColumnName, dr[dc.ColumnName]);
                         }
                         rows.Add(row);
                    }
                    tables.Add(rows);
               }
               return tables;
          }

          #endregion Private methods
     }
}