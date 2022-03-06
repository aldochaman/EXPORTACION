using Framework.Extensions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
namespace Framework.DataBase.TypedGenericTable
{
    /// <summary>
    /// GenericRow
    /// </summary>
    /// <seealso cref="System.Dynamic.DynamicObject" />
    /// <seealso cref="System.Dynamic.IDynamicMetaObjectProvider" />
    public class GenericRow : DynamicObject, IDynamicMetaObjectProvider
     {
          /// <summary>
          /// Gets or sets the properties.
          /// </summary>
          /// <value>
          /// The properties.
          /// </value>
          public Dictionary<string, Property> Properties { get; set; }

          #region Dynamic property management

          /// <summary>
          /// Initializes the specified instance.
          /// </summary>
          /// <param name="instance">The instance.</param>
          protected virtual void Initialize(object instance)
          {
          }

          /// <summary>
          /// Try to retrieve a member by name first from instance properties
          /// followed by the collection entries.
          /// </summary>
          /// <param name="binder"></param>
          /// <param name="result"></param>
          /// <returns></returns>
          public override bool TryGetMember(GetMemberBinder binder, out object result)
          {
               result = null;
               // first check the Properties collection for member
               if (!ExistsProperty(binder.Name))
               {
                    return false;
               }
               result = GetPropertyValue(binder.Name);
               return true;
          }

          /// <summary>
          /// Property setter implementation tries to retrieve value from instance
          /// first then into this object
          /// </summary>
          /// <param name="binder"></param>
          /// <param name="value"></param>
          /// <returns></returns>
          public override bool TrySetMember(SetMemberBinder binder, object value)
          {
               if (!ExistsProperty(binder.Name))
               {
                    return false;
               }
               SetPropertyValue(binder.Name, value);
               return true;
          }

          #endregion Dynamic property management

          #region Properties Get Set Basic

          /// <summary>
          /// Gets or sets the <see cref="System.Object"/> with the specified name.
          /// </summary>
          /// <value>
          /// The <see cref="System.Object"/>.
          /// </value>
          /// <param name="name">The name.</param>
          /// <returns></returns>
          public object this[string name]
          {
               get => GetPropertyValue(name);
               set => SetPropertyValue(name, value);
          }

          /// <summary>
          /// Establecers the valor propiedad.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="property">The ps propiedad.</param>
          /// <param name="value">The po valor.</param>
          public void SetValue<T>(string property, T value)
          {
               SetValueToProperty<T>(property, value);
          }

          /// <summary>
          /// Obteners the valor.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="property">The ps propiedad.</param>
          /// <returns></returns>
          public T GetValue<T>(string property)
          {
               return GetPropertyValue<T>(property);
          }

          /// <summary>
          /// Existses the property.
          /// </summary>
          /// <param name="propertyName">Name of the property.</param>
          /// <returns></returns>
          protected bool ExistsProperty(string propertyName)
          {
               return Properties == null ? false : Properties.ContainsKey(propertyName.ToUpper());
          }

          /// <summary>
          /// GetPropertyValue
          /// </summary>
          /// <typeparam name="TValue"></typeparam>
          /// <param name="nameProperty"></param>
          /// <returns></returns>
          protected TValue GetPropertyValue<TValue>(string nameProperty)
          {
               ColumnProperty<TValue> property = GetProperty<TValue>(nameProperty);
               if (property != null)
               {
                    return property.Value;
               }
               return default;
          }

          /// <summary>
          /// Gets the property.
          /// </summary>
          /// <typeparam name="TValue">The type of the value.</typeparam>
          /// <param name="propertyName">Name of the ps.</param>
          /// <returns></returns>
          /// <exception cref="System.InvalidOperationException">
          /// </exception>
          private ColumnProperty<TValue> GetProperty<TValue>(string propertyName)
          {
               ColumnProperty<TValue> propertyColumn;
               if (!ExistsProperty(propertyName))
               {
                    throw new InvalidOperationException(String.Format("Can't get property {0} value, because it doesn't exist.", propertyName));
               }
               propertyColumn = Properties[propertyName.ToUpper()] as ColumnProperty<TValue>;
               if (propertyColumn == null)
               {
                    throw new InvalidOperationException(String.Format("Invalid type {0} specified for {1} property.", typeof(TValue).FullName, propertyName));
               }
               return propertyColumn;
          }

          /// <summary>
          /// Agregars the propiedad.
          /// </summary>
          /// <typeparam name="TValue">The type of the value.</typeparam>
          /// <param name="propertyName">Name of the ps.</param>
          /// <param name="propertyColumn">The po property.</param>
          /// <exception cref="InvalidOperationException"></exception>
          protected void AddProperty<TValue>(string propertyName, ColumnProperty<TValue> propertyColumn)
          {
               if (ExistsProperty(propertyName.ToUpper()))
               {
                    throw new InvalidOperationException(String.Format("Can't add property {0}, because it is already added.", propertyName));
               }
               Properties.Add(propertyName.ToUpper(), propertyColumn);
          }

          /// <summary>
          /// Obteners the valor propiedad.
          /// </summary>
          /// <param name="nameProperty">Name of the ps.</param>
          /// <returns></returns>
          /// <exception cref="System.InvalidOperationException"></exception>
          public object GetPropertyValue(string nameProperty)
          {
               nameProperty = nameProperty.ToUpper();
               if (!ExistsProperty(nameProperty))
               {
                    throw new InvalidOperationException(String.Format("Can't get property {0} value, because it doesn't exist.", nameProperty));
               }
               if (Properties[nameProperty] is ColumnProperty<bool>)
               {
                    return GetPropertyValue<bool>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<byte>)
               {
                    return GetPropertyValue<byte>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<Int16>)
               {
                    return GetPropertyValue<Int16>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<Int32>)
               {
                    return GetPropertyValue<Int32>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<Int64>)
               {
                    return GetPropertyValue<Int64>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<DateTime>)
               {
                    return GetPropertyValue<DateTime>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<decimal>)
               {
                    return GetPropertyValue<decimal>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<Single>)
               {
                    return GetPropertyValue<Single>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<double>)
               {
                    return GetPropertyValue<double>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<object>)
               {
                    return GetPropertyValue<object>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<byte[]>)
               {
                    return GetPropertyValue<byte[]>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<string>)
               {
                    return GetPropertyValue<string>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<byte?>)
               {
                    return GetPropertyValue<byte?>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<bool?>)
               {
                    return GetPropertyValue<bool?>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<Int16?>)
               {
                    return GetPropertyValue<Int16?>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<Int32?>)
               {
                    return GetPropertyValue<Int32?>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<Int64?>)
               {
                    return GetPropertyValue<Int64?>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<DateTime?>)
               {
                    return GetPropertyValue<DateTime?>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<decimal?>)
               {
                    return GetPropertyValue<decimal?>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<Single?>)
               {
                    return GetPropertyValue<Single?>(nameProperty);
               }
               if (Properties[nameProperty] is ColumnProperty<double?>)
               {
                    return GetPropertyValue<double?>(nameProperty);
               }
               return null;
          }

          /// <summary>
          /// Establecers the valor propiedad.
          /// </summary>
          /// <param name="propertyName">Name of the ps.</param>
          /// <param name="propertyValue">The po value.</param>
          /// <exception cref="InvalidOperationException"></exception>
          public void SetPropertyValue(string propertyName, object propertyValue)
          {
               propertyName = propertyName.ToUpper();
               if (!ExistsProperty(propertyName))
               {
                    throw new InvalidOperationException(String.Format("Can't get property {0} value, because it doesn't exist.", propertyName));
               }
               if (Properties[propertyName] is ColumnProperty<bool>)
               {
                    SetValueToProperty<bool>(propertyName, Convert.ToString(propertyValue).ToBoolean());
                    return;
               }
               if (Properties[propertyName] is ColumnProperty<byte>)
               {
                    SetValueToProperty<byte>(propertyName, Convert.ToByte(propertyValue));
                    return;
               }
               if (Properties[propertyName] is ColumnProperty<Int16>)
               {
                    SetValueToProperty<Int16>(propertyName, Convert.ToInt16(propertyValue));
                    return;
               }
               if (Properties[propertyName] is ColumnProperty<Int32>)
               {
                    SetValueToProperty<Int32>(propertyName, Convert.ToInt32(propertyValue));
                    return;
               }
               if (Properties[propertyName] is ColumnProperty<Int64>)
               {
                    SetValueToProperty<Int64>(propertyName, Convert.ToInt64(propertyValue));
                    return;
               }
               if (Properties[propertyName] is ColumnProperty<DateTime>)
               {
                    SetValueToProperty<DateTime>(propertyName, Convert.ToDateTime(propertyValue));
                    return;
               }
               if (Properties[propertyName] is ColumnProperty<decimal>)
               {
                    SetValueToProperty<decimal>(propertyName, Convert.ToDecimal(propertyValue));
                    return;
               }
               if (Properties[propertyName] is ColumnProperty<Single>)
               {
                    SetValueToProperty<Single>(propertyName, Convert.ToSingle(propertyValue));
                    return;
               }
               if (Properties[propertyName] is ColumnProperty<double>)
               {
                    SetValueToProperty<double>(propertyName, Convert.ToDouble(propertyValue));
                    return;
               }
               if (Properties[propertyName] is ColumnProperty<object>)
               {
                    SetValueToProperty<object>(propertyName, propertyValue);
                    return;
               }
               if (Properties[propertyName] is ColumnProperty<byte[]>)
               {
                    if (!String.IsNullOrEmpty(Convert.ToString(propertyValue)))
                    {
                         SetValueToProperty<byte[]>(propertyName, (byte[])propertyValue);
                         return;
                    }
                    else
                    {
                         SetValueToProperty<byte[]>(propertyName, null);
                         return;
                    }
               }
               if (Properties[propertyName] is ColumnProperty<string>)
               {
                    if (!Object.Equals(propertyValue, null))
                    {
                         SetValueToProperty<string>(propertyName, Convert.ToString(propertyValue));
                         return;
                    }
                    else
                    {
                         SetValueToProperty<string>(propertyName, null);
                         return;
                    }
               }
               if (Properties[propertyName] is ColumnProperty<byte?>)
               {
                    if (!Object.Equals(propertyValue, null) && !(Convert.ToString(propertyValue) == ""))
                    {
                         SetValueToProperty<byte?>(propertyName, Convert.ToByte(propertyValue));
                         return;
                    }
                    else
                    {
                         SetValueToProperty<byte?>(propertyName, null);
                         return;
                    }
               }
               if (Properties[propertyName] is ColumnProperty<bool?>)
               {
                    if (!Object.Equals(propertyValue, null) && !(Convert.ToString(propertyValue) == ""))
                    {
                         SetValueToProperty<bool?>(propertyName, Convert.ToString(propertyValue).ToBoolean());
                         return;
                    }
                    else
                    {
                         SetValueToProperty<bool?>(propertyName, null);
                         return;
                    }
               }
               if (Properties[propertyName] is ColumnProperty<Int16?>)
               {
                    if (!Object.Equals(propertyValue, null) && !(Convert.ToString(propertyValue) == ""))
                    {
                         SetValueToProperty<Int16?>(propertyName, Convert.ToInt16(propertyValue));
                         return;
                    }
                    else
                    {
                         SetValueToProperty<Int16?>(propertyName, null);
                         return;
                    }
               }
               if (Properties[propertyName] is ColumnProperty<Int32?>)
               {
                    if (!Object.Equals(propertyValue, null) && !(Convert.ToString(propertyValue) == ""))
                    {
                         SetValueToProperty<Int32?>(propertyName, Convert.ToInt32(propertyValue));
                         return;
                    }
                    else
                    {
                         SetValueToProperty<Int32?>(propertyName, null);
                         return;
                    }
               }
               if (Properties[propertyName] is ColumnProperty<Int64?>)
               {
                    if (!Object.Equals(propertyValue, null) && !(Convert.ToString(propertyValue) == ""))
                    {
                         SetValueToProperty<Int64?>(propertyName, Convert.ToInt64(propertyValue));
                         return;
                    }
                    else
                    {
                         SetValueToProperty<Int64?>(propertyName, null);
                         return;
                    }
               }
               if (Properties[propertyName] is ColumnProperty<DateTime?>)
               {
                    if (!Object.Equals(propertyValue, null) && !(Convert.ToString(propertyValue) == ""))
                    {
                         SetValueToProperty<DateTime?>(propertyName, Convert.ToDateTime(propertyValue));
                         return;
                    }
                    else
                    {
                         SetValueToProperty<DateTime?>(propertyName, null);
                         return;
                    }
               }
               if (Properties[propertyName] is ColumnProperty<decimal?>)
               {
                    if (!Object.Equals(propertyValue, null) && !(Convert.ToString(propertyValue) == ""))
                    {
                         SetValueToProperty<decimal?>(propertyName, Convert.ToDecimal(propertyValue));
                    }
                    else
                    {
                         SetValueToProperty<decimal?>(propertyName, null);
                    }
               }
               if (Properties[propertyName] is ColumnProperty<Single?>)
               {
                    if (!Object.Equals(propertyValue, null) && !(Convert.ToString(propertyValue) == ""))
                    {
                         SetValueToProperty<Single?>(propertyName, Convert.ToSingle(propertyValue));
                         return;
                    }
                    else
                    {
                         SetValueToProperty<Single?>(propertyName, null);
                         return;
                    }
               }
               if (Properties[propertyName] is ColumnProperty<double?>)
               {
                    if (!Object.Equals(propertyValue, null) && Convert.ToString(propertyValue) != "")
                    {
                         SetValueToProperty<double?>(propertyName, Convert.ToDouble(propertyValue));
                         return;
                    }
                    else
                    {
                         SetValueToProperty<double?>(propertyName, null);
                         return;
                    }
               }
          }

          /// <summary>
          /// Sets the value to property.
          /// </summary>
          /// <typeparam name="TValue">The type of the value.</typeparam>
          /// <param name="propertyName">Name of the property.</param>
          /// <param name="propertyValue">The property value.</param>
          protected void SetValueToProperty<TValue>(string propertyName, TValue propertyValue)
          {
               SetValueToProperty<TValue>(propertyName, propertyValue, false);
          }

          /// <summary>
          /// Sets the value.
          /// </summary>
          /// <typeparam name="TValue">The type of the value.</typeparam>
          /// <param name="propertyName">Name of the ps.</param>
          /// <param name="propertyValue">The po value.</param>
          /// <param name="isLastValue">if set to <c>true</c> [pb valor anterior].</param>
          protected void SetValueToProperty<TValue>(string propertyName, TValue propertyValue, bool isLastValue)
          {
               ColumnProperty<TValue> propertyColumn = GetProperty<TValue>(propertyName);
               if (propertyColumn == null)
               {
                    return;
               }
               propertyColumn.Value = propertyValue;
          }

          /// <summary>
          /// SetValueToProperty
          /// </summary>
          /// <param name="propertyName"></param>
          protected void SetValueToProperty<TValue>(string propertyName)
          {
               ColumnProperty<TValue> propertyColumn = GetProperty<TValue>(propertyName);
               if (propertyColumn == null)
               {
                    return;
               }
               propertyColumn.Value = propertyColumn.Value;
          }

          #endregion Properties Get Set Basic

          #region LoadData

          /// <summary>
          /// Loads the row data.
          /// </summary>
          /// <param name="dataReader">The data reader.</param>
          protected internal void LoadRowData(IDataReader dataReader)
          {
               IEnumerable<DataColumn> columnNames;
               columnNames = dataReader.GetSchemaTable().Columns.Cast<DataColumn>();
               foreach (DataColumn column in columnNames)
               {
                    //AddProperty(column.ColumnName, dataReader[column.ColumnName]);
               }

               foreach (DataColumn column in columnNames)
               {
                    if (column.DataType == typeof(bool))
                    {
                         AddProperty<bool>(column.ColumnName, new ColumnProperty<bool>() { Value = dataReader.GetValue<Boolean>(column.ColumnName) });
                    }
                    if (column.DataType == typeof(byte))
                    {
                         AddProperty<byte>(column.ColumnName, new ColumnProperty<byte>() { Value = dataReader.GetValue<byte>(column.ColumnName) });
                    }
                    if (column.GetType() == typeof(Int16))
                    {
                         AddProperty<Int16>(column.ColumnName, new ColumnProperty<Int16>() { Value = dataReader.GetValue<Int16>(column.ColumnName) });
                    }
                    if (column.GetType() == typeof(Int32))
                    {
                         AddProperty<Int32>(column.ColumnName, new ColumnProperty<Int32>() { Value = dataReader.GetValue<Int32>(column.ColumnName) });
                    }
                    if (column.GetType() == typeof(Int64))
                    {
                         AddProperty<Int64>(column.ColumnName, new ColumnProperty<Int64>() { Value = dataReader.GetValue<Int64>(column.ColumnName) });
                    }
                    if (column.GetType() == typeof(DateTime))
                    {
                         AddProperty<DateTime>(column.ColumnName, new ColumnProperty<DateTime>() { Value = dataReader.GetValue<DateTime>(column.ColumnName) });
                    }
                    if (column.GetType() == typeof(decimal))
                    {
                         AddProperty<decimal>(column.ColumnName, new ColumnProperty<decimal>() { Value = dataReader.GetValue<decimal>(column.ColumnName) });
                    }
                    if (column.GetType() == typeof(Single))
                    {
                         AddProperty<Single>(column.ColumnName, new ColumnProperty<Single>() { Value = dataReader.GetValue<Single>(column.ColumnName) });
                    }
                    if (column.GetType() == typeof(double))
                    {
                         AddProperty<double>(column.ColumnName, new ColumnProperty<double>() { Value = dataReader.GetValue<double>(column.ColumnName) });
                    }
                    if (column.GetType() == typeof(object))
                    {
                         AddProperty<object>(column.ColumnName, new ColumnProperty<object>() { Value = dataReader.GetValue<object>(column.ColumnName) });
                    }
                    if (column.GetType() == typeof(byte[]))
                    {
                         AddProperty<byte[]>(column.ColumnName, new ColumnProperty<byte[]>() { Value = dataReader.GetValue<byte[]>(column.ColumnName) });
                    }
                    if (column.GetType() == typeof(string))
                    {
                         AddProperty<string>(column.ColumnName, new ColumnProperty<string>() { Value = dataReader.GetValue<string>(column.ColumnName) });
                    }
                    if (column.GetType() == typeof(byte?))
                    {
                         if (Convert.IsDBNull(dataReader[column.ColumnName]) || dataReader[column.ColumnName] == null)
                         {
                              AddProperty<byte?>(column.ColumnName, new ColumnProperty<byte?>() { Value = null });
                         }
                         else
                         {
                              AddProperty<byte?>(column.ColumnName, new ColumnProperty<byte?>() { Value = dataReader.GetValue<byte>(column.ColumnName) });
                         }
                    }
                    if (column.GetType() == typeof(bool?))
                    {
                         if (Convert.IsDBNull(dataReader[column.ColumnName]) || dataReader[column.ColumnName] == null)
                         {
                              AddProperty<bool?>(column.ColumnName, new ColumnProperty<bool?>() { Value = null });
                         }
                         else
                         {
                              AddProperty<bool?>(column.ColumnName, new ColumnProperty<bool?>() { Value = dataReader.GetValue<Boolean>(column.ColumnName) });
                         }
                    }
                    if (column.GetType() == typeof(Int16?))
                    {
                         if (Convert.IsDBNull(dataReader[column.ColumnName]) || dataReader[column.ColumnName] == null)
                         {
                              AddProperty<Int16?>(column.ColumnName, new ColumnProperty<Int16?>() { Value = null });
                         }
                         else
                         {
                              AddProperty<Int16?>(column.ColumnName, new ColumnProperty<Int16?>() { Value = dataReader.GetValue<Int16>(column.ColumnName) });
                         }
                    }
                    if (column.GetType() == typeof(Int32?))
                    {
                         if (Convert.IsDBNull(dataReader[column.ColumnName]) || dataReader[column.ColumnName] == null)
                         {
                              AddProperty<Int32?>(column.ColumnName, new ColumnProperty<Int32?>() { Value = null });
                         }
                         else
                         {
                              AddProperty<Int32?>(column.ColumnName, new ColumnProperty<Int32?>() { Value = dataReader.GetValue<Int32>(column.ColumnName) });
                         }
                    }
                    if (column.GetType() == typeof(byte))
                    {
                         if (Convert.IsDBNull(dataReader[column.ColumnName]) || dataReader[column.ColumnName] == null)
                         {
                              AddProperty<Int64?>(column.ColumnName, new ColumnProperty<Int64?>() { Value = null });
                         }
                         else
                         {
                              AddProperty<Int64?>(column.ColumnName, new ColumnProperty<Int64?>() { Value = dataReader.GetValue<Int64>(column.ColumnName) });
                         }
                    }
                    if (column.GetType() == typeof(DateTime?))
                    {
                         if (Convert.IsDBNull(dataReader[column.ColumnName]) || dataReader[column.ColumnName] == null)
                         {
                              AddProperty<DateTime?>(column.ColumnName, new ColumnProperty<DateTime?>() { Value = null });
                         }
                         else
                         {
                              AddProperty<DateTime?>(column.ColumnName, new ColumnProperty<DateTime?>() { Value = dataReader.GetValue<DateTime>(column.ColumnName) });
                         }
                    }
                    if (column.GetType() == typeof(decimal?))
                    {
                         if (Convert.IsDBNull(dataReader[column.ColumnName]) || dataReader[column.ColumnName] == null)
                         {
                              AddProperty<decimal?>(column.ColumnName, new ColumnProperty<decimal?>() { Value = null });
                         }
                         else
                         {
                              AddProperty<decimal?>(column.ColumnName, new ColumnProperty<decimal?>() { Value = dataReader.GetValue<decimal>(column.ColumnName) });
                         }
                    }
                    if (column.GetType() == typeof(Single?))
                    {
                         if (Convert.IsDBNull(dataReader[column.ColumnName]) || dataReader[column.ColumnName] == null)
                         {
                              AddProperty<Single?>(column.ColumnName, new ColumnProperty<Single?>() { Value = null });
                         }
                         else
                         {
                              AddProperty<Single?>(column.ColumnName, new ColumnProperty<Single?>() { Value = dataReader.GetValue<Single>(column.ColumnName) });
                         }
                    }
                    if (column.GetType() == typeof(double?))
                    {
                         if (Convert.IsDBNull(dataReader[column.ColumnName]) || dataReader[column.ColumnName] == null)
                         {
                              AddProperty<double?>(column.ColumnName, new ColumnProperty<double?>() { Value = null });
                         }
                         else
                         {
                              AddProperty<double?>(column.ColumnName, new ColumnProperty<double?>() { Value = dataReader.GetValue<double>(column.ColumnName) });
                         }
                    }
               }
          }

          #endregion LoadData
     }
}