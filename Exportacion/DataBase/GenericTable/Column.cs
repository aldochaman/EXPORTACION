using Framework.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Framework.DataBase
{
    /// <summary>
    /// Clase para alcenar las columnas de una tabla de manejra dinamica
    /// </summary>
    public class Column
     {
          /// <summary>
          ///Dictionary that hold all the dynamic property values
          /// </summary>
          private Dictionary<string, object> _ColumnValue = new Dictionary<string, object>();

          /// <summary>
          /// the property call to get any dynamic property in our Dictionary, or "" if none found. rvt [DataMember]
          /// </summary>
          /// <param name="name"></param>
          /// <returns></returns>
          public object this[string name]
          {
               get
               {
                    if (_ColumnValue.ContainsKey(name.ToLower()))
                    {
                         return _ColumnValue[name.ToLower()];
                    }
                    return "";
               }
               set
               {
                    _ColumnValue[name.ToLower()] = value;
               }
          }

          public List<object> Data { get { return _ColumnValue.Values.ToList(); } }

          /// <summary>
          /// Gets the value.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="name">The name.</param>
          /// <returns></returns>
          public T GetValue<T>(string name)
          {
               object poValor = _ColumnValue[name.ToLower()];
               if (Convert.IsDBNull(poValor) || String.IsNullOrEmpty(Convert.ToString(poValor).Trim()))
                    return ObjectsUtilities.GetDefaultValue<T>();
               return (T)Convert.ChangeType(poValor, typeof(T));
          }
     }
}