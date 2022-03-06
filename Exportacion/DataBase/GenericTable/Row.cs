using Framework.Extensions;
using System;
using System.Collections.Generic;
namespace Framework.DataBase
{
    /// <summary>
    /// Clase para almancear las columnas dinamicas de la tabla
    /// </summary>
    public class Row
     {
          #region Variables
          /// <summary>
          /// property is a class that will create dynamic properties at runtime
          /// </summary>
          private Column _property = new Column();
          private Dictionary<string, int> _informationadditional;
          #endregion

          #region Properties
          public Dictionary<string, int> Additional
          {
               get
               {
                    return _informationadditional;
               }
               set
               {
                    _informationadditional = value;
               }
          }
          /// <summary>
          /// Propiedade para acceder a las propiedades dinamicas
          /// </summary>
          public Column RowValue
          {
               get
               {
                    return _property;
               }
               set
               {
                    _property = value;
               }
          }

          /// <summary>
          /// Gets or sets the <see cref="System.Object"/> with the specified input value.
          /// </summary>
          /// <value>
          /// The <see cref="System.Object"/>.
          /// </value>
          /// <param name="inputValue">The input value.</param>
          /// <returns></returns>
          public object this[string inputValue]
          {
               get
               {
                    return _property[inputValue.ToLower()];
               }
               set
               {
                    _property[inputValue.ToLower()] = value;
               }
          }


          #endregion

          #region Methods
          /// <summary>
          /// Gets the value.
          /// </summary>
          /// <typeparam name="T"></typeparam>
          /// <param name="name">The name.</param>
          /// <returns></returns>
          public T GetValue<T>(string name)
          {
               object value = RowValue[name.ToLower()];
               if (Convert.IsDBNull(value) || String.IsNullOrEmpty(Convert.ToString(value).Trim()))
                    return ObjectsUtilities.GetDefaultValue<T>();
               return (T)Convert.ChangeType(value, typeof(T));
          }
          #endregion
     }
}