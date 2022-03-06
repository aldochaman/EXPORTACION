using System;

namespace Framework.DataBase.Utilities
{
     /// <summary>
     /// Clase para los parametros de una consulta en SQL
     /// </summary>
     [Serializable]
     public class ParameterSql
     {
          /// <summary>
          /// NOmbre del parametro
          /// </summary>
          public string Parameter { get; set; }

          /// <summary>
          /// Valor del parametro
          /// </summary>
          public object Value { get; set; }

          /// <summary>
          /// Crea una nueva instancia para los parametros
          /// </summary>
          /// <param name="parameter">NOmbre del parametro</param>
          /// <param name="value">Valor del parametro</param>
          public ParameterSql(string parameter, object value)
          {
               Parameter = parameter;
               Value = value;
          }
     }
}