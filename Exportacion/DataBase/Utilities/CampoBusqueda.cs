namespace Framework.DataBase.Utilities
{
     /// <summary>
     /// Clase para determina los campos que se buscaran
     /// </summary>
     public struct FieldSearch
     {
          /// <summary>
          /// Nombre del campo
          /// </summary>
          public string Field { get; set; }

          /// <summary>
          /// Valor del campos
          /// </summary>
          public object Value { get; set; }
     }
}