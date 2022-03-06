namespace Framework.DataBase
{
     /// <summary>
     /// Clase para almacenar la definicion de la columna
     /// </summary>
     public class ColumnDefinition
     {
          /// <summary>
          /// NOmbre de la columna
          /// </summary>
          public string Name { get; set; }

          /// <summary>
          /// Tipo de datos
          /// </summary>
          public string DataType { get; set; }

          /// <summary>
          /// Tipo nativo
          /// </summary>
          public string NativeType { get; set; }

          /// <summary>
          /// Longitud del campo
          /// </summary>
          public int Length { get; set; }

          /// <summary>
          /// Precision del campo
          /// </summary>
          public int Precision { get; set; }

          /// <summary>
          /// Orden del campo
          /// </summary>
          public int FieldId { get; set; }

          /// <summary>
          /// Es llave primaria
          /// </summary>
          public int PrimaryKey { get; set; }

          /// <summary>
          /// Permite camposo nullos
          /// </summary>
          public bool IsRequiredInDataBase { get; set; }

          /// <summary>
          /// Tipo de valor en C#
          /// </summary>
          public string CSharpType { get; set; }

          /// <summary>
          /// Valor por omision
          /// </summary>
          public string DefaultValue { get; set; }

          /// <summary>
          /// Gets or sets the descripcion.
          /// </summary>
          /// <value>
          /// The descripcion.
          /// </value>
          public string Description { get; set; }

          /// <summary>
          /// Gets or sets a value indicating whether [es identity].
          /// </summary>
          /// <value>
          ///   <c>true</c> if [es identity]; otherwise, <c>false</c>.
          /// </value>
          public bool EsIdentity { get; set; }

          /// <summary>
          /// Gets or sets a value indicating whether this <see cref="ColumnDefinition"/> is ordenar.
          /// </summary>
          /// <value>
          ///   <c>true</c> if ordenar; otherwise, <c>false</c>.
          /// </value>
          public bool UserOrder { get; set; }

          /// <summary>
          /// Gets or sets a value indicating whether [es campo empresa].
          /// </summary>
          /// <value>
          ///   <c>true</c> if [es campo empresa]; otherwise, <c>false</c>.
          /// </value>
          public bool CompanyField { get; set; }

          /// <summary>
          /// Gets or sets a value indicating whether this instance is year period field.
          /// </summary>
          /// <value>
          ///   <c>true</c> if this instance is year period field; otherwise, <c>false</c>.
          /// </value>
          public bool YearPeriodField { get; set; }

          /// <summary>
          /// Gets or sets a value indicating whether this instance is virtual.
          /// </summary>
          /// <value>
          ///   <c>true</c> if this instance is virtual; otherwise, <c>false</c>.
          /// </value>
          public bool IsVirtual { get; set; }

          /// <summary>
          /// Gets or sets a value indicating whether this instance is encrypted.
          /// </summary>
          /// <value>
          ///   <c>true</c> if this instance is encrypted; otherwise, <c>false</c>.
          /// </value>
          public bool Encrypted { get; set; }

          /// <summary>
          /// Gets or sets a value indicating whether this instance is password.
          /// </summary>
          /// <value>
          ///   <c>true</c> if this instance is password; otherwise, <c>false</c>.
          /// </value>
          public bool Password { get; set; }

          /// <summary>
          /// Gets or sets a value indicating whether this instance is include hours.
          /// </summary>
          /// <value>
          ///   <c>true</c> if this instance is include hours; otherwise, <c>false</c>.
          /// </value>
          public bool IncludeHours { get; set; }
     }
}