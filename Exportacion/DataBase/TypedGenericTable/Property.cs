using System;

namespace Framework.DataBase.TypedGenericTable
{
     /// <summary>
     /// Property
     /// </summary>
     public class Property
     {
          /// <summary>
          /// Gets or sets the type of the data.
          /// </summary>
          /// <value>
          /// The type of the data.
          /// </value>
          public Type DataType { get; set; }

          /// <summary>
          /// Gets or sets the name.
          /// </summary>
          /// <value>
          /// The name.
          /// </value>
          public String Name { get; set; }
     }
}