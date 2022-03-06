using System.Collections.Generic;
using System.Text;

namespace Framework.DataBase.TypedGenericTable
{
     /// <summary>
     /// GenericTable
     /// </summary>
     public class GenericTable
     {
        

          /// <summary>
          /// Gets or sets the table.
          /// </summary>
          /// <value>
          /// The table.
          /// </value>
          public string Table { get; set; }

          /// <summary>
          /// Gets or sets the query.
          /// </summary>
          /// <value>
          /// The query.
          /// </value>
          public StringBuilder Query { get; set; }

          /// <summary>
          /// Gets or sets the owner.
          /// </summary>
          /// <value>
          /// The owner.
          /// </value>
          public string Owner { get; set; }

          /// <summary>
          /// Gets or sets the rows.
          /// </summary>
          /// <value>
          /// The rows.
          /// </value>
          public HashSet<GenericRow> Rows { get; set; }

          /// <summary>
          /// Determina si exiten valores en la coleccion de registros
          /// </summary>
          public bool EOF { get; private set; }

        
     }
}