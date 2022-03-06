namespace Framework.DataBase.TypedGenericTable
{
     /// <summary>
     /// ColumnProperty
     /// </summary>
     /// <typeparam name="T"></typeparam>
     /// <seealso cref="Framework.DataBase.TypedGenericTable.Property" />
     public class ColumnProperty<T> : Property
     {
          /// <summary>
          /// Gets or sets the value.
          /// </summary>
          /// <value>
          /// The value.
          /// </value>
          public T Value { get; set; }
     }
}