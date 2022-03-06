namespace Framework.Enumerations
{
     /// <summary>
     /// Allows you identify the type of operation you perform for the generic table filter
     /// </summary>
     public enum Operations
     {
          /// <summary>
          /// Equal to
          /// </summary>
          Equal = 0,

          /// <summary>
          /// Different to
          /// </summary>
          Different = 1,

          /// <summary>
          /// Less than to
          /// </summary>
          LessThan = 2,

          /// <summary>
          /// Greater than to
          /// </summary>
          GreaterThan = 3,

          /// <summary>
          /// Less than or equal to
          /// </summary>
          LessThanOrEqual = 4,

          /// <summary>
          /// Greater than or equal to
          /// </summary>
          GreaterThanOrEqual = 5,

          /// <summary>
          /// It is inside of
          /// </summary>
          InsideOf = 6,

          /// <summary>
          /// Starts with
          /// </summary>
          /// <remarks></remarks>
          StartWith = 7,

          /// <summary>
          /// Ends with
          /// </summary>
          /// <remarks></remarks>
          EndsWith = 8,

          /// <summary>
          /// Contains
          /// </summary>
          /// <remarks></remarks>
          Contains = 9
     };
}