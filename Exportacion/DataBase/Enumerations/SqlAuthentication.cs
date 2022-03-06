namespace Framework.Enumerations
{
     /// <summary>
     /// Authentication modes for Microsoft SQL Server
     /// </summary>
     public enum SqlAuthentication
     {
          /// <summary>
          /// Mexed mode (Authentication with sql credentials)
          /// </summary>
          Mixed = 0,

          /// <summary>
          /// Use windows credentials
          /// </summary>
          WindowsAuthentication = 1
     }
}