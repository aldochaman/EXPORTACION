using System.Configuration;

namespace Framework.DataBase.Utilities
{
     /// <summary>
     /// Clase par ala configuracion de la base de datos en el .config
     /// </summary>
     public static class ConnectionString 
     {
          #region constants

          public const string TITLE_CONNECTION_NAME_AUTH = "AuthConnection";
          public const string TITLE_CONNECTION_NAME_BANK = "BankConnection";
          public const string TITLE_CONNECTION_NAME_PANEL = "PanelConnection";
          
          public const string TITLE_CONNECTION_NAME_AUTH_PROVIDER = "AuthConnectionProvider";
          public const string TITLE_CONNECTION_NAME_BANK_PROVIDER = "BankConnectionProvider";
          public const string TITLE_CONNECTION_NAME_PANEL_PROVIDER = "PanelConnectionProvider";

          //private const string NUMBER_OF_ITEMS_PROPERTY_NAME = "NumberOfItemsPerPage";

          #endregion constants

          #region properties

          public static  string DataBaseAuth
          {
               //Las configuraciones son de solo lectura
               get
               {                                            
                    return ConfigurationManager.ConnectionStrings[TITLE_CONNECTION_NAME_AUTH]?.ConnectionString ;                    
               }
          }
          public static string DataBaseBank
          {
               //Las configuraciones son de solo lectura
               get
               {
                    return ConfigurationManager.ConnectionStrings[TITLE_CONNECTION_NAME_BANK]?.ConnectionString;
               }
          }

          public static string DataBasePanel
          {
               //Las configuraciones son de solo lectura
               get
               {
                    return ConfigurationManager.ConnectionStrings[TITLE_CONNECTION_NAME_PANEL]?.ConnectionString;
               }
          }

          public static string DataBaseAuthProvider
          {
               //Las configuraciones son de solo lectura
               get
               {
                    return ConfigurationManager.ConnectionStrings[TITLE_CONNECTION_NAME_AUTH]?.ProviderName;
               }
          }
          public static string DataBaseBankProvider
          {
               //Las configuraciones son de solo lectura
               get
               {
                    return ConfigurationManager.ConnectionStrings[TITLE_CONNECTION_NAME_BANK]?.ProviderName;
               }
          }

          public static string DataBasePanelProvider
          {
               //Las configuraciones son de solo lectura
               get
               {
                    return ConfigurationManager.ConnectionStrings[TITLE_CONNECTION_NAME_PANEL]?.ProviderName;
               }
          }

          #endregion properties
     }




}