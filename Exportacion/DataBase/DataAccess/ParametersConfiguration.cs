using System;

namespace Framework.DataBase
{
     /// <summary>
     /// ParametersConfiguration
     /// </summary>
     [Serializable]
     public class ParametersConfiguration
     {
          #region Generales

          /// <summary>
          /// Para saber si es gobierno web
          /// </summary>
          public bool IsGovernment { get; set; }

          /// <summary>
          /// Gets or sets the string connection firebase.
          /// </summary>
          /// <value>
          /// The string connection firebase.
          /// </value>
          public string StringConnectionFirebase { get; set; }

          public bool Azure { get; set; }

          #endregion Generales

          
     }
}