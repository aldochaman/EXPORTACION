using System;

namespace Framework.Enumerations
{
     /// <summary>
     /// Editions mode fot meta data
     /// </summary>
     [Serializable]
     public enum EditionModes
     {
          /// <summary>
          /// Without edition
          /// </summary>
          AdEditNone = 0,

          /// <summary>
          /// Edition in progress
          /// </summary>
          AdEditInProgress = 1,

          /// <summary>
          /// Add edition
          /// </summary>
          AdEditAdd = 2
     }
}