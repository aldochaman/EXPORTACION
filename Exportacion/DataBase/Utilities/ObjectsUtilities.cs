using Framework.DataBase;
using Framework.Enumerations;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace Framework.Extensions
{
    /// <summary>
    /// Clase para el manejo de objetos
    /// </summary>
    public static class ObjectsUtilities
    {
        /// <summary>
        /// To the null.
        /// </summary>
        /// <param name="o">The o.</param>
        /// <returns></returns>
        public static object ToNull(this object o)
        {
            return null;
        }
        public static bool ToBoolean(this string inputString)
        {
            if (String.IsNullOrEmpty(inputString))
                inputString = "";
            switch (inputString.ToLower())
            {
                case "true":
                case "t":
                case "1":
                case "si":
                case "yes":
                case "y":
                case "s":
                    return true;

                case "0":
                case "false":
                case "f":
                case "":
                case "no":
                case "n":
                    return false;

                default:
                    return false;
            }
        }
        /// <summary>
        /// AllMessages
        /// </summary>
        /// <param name="ex"></param>
        /// <returns></returns>
        public static string AllMessages(this Exception ex)
        {
            if (ex == null) return string.Empty;
            StringBuilder lsMsgs;
            lsMsgs = new StringBuilder();
            lsMsgs.AppendFormat("{0}", ex.Message);
            if (ex.InnerException != null)
                lsMsgs.AppendFormat("{0} InnerException: {1}", Environment.NewLine, AllMessages(ex.InnerException));
            return lsMsgs.ToString();
        }

        /// <summary>
        /// Checks an object to see if it is null or empty.
        /// <para>Empty is any collection, array or dictionary with an item count of 0 or a string that is empty.</para>
        /// </summary>
        public static bool NotEmpty(this object obj)
        {
            if (obj != null && obj is ICollection)
            {
                return ((ICollection)obj).Count > 0;
            }
            if (obj != null && obj is IDictionary)
            {
                return ((IDictionary)obj).Keys.Count > 0;
            }
            if (obj != null && obj is Array)
            {
                return ((Array)obj).Length > 0;
            }
            return !(obj == null || string.IsNullOrEmpty(obj.ToString()));
        }

        /// <summary>
        /// Libera de moemria la tabla generica
        /// </summary>
        /// <param name="dataTable"></param>
        public static void LiberaTablaGenerica(GenericTable dataTable)
        {
            if (!Object.Equals(dataTable, null))
            {
                dataTable.Clear();
                dataTable.Dispose();
                dataTable = null;
            }
        }

        /// <summary>
        /// Simula IIF de VB.net
        /// </summary>
        /// <param name="poCondicion"></param>
        /// <param name="poIzquierda"></param>
        /// <param name="poDerecha"></param>
        /// <returns></returns>
        public static object Iif(bool poCondicion, object poIzquierda, object poDerecha)
        {
            return poCondicion ? poIzquierda : poDerecha;
        }

        /// <summary>
        /// Simula un IIf de Vb.net con generics
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="poCondicion"></param>
        /// <param name="poIzquierda"></param>
        /// <param name="poDerecha"></param>
        /// <returns></returns>
        public static T Iif<T>(bool poCondicion, T poIzquierda, T poDerecha)
        {
            return poCondicion ? poIzquierda : poDerecha;
        }

        /// <summary>
        /// The equivalent of Microsoft.VisualBasic.Command() in C# is Environment.CommandLine.
        /// However, this property includes also the full path of the executable, that isn't returned by Microsoft.VisualBasic.Command.
        /// If you just want to obtain in C# the same value that you have with Microsoft.VisualBasic.Command method
        /// </summary>
        /// <param name="psNombreParametro"></param>
        /// <returns></returns>
        public static string RegresaParametroCommandLine(string psNombreParametro)
        {
            int liPosicionInicio = 0;
            int liPosicionFinal = 0;
            bool lbPosicionInicioEncontrada = false;
            try
            {
                psNombreParametro = psNombreParametro.ToLower();
                liPosicionInicio = Environment.GetCommandLineArgs().ToString().ToLower().IndexOf(psNombreParametro);
                if (liPosicionInicio == -1)
                {
                    return "";
                }
                for (liPosicionFinal = liPosicionInicio + 1; liPosicionFinal <= Environment.GetCommandLineArgs().Length + 1; liPosicionFinal++)
                {
                    if (!lbPosicionInicioEncontrada)
                    {
                        if (Environment.GetCommandLineArgs().ToString().ToLower().Substring(liPosicionFinal, 1) == "=")
                        {
                            liPosicionInicio = liPosicionFinal + 1;
                            lbPosicionInicioEncontrada = true;
                        }
                        else if (Environment.GetCommandLineArgs().ToString().ToLower().Substring(liPosicionFinal, 1) == " ")
                        {
                            return Environment.GetCommandLineArgs().ToString().Substring(liPosicionInicio, liPosicionFinal - liPosicionInicio);
                        }
                    }
                }
                if (!lbPosicionInicioEncontrada)
                {
                    return "";
                }
                return Environment.GetCommandLineArgs().ToString().Substring(liPosicionInicio, liPosicionFinal - liPosicionInicio);
            }
            catch
            {
                return "";
            }
        }

       

        /// <summary>
        /// Regresas the valor fila.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dataTable">The po tabla.</param>
        /// <param name="columnName">The ps columna.</param>
        /// <returns></returns>
        public static T GetValue<T>(this DataTable dataTable, string columnName)
        {
            Object value;
            if (dataTable.Rows.Count == 0)
            {
                return GetDefaultValue<T>();
            }
            if (!dataTable.Columns.Contains(columnName))
            {
                return GetDefaultValue<T>();
            }
            value = dataTable.Rows[0][columnName];
            if (Convert.IsDBNull(value) || String.IsNullOrEmpty(Convert.ToString(value)))
            {
                return GetDefaultValue<T>();
            }
            return (T)Convert.ChangeType(value, typeof(T));
        }

        /// <summary>
        /// Regresas the valor fila.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dataTable">The po tabla.</param>
        /// <param name="numberRow">The pi fila.</param>
        /// <param name="columnName">The ps columna.</param>
        /// <returns></returns>
        public static T GetValue<T>(this DataTable dataTable, int numberRow, string columnName)
        {
            Object value;
            if (dataTable.Rows.Count == 0)
            {
                return GetDefaultValue<T>();
            }
            if (numberRow >= dataTable.Rows.Count)
            {
                return GetDefaultValue<T>();
            }
            if (!dataTable.Columns.Contains(columnName))
            {
                return GetDefaultValue<T>();
            }
            value = dataTable.Rows[numberRow][columnName];
            if (Convert.IsDBNull(value) || String.IsNullOrEmpty(Convert.ToString(value)))
            {
                return GetDefaultValue<T>();
            }
            return (T)Convert.ChangeType(value, typeof(T));
        }

        /// <summary>
        /// Regresas the valor.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="row">The po fila.</param>
        /// <param name="columnName">The ps columna.</param>
        /// <returns></returns>
        public static T GetValue<T>(this DataRow row, string columnName)
        {
            Object value;
            Type type = typeof(T);
            Type nullableType = Nullable.GetUnderlyingType(type);
            if (!row.Table.Columns.Contains(columnName))
            {
                return GetDefaultValue<T>();
            }
            value = row[columnName];
            if (Convert.IsDBNull(value) || String.IsNullOrEmpty(Convert.ToString(value)))
            {
                return GetDefaultValue<T>();
            }
            if (nullableType != null)
            {
                return (T)Convert.ChangeType(value, nullableType);
            }
            else
            {

                if (value.GetType() == typeof(System.Guid))
                {
                    return (T)Convert.ChangeType(Convert.ToString(value), typeof(T));
                }
                else if (value.GetType() == typeof(System.String) && type == typeof(System.Boolean))
                {
                    return (T)Convert.ChangeType(Convert.ToString(value).ToBoolean(), typeof(T));
                }
                else if (value.GetType() == typeof(System.Int32) && !IsNumeric(Convert.ToString(value)))
                {
                    return GetDefaultValue<T>();
                }
                else if (value.GetType() == typeof(System.DateTimeOffset))
                {
                    DateTimeOffset sourceTime = DateTimeOffset.Parse(Convert.ToString(value));
                    return (T)Convert.ChangeType(sourceTime.DateTime, typeof(T));
                }
                else
                {
                    return (T)Convert.ChangeType(value, typeof(T));
                }
            }
        }
        public static bool IsNumeric(string psValor)
        {
            return Double.TryParse(psValor, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.CurrentCulture, out double result);
        }

        /// <summary>
        /// Determines whether the specified ps valor is numeric.
        /// </summary>
        /// <param name="psValor">The ps valor.</param>
        /// <returns>
        ///   <c>true</c> if the specified ps valor is numeric; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsNumeric(object psValor)
        {
            return Double.TryParse(Convert.ToString(psValor), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.CurrentCulture, out double result);
        }
        /// <summary>
        /// Gets the value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="row">The row.</param>
        /// <param name="columnName">Name of the column.</param>
        /// <returns></returns>
        public static T GetValue<T>(this IDataReader row, string columnName)
        {
            Object value;
            Type type = typeof(T);
            Type nullableType = Nullable.GetUnderlyingType(type);
            if (row.GetOrdinal(columnName) < 0)
            {
                return GetDefaultValue<T>();
            }
            value = row[columnName];
            if (Convert.IsDBNull(value) || String.IsNullOrEmpty(Convert.ToString(value)))
            {
                return GetDefaultValue<T>();
            }
            if (nullableType != null)
                return (T)Convert.ChangeType(value, nullableType);
            else
            {
                if (value.GetType() == typeof(System.Guid))
                    return (T)Convert.ChangeType(Convert.ToString(value), typeof(T));
                else
                    return (T)Convert.ChangeType(value, typeof(T));
            }
        }

        /// <summary>
        /// Gets the value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="row">The row.</param>
        /// <param name="columnName">Name of the column.</param>
        /// <returns></returns>
        public static T GetValue<T>(this DataRowView row, string columnName)
        {
            Object value;
            Type type = typeof(T);
            Type nullableType = Nullable.GetUnderlyingType(type);
            if (!row.Row.Table.Columns.Contains(columnName))
            {
                return GetDefaultValue<T>();
            }
            value = row[columnName];
            if (Convert.IsDBNull(value) || String.IsNullOrEmpty(Convert.ToString(value)))
            {
                return GetDefaultValue<T>();
            }
            if (nullableType != null)
                return (T)Convert.ChangeType(value, nullableType);
            else
            {
                if (value.GetType() == typeof(System.Guid))
                    return (T)Convert.ChangeType(Convert.ToString(value), typeof(T));
                else
                    return (T)Convert.ChangeType(value, typeof(T));
            }
        }
        public static T GetValue<T>(this DataView row, string columnName)
        {
            Object value;
            Type type = typeof(T);
            Type nullableType = Nullable.GetUnderlyingType(type);
            if (row.Count == 0)
                return GetDefaultValue<T>();
            if (!row[0].Row.Table.Columns.Contains(columnName))
                return GetDefaultValue<T>();
            value = row[0][columnName];
            if (Convert.IsDBNull(value) || String.IsNullOrEmpty(Convert.ToString(value)))
                return GetDefaultValue<T>();
            if (nullableType != null)
            {
                return (T)Convert.ChangeType(value, nullableType);
            }
            else
            {
                if (value.GetType() == typeof(System.Guid))
                    return (T)Convert.ChangeType(Convert.ToString(value), typeof(T));
                else
                    return (T)Convert.ChangeType(value, typeof(T));
            }
        }
     
        /// <summary>
        /// Gets the default value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static T GetDefaultValue<T>()
        {
            if (typeof(T) == typeof(DateTime))
                return (T)(object)(new DateTime(1900, 1, 1));
            if (typeof(T) == typeof(String))
                return (T)(object)String.Empty;
            return default;
        }

        /// <summary>
        /// Extension method to return an enum value of type T for the given string.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="psValue"></param>
        /// <returns></returns>
        public static T ToEnum<T>(this string psValue)
        {
            if (String.IsNullOrEmpty(psValue))
                psValue = "es-MX";
            return (T)Enum.Parse(typeof(T), psValue.Replace("_", "-"), true);
        }

        /// <summary>
        /// ByteArrayToImg
        /// </summary>
        /// <param name="byteArrayIn"></param>
        /// <returns></returns>
        public static string ByteArrayToImg(byte[] byteArrayIn)
        {
            using (MemoryStream loStream = new MemoryStream(byteArrayIn))
            {
                return Convert.ToBase64String(loStream.ToArray());
            }
        }

        /// <summary>
        /// Existes the campo en data view.
        /// </summary>
        /// <param name="Pds_Datos">The PDS datos.</param>
        /// <param name="psCampo">The ps campo.</param>
        /// <returns></returns>
        public static bool ExisteCampoEnDataView(this DataView Pds_Datos, string psCampo)
        {
            return Pds_Datos.Table.Columns.Contains(psCampo);
        }

        
       

       
      
        /// <summary>
        /// Downloads the data.
        /// </summary>
        /// <param name="url">The URL.</param>
        /// <returns></returns>
        public static byte[] DownloadData(string url)
        {
            try
            {
                using (WebClient client = new WebClient())
                {
                    return client.DownloadData(url);
                }
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Downloads the FTP data.
        /// </summary>
        /// <param name="url">The URL.</param>
        /// <returns></returns>
        public static byte[] DownloadFtpData(string url)
        {
            try
            {
                FtpWebRequest request = (FtpWebRequest)WebRequest.Create(url);
                request.Method = WebRequestMethods.Ftp.DownloadFile;
                FtpWebResponse response = (FtpWebResponse)request.GetResponse();
                Stream responseStream = response.GetResponseStream();
                StreamReader reader = new StreamReader(responseStream);
                byte[] data;

                using (MemoryStream ms = new MemoryStream())
                {
                    reader.BaseStream.CopyTo(ms);
                    data = ms.ToArray();
                }
                return data;
            }
            catch
            {
                return null;
            }
        }
        public static string DateSql(this DateTime date, bool isHHmmss, DatabaseEngines engines)
        {
            string caracter;
            StringBuilder str;
            switch (engines)
            {
                case DatabaseEngines.SqlServer:
                    caracter = "";
                    break;

                default:
                    caracter = "";
                    break;
            }

            str = new StringBuilder();
            str.AppendFormat("'{0}{3}{1}{3}{2}", (date.Year).ToString().PadLeft(4, '0'), (date.Month).ToString().PadLeft(2, '0'), date.Day.ToString().PadLeft(2, '0'), caracter);
            if (isHHmmss)
            {
                str.AppendFormat(" {0}:{1}:{2}'", date.Hour.ToString().PadLeft(2, '0'), date.Minute.ToString().PadLeft(2, '0'), date.Second.ToString().PadLeft(2, '0'));
            }
            else
            {
                str.AppendFormat("'");
            }
            return str.ToString();
        }
    }
}