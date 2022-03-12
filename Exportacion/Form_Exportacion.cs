using FileHelpers;
using Framework.DataBase;
using Framework.Enumerations;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Drawing;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Framework.Extensions;

namespace Exportacion
{
     public partial class Form_Exportacion : Form
     {
          public Form_Exportacion()
          {
               InitializeComponent();
          }

          private void btn_exportar_Click(object sender, EventArgs e)
          {
               DateTime FechaInicio = dtp_FechaInicio.Value;
               DateTime FechaFinal = dtp_FechaFinal.Value;



               string ruta = Path.GetFullPath("..\\..\\3D IXACHI.mdb");





               DataAccess conection = new DataAccess(@"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + ruta + "", "", DatabaseEngines.OleDb);
               StringBuilder Export_Squery = new StringBuilder();
               StringBuilder Export_Xquery = new StringBuilder();
               StringBuilder STNsSPSX_query = new StringBuilder();
               StringBuilder LinkR_query = new StringBuilder();
               StringBuilder Export_Rquery = new StringBuilder();

               using (HandlesConnection manager = new HandlesConnection(conection))
               {
                    if (cmbTipo.Text == "Todos Los Tipos")
                    {
                         //SQL que crea la datatable  para generar el Archivo S  Todos los tipos

                         /*SQL que crea la datatable  para generar el Archivo S  Todos los tipos
                         SELECT 'S' AS Inicial, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex, TReporte_Est.Tipo As Fuente, TReporte_Est.SourceDepth,
                         IIf(Len([SourceUpholeTime]) = 1,' ' & [SourceUpholeTime],[SourceUpholeTime]) AS Uphole, Format$([WaterDepth], '0.0') AS WaterDep, Format$([Easting], '#.00') AS Este,
                         Format$([Northing], '#.00') AS Norte, IIf(Len(Int([Elevation]))= 2,' ' & Format$([Elevation], '0.00'),IIf(Len(Int([Elevation])) = 1,'  ' & Format$([Elevation], '0.00'),
                         Format$([Elevation], '0.00'))) AS Elevacion, IIf(Len(Format$([SourceEventDate], 'y')) = 1, '  ' & Format$([SourceEventDate], 'y'), IIf(Len(Format$([SourceEventDate], 'y')) = 2,
                         ' ' & Format$([SourceEventDate], 'y'), Format$([SourceEventDate], 'y'))) AS Dia, Format$([SourceEventTime], 'HHnnss') AS Hora INTO ExportSPS_S
                         FROM TReporte_Est WHERE(((TReporte_Est.SourceEventDate) >=#07/28/2021# And (TReporte_Est.SourceEventDate)<=#07/28/2021#)) ORDER BY TReporte_Est.RecordNr;*/


                         Export_Squery.AppendFormat("SELECT 'S' AS Inicial, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex, TReporte_Est.Tipo As Fuente, TReporte_Est.SourceDepth, ");
                         Export_Squery.AppendFormat("IIf(Len([SourceUpholeTime])=1,' ' & [SourceUpholeTime],[SourceUpholeTime]) AS Uphole, Format$([WaterDepth], '0.0') AS WaterDep, Format$([Easting],'#.00') AS Este,");
                         Export_Squery.AppendFormat(" Format$([Northing],'#.00') AS Norte, IIf(Len(Int([Elevation]))=2,' ' & Format$([Elevation],'0.00'),IIf(Len(Int([Elevation]))=1,'  ' & Format$([Elevation],'0.00'),");
                         Export_Squery.AppendFormat("Format$([Elevation],'0.00'))) AS Elevacion, IIf(Len(Format$([SourceEventDate],'y')) = 1, '  ' & Format$([SourceEventDate],'y'), IIf(Len(Format$([SourceEventDate],'y')) = 2,");
                         Export_Squery.AppendFormat(" ' ' & Format$([SourceEventDate],'y'), Format$([SourceEventDate],'y'))) AS Dia, Format$([SourceEventTime],'HHnnss') AS Hora");
                         Export_Squery.AppendFormat(" FROM TReporte_Est ");
                         Export_Squery.AppendFormat("WHERE (((TReporte_Est.SourceEventDate)>={0} And (TReporte_Est.SourceEventDate)<={1})) ORDER BY TReporte_Est.RecordNr;", FechaInicio.DateSql(false, DatabaseEngines.OleDb), FechaFinal.DateSql(false, DatabaseEngines.OleDb));

                         //DataTable ExportSPS_SDatatable = conection.GetDataTable(Export_Squery);


                         //SQL que crea la datatable  para generar el Archivo X Todos los tipos

                         /*SELECT 'X' AS Inicial, TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, 11 AS BaseLineIndex, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex, 
                        IIf(Len([Chann1])= 1,'    ' & [Chann1],IIf(Len([Chann1]) = 2,'   ' & [Chann1],IIf(Len([Chann1]) = 3,'  ' & [Chann1], IIf(Len([Chann1]) = 4,' ' & [Chann1],[Chann1])))) AS FstCanal,
                        IIf(Len([ChannU])= 1,'    ' & [ChannU],IIf(Len([ChannU]) = 2,'   ' & [ChannU],IIf(Len([ChannU]) = 3,'  ' & [ChannU],IIf(Len([ChannU]) = 4,' ' & [ChannU], [ChannU])))) AS LastCanal,
                        1 AS LineIndex, [Spread - VT].RecLine As RecepLine, [Spread-VT].[1aSTN] As PSTN, [Spread - VT].LsSTN As USTN, [Spread-VT].IndexSTN AS VersionIdx INTO ExportSPS_X 
                        FROM TReporte_Est INNER JOIN[Spread - VT] ON(TReporte_Est.ActPointNr = [Spread - VT].ActPointNr) AND(TReporte_Est.ActLineNr = [Spread - VT].ActLineNr) 
                        WHERE(((TReporte_Est.SourceEventDate) >= #07/28/2021# And (TReporte_Est.SourceEventDate) <= #07/28/2021#)) ORDER BY TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr,
                        TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, [Spread - VT].Chann1;*/


                         Export_Xquery.AppendFormat("SELECT 'X' AS Inicial, TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, 11 AS BaseLineIndex, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex,");
                         Export_Xquery.AppendFormat(" IIf(Len([Chann1])=1,'    ' & [Chann1],IIf(Len([Chann1])=2,'   ' & [Chann1],IIf(Len([Chann1])=3,'  ' & [Chann1], IIf(Len([Chann1])=4,' ' & [Chann1],[Chann1])))) AS FstCanal,");
                         Export_Xquery.AppendFormat(" IIf(Len([ChannU])=1,'    ' & [ChannU],IIf(Len([ChannU])=2,'   ' & [ChannU],IIf(Len([ChannU])=3,'  ' & [ChannU],IIf(Len([ChannU])=4,' ' & [ChannU], [ChannU])))) AS LastCanal,");
                         Export_Xquery.AppendFormat(" 1 AS LineIndex, [Spread-VT].RecLine As RecepLine, [Spread-VT].[1aSTN] As PSTN, [Spread-VT].LsSTN As USTN, [Spread-VT].IndexSTN AS VersionIdx ");
                         Export_Xquery.AppendFormat("FROM TReporte_Est INNER JOIN [Spread-VT] ON (TReporte_Est.ActPointNr = [Spread-VT].ActPointNr) AND (TReporte_Est.ActLineNr = [Spread-VT].ActLineNr)");
                         Export_Xquery.AppendFormat(" WHERE (((TReporte_Est.SourceEventDate) >={0} And (TReporte_Est.SourceEventDate) <= {1}))", FechaInicio.DateSql(false, DatabaseEngines.OleDb), FechaFinal.DateSql(false, DatabaseEngines.OleDb));
                         Export_Xquery.AppendFormat(" ORDER BY TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, [Spread-VT].Chann1");
                         //DataTable ExportSPS_XDatatable = conection.GetDataTable(Export_Xquery);
                    }

                    else
                    {
                         // SQL que crea la datatable  para generar el Archivo S E1 y V1


                         /*SELECT 'S' AS Inicial, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex, TReporte_Est.Tipo As Fuente, TReporte_Est.SourceDepth,
                         IIf(Len([SourceUpholeTime]) = 1,' ' & [SourceUpholeTime],[SourceUpholeTime]) AS Uphole, Format$([WaterDepth], '0.0') AS WaterDep, Format$([Easting], '#.00') AS Este, 
                         Format$([Northing], '#.00') AS Norte, IIf(Len(Int([Elevation]))= 2,' ' & Format$([Elevation], '0.00'),IIf(Len(Int([Elevation])) = 1,'  ' & Format$([Elevation], '0.00'),
                         Format$([Elevation], '0.00'))) AS Elevacion, IIf(Len(Format$([SourceEventDate], 'y')) = 1, '  ' & Format$([SourceEventDate], 'y'), IIf(Len(Format$([SourceEventDate], 'y')) = 2,
                         ' ' & Format$([SourceEventDate], 'y'), Format$([SourceEventDate], 'y'))) AS Dia, Format$([SourceEventTime], 'HHnnss') AS Hora INTO ExportSPS_S 
                         FROM TReporte_Est 
                         WHERE(((TReporte_Est.SourceEventDate) >=#07/28/2021# And (TReporte_Est.SourceEventDate)<=#07/28/2021#) And TReporte_Est.Tipo='V1') 
                         ORDER BY TReporte_Est.RecordNr;*/



                         Export_Squery.AppendFormat("SELECT 'S' AS Inicial, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex, TReporte_Est.Tipo As Fuente, TReporte_Est.SourceDepth,");
                         Export_Squery.AppendFormat(" IIf(Len([SourceUpholeTime])=1,' ' & [SourceUpholeTime],[SourceUpholeTime]) AS Uphole, Format$([WaterDepth], '0.0') AS WaterDep, Format$([Easting],'#.00') AS Este,");
                         Export_Squery.AppendFormat(" Format$([Northing],'#.00') AS Norte, IIf(Len(Int([Elevation]))=2,' ' & Format$([Elevation],'0.00'),IIf(Len(Int([Elevation]))=1,'  ' & Format$([Elevation],'0.00'),");
                         Export_Squery.AppendFormat("Format$([Elevation],'0.00'))) AS Elevacion, IIf(Len(Format$([SourceEventDate],'y')) = 1, '  ' & Format$([SourceEventDate],'y'), IIf(Len(Format$([SourceEventDate],'y')) = 2,");
                         Export_Squery.AppendFormat(" ' ' & Format$([SourceEventDate],'y'), Format$([SourceEventDate],'y'))) AS Dia, Format$([SourceEventTime],'HHnnss') AS Hora ");
                         Export_Squery.AppendFormat("FROM TReporte_Est ");
                         Export_Squery.AppendFormat("WHERE (((TReporte_Est.SourceEventDate)>= {0} And (TReporte_Est.SourceEventDate)<={1}) And TReporte_Est.Tipo='" + cmbTipo.Text + "') ", FechaInicio.DateSql(false, DatabaseEngines.OleDb), FechaFinal.DateSql(false, DatabaseEngines.OleDb));
                         Export_Squery.AppendFormat("ORDER BY TReporte_Est.RecordNr;");

                         //DataTable ExportSPS_SDatabla = conection.GetDataTable(Export_Squery);

                         //SQL que crea la datatable  para generar el Archivo X e1 y v1

                         /*SELECT 'X' AS Inicial, TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, 11 AS BaseLineIndex, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex, 
                         IIf(Len([Chann1])= 1,'    ' & [Chann1],IIf(Len([Chann1]) = 2,'   ' & [Chann1],IIf(Len([Chann1]) = 3,'  ' & [Chann1], IIf(Len([Chann1]) = 4,' ' & [Chann1],[Chann1])))) AS FstCanal,
                         IIf(Len([ChannU])= 1,'    ' & [ChannU],IIf(Len([ChannU]) = 2,'   ' & [ChannU],IIf(Len([ChannU]) = 3,'  ' & [ChannU],IIf(Len([ChannU]) = 4,' ' & [ChannU], [ChannU])))) AS LastCanal,
                         1 AS LineIndex, [Spread - VT].RecLine As RecepLine, [Spread-VT].[1aSTN] As PSTN, [Spread - VT].LsSTN As USTN, [Spread-VT].IndexSTN AS VersionIdx INTO ExportSPS_X
                         FROM TReporte_Est INNER JOIN[Spread - VT] ON(TReporte_Est.ActPointNr = [Spread - VT].ActPointNr) AND(TReporte_Est.ActLineNr = [Spread - VT].ActLineNr) 
                         WHERE(((TReporte_Est.SourceEventDate) >= #07/28/2021# And (TReporte_Est.SourceEventDate) <= #07/28/2021#) And TReporte_Est.Tipo='V1') 
                             ORDER BY TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, [Spread - VT].Chann1;*/



                         Export_Xquery.AppendFormat("SELECT 'X' AS Inicial, TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, 11 AS BaseLineIndex, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex,");
                         Export_Xquery.AppendFormat(" IIf(Len([Chann1])=1,'    ' & [Chann1],IIf(Len([Chann1])=2,'   ' & [Chann1],IIf(Len([Chann1])=3,'  ' & [Chann1], IIf(Len([Chann1])=4,' ' & [Chann1],[Chann1])))) AS FstCanal,");
                         Export_Xquery.AppendFormat(" IIf(Len([ChannU])=1,'    ' & [ChannU],IIf(Len([ChannU])=2,'   ' & [ChannU],IIf(Len([ChannU])=3,'  ' & [ChannU],IIf(Len([ChannU])=4,' ' & [ChannU], [ChannU])))) AS LastCanal,");
                         Export_Xquery.AppendFormat(" 1 AS LineIndex, [Spread-VT].RecLine As RecepLine, [Spread-VT].[1aSTN] As PSTN, [Spread-VT].LsSTN As USTN, [Spread-VT].IndexSTN AS VersionIdx ");
                         Export_Xquery.AppendFormat("FROM TReporte_Est ");
                         Export_Xquery.AppendFormat("INNER JOIN [Spread-VT] ON (TReporte_Est.ActPointNr = [Spread-VT].ActPointNr) AND (TReporte_Est.ActLineNr = [Spread-VT].ActLineNr) ");
                         Export_Xquery.AppendFormat("WHERE (((TReporte_Est.SourceEventDate) >= {0} And (TReporte_Est.SourceEventDate) <= {1}) And TReporte_Est.Tipo='" + cmbTipo.Text + "') ", FechaInicio.DateSql(false, DatabaseEngines.OleDb), FechaFinal.DateSql(false, DatabaseEngines.OleDb));
                         Export_Xquery.AppendFormat("ORDER BY TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, [Spread-VT].Chann1");
                         //DataTable ExportSPS_XDatatable = conection.GetDataTable(Export_Xquery);


                    }
                    DataTable ExportSPS_SDatatable = conection.GetDataTable(Export_Squery);
                    DataTable ExportSPS_XDatatable = conection.GetDataTable(Export_Xquery);
                    //SQL que crea el datatable STNsSPSX

                    //SELECT RecepLine, PSTN, USTN, VersionIdx INTO[STNsSPSX] FROM ExportSPS_X GROUP BY RecepLine, PSTN, USTN, VersionIdx


                    STNsSPSX_query.Append("SELECT RecepLine, PSTN, USTN, VersionIdx ");
                    STNsSPSX_query.AppendFormat(" FROM ({0}) ", Export_Xquery);
                    STNsSPSX_query.Append(" GROUP BY RecepLine, PSTN, USTN, VersionIdx");
                    DataTable STNsSPSX_Datatable = conection.GetDataTable(STNsSPSX_query);

                    //SQL que crea el datatable LinkR

                    //SELECT LReceptoras.BaseLineNr, LReceptoras.BasePointNr, LReceptoras.IndexSTN INTO LinkR FROM LReceptoras WHERE(((LReceptoras.BaseLineNr) Is Null));
                    //Verificar si existe la tabla
                    if (conection.ExistsObjectInDataBase("LinkR"))
                    {
                         LinkR_query = new StringBuilder();
                         LinkR_query.Append(" drop table LinkR ");
                         conection.ExecuteCommand(LinkR_query);

                    }
                    LinkR_query = new StringBuilder();
                    LinkR_query.Append("SELECT LReceptoras.BaseLineNr, LReceptoras.BasePointNr, LReceptoras.IndexSTN INTO LinkR ");
                    LinkR_query.Append(" FROM LReceptoras  WHERE LReceptoras.BaseLineNr Is Null");
                    conection.ExecuteCommand(LinkR_query);

                    LinkR_query = new StringBuilder();
                    LinkR_query.Append("SELECT LReceptoras.BaseLineNr, LReceptoras.BasePointNr, LReceptoras.IndexSTN");
                    LinkR_query.Append(" FROM LReceptoras  WHERE LReceptoras.BaseLineNr Is Null)");
                    //DataTable LinkR_Datatable = conection.GetDataTable(LinkR_query);

                    //Update//Insert directo a la base de datos

                    /*'Este SQL prepara las Estaciones que esten en el archivo X y las anexa a la tabla LinkR

                    Set rstLinkR = CurrentDb.OpenRecordset("LinkR")
                    Set rstRelacional = CurrentDb.OpenRecordset("STNsSPSX")
                    rstRelacional.MoveFirst
                    For i = 1 To rstRelacional.RecordCount
                        RecLine = rstRelacional!RecepLine
                        STN1a = rstRelacional!PSTN
                        STNLst = rstRelacional!USTN
                        STNIdx = rstRelacional![VersionIdx]
                        For Estacion = STN1a To STNLst
                            rstLinkR.AddNew
                            rstLinkR!BaseLineNR = RecLine
                            rstLinkR!BasePointNR = Estacion
                            rstLinkR!IndexSTN = STNIdx
                            rstLinkR.Update
                        Next
                        rstRelacional.MoveNext
                    Next
                    rstLinkR.Close
                    rstRelacional.Close*/
                    foreach (DataRow rst in STNsSPSX_Datatable.Rows)
                    {
                         for (int estacion = rst.GetValue<int>("PSTN"); estacion <= rst.GetValue<int>("USTN"); estacion++)
                         {
                              StringBuilder query = new StringBuilder();
                              query.AppendFormat("Insert into LinkR (BaseLineNr, BasePointNr, IndexSTN) ");
                              query.AppendFormat("values ({0},{1},{2})", rst.GetValue<string>("RecepLine"), estacion, rst.GetValue<string>("VersionIdx"));
                              conection.ExecuteCommand(query);
                         }
                    }

                    
                    //SQL que crea la TABLA ExportSPS_R en base a la Tabla Exportada ExportSPS_X

                    /*SELECT 'R' AS Inicial, LinkR.BaseLineNr, LinkR.BasePointNr, LinkR.IndexSTN AS RecLineIndex, IIf(LReceptoras.Tipo is not null, LReceptoras.Tipo, [Preplot - R].Tipo) As Detector,
                        IIf([LReceptoras].[Easting] Is Not Null,Format$([LReceptoras].[Easting], '#.00'),Format$([Preplot - R].[Easting], '#.00')) AS Este, IIf([LReceptoras].[Northing] Is Not Null,
                        Format$([LReceptoras].[Northing], '#.00'),Format$([Preplot - R].[Northing], '#.00')) AS Norte, IIf(Int([Elevation])= 0,'  ' & Format$([Elevation], '0.0'),IIf(Int([Elevation]) = -1,
                        ' ' & Format$([Elevation], '0.0'),IIf(Len(Int([Elevation])) = 2,' ' & Format$([Elevation], '#.00'),IIf(Len(Int([Elevation])) = 1,'  ' & Format$([Elevation], '#.00'),
                        Format$([Elevation], '#.00'))))) AS Elevacion, 1000001 AS BoxVersion INTO ExportSPS_R 
                        FROM(LinkR LEFT JOIN LReceptoras ON(LinkR.IndexSTN = LReceptoras.IndexSTN) AND(LinkR.BaseLineNr = LReceptoras.BaseLineNr) AND(LinkR.BasePointNr = LReceptoras.BasePointNr))
                        INNER JOIN[Preplot - R] ON(LinkR.BaseLineNr = [Preplot - R].BaseLineNr) AND(LinkR.BasePointNr = [Preplot - R].BasePointNr)
                        Group BY 'R', LinkR.BaseLineNr, LinkR.BasePointNr, LinkR.IndexSTN, IIf(LReceptoras.Tipo is not null, LReceptoras.Tipo, [Preplot - R].Tipo), LReceptoras.BaseLineNr,
                        LReceptoras.BasePointNr, LReceptoras.IndexSTN, LReceptoras.Easting, LReceptoras.Northing, LReceptoras.Elevation, [Preplot-R].Easting, [Preplot-R].Northing 
                        ORDER BY LinkR.BaseLineNr, LinkR.BasePointNr;*/

                    /*Export_Rquery.AppendFormat("SELECT 'R' AS Inicial, LinkR.BaseLineNr, LinkR.BasePointNr, LinkR.IndexSTN AS RecLineIndex, IIf(LReceptoras.Tipo is not null, LReceptoras.Tipo, [Preplot-R].Tipo) As Detector,");
                    Export_Rquery.AppendFormat(" IIf([LReceptoras].[Easting] Is Not Null,Format$([LReceptoras].[Easting],'#.00'),Format$([Preplot-R].[Easting],'#.00')) AS Este, IIf([LReceptoras].[Northing] Is Not Null,");
                    Export_Rquery.AppendFormat("Format$([LReceptoras].[Northing],'#.00'),Format$([Preplot-R].[Northing],'#.00')) AS Norte, IIf(Int([Elevation])=0,'  ' & Format$([Elevation],'0.0'),IIf(Int([Elevation])=-1,");
                    Export_Rquery.AppendFormat("' ' & Format$([Elevation],'0.0'),IIf(Len(Int([Elevation]))=2,' ' & Format$([Elevation],'#.00'),IIf(Len(Int([Elevation]))=1,'  ' & Format$([Elevation],'#.00'),");
                    Export_Rquery.AppendFormat(" FROM ({0}) LinkR  ", LinkR_query);
                    Export_Rquery.AppendFormat("LEFT JOIN LReceptoras ON (LinkR.IndexSTN = LReceptoras.IndexSTN) AND (LinkR.BaseLineNr = LReceptoras.BaseLineNr) AND (LinkR.BasePointNr = LReceptoras.BasePointNr)) ");
                    Export_Rquery.AppendFormat("INNER JOIN [Preplot-R] ON (LinkR.BaseLineNr = [Preplot-R].BaseLineNr) AND (LinkR.BasePointNr = [Preplot-R].BasePointNr) ");
                    Export_Rquery.AppendFormat("Group BY 'R', LinkR.BaseLineNr, LinkR.BasePointNr, LinkR.IndexSTN, ");
                    Export_Rquery.AppendFormat("IIf(LReceptoras.Tipo is not null, LReceptoras.Tipo, [Preplot-R].Tipo),");
                    Export_Rquery.AppendFormat(" LReceptoras.BaseLineNr, LReceptoras.BasePointNr, LReceptoras.IndexSTN, LReceptoras.Easting, LReceptoras.Northing, LReceptoras.Elevation, [Preplot-R].Easting, [Preplot-R].Northing ");
                    Export_Rquery.AppendFormat("ORDER BY LinkR.BaseLineNr, LinkR.BasePointNr;");*/
                    

                    Export_Rquery.AppendFormat("SELECT 'R' AS Inicial, LinkR.BaseLineNr, LinkR.BasePointNr, LinkR.IndexSTN AS RecLineIndex, IIf(LReceptoras.Tipo is not null, LReceptoras.Tipo, [Preplot-R].Tipo) As Detector,");
                    Export_Rquery.AppendFormat(" IIf([LReceptoras].[Easting] Is Not Null,Format$([LReceptoras].[Easting],'#.00'),Format$([Preplot-R].[Easting],'#.00')) AS Este, IIf([LReceptoras].[Northing] Is Not Null,");
                    Export_Rquery.AppendFormat("Format$([LReceptoras].[Northing],'#.00'),Format$([Preplot-R].[Northing],'#.00')) AS Norte, IIf(Int([Elevation])=0,'  ' & Format$([Elevation],'0.0'),IIf(Int([Elevation])=-1,");
                    Export_Rquery.AppendFormat("' ' & Format$([Elevation],'0.0'),IIf(Len(Int([Elevation]))=2,' ' & Format$([Elevation],'#.00'),IIf(Len(Int([Elevation]))=1,'  ' & Format$([Elevation],'#.00'),Format$([Elevation],'#.00')))))");
                    Export_Rquery.AppendFormat(" AS Elevacion, 1000001 AS BoxVersion  " );
                    Export_Rquery.AppendFormat("FROM  (LinkR ");
                    Export_Rquery.AppendFormat("LEFT JOIN LReceptoras ON (LinkR.IndexSTN = LReceptoras.IndexSTN) AND (LinkR.BaseLineNr = LReceptoras.BaseLineNr) AND (LinkR.BasePointNr = LReceptoras.BasePointNr))");
                    Export_Rquery.AppendFormat(" INNER JOIN [Preplot-R] ON (LinkR.BaseLineNr = [Preplot-R].BaseLineNr) AND (LinkR.BasePointNr = [Preplot-R].BasePointNr) ");
                    Export_Rquery.AppendFormat("Group BY 'R', LinkR.BaseLineNr, LinkR.BasePointNr, LinkR.IndexSTN,");
                    Export_Rquery.AppendFormat(" IIf(LReceptoras.Tipo is not null, LReceptoras.Tipo, [Preplot-R].Tipo), LReceptoras.BaseLineNr, LReceptoras.BasePointNr, LReceptoras.IndexSTN, LReceptoras.Easting, LReceptoras.Northing,");
                    Export_Rquery.AppendFormat(" LReceptoras.Elevation, [Preplot-R].Easting, [Preplot-R].Northing ");
                    Export_Rquery.AppendFormat("ORDER BY LinkR.BaseLineNr, LinkR.BasePointNr;");
                    DataTable ExportSPS_R = conection.GetDataTable(Export_Rquery);



               }
          }
          public void Form_Exportacion_Load(object sender, EventArgs e)
          {
               CultureInfo.CreateSpecificCulture("en-US");
               cmbTipo.Text = "Todos Los Tipos";
          }

          private void Carpeta_Click(object sender, EventArgs e)
          {

               string FechaInicio = dtp_FechaInicio.Text;
               string FechaFinal = dtp_FechaFinal.Text;
               int FechaInicioLength = FechaInicio.Length;
               int FechaFinLength = FechaFinal.Length;

               FolderBrowserDialog Carpeta = new FolderBrowserDialog();

               if (Carpeta.ShowDialog() == DialogResult.OK)

                    if (!(FechaInicioLength > 0) | !(FechaFinLength > 0))
                         MessageBox.Show("Es necesario que introduzca una Fecha inicio y una Fecha final para la exportación del archivo");

               {
                    txt_ruta.Text = string.Format("{0}\\SPS {1} a {2}", Carpeta.SelectedPath, dtp_FechaInicio.Text, dtp_FechaFinal.Text);
               }
          }

          public void pictureBox1_Click(object sender, EventArgs e)
          {
               /*
               OpenFileDialog openFileDialog1 = new OpenFileDialog();
               openFileDialog1.Title = "Select File";
               openFileDialog1.InitialDirectory = @"C:\";//--"C:\\";
               openFileDialog1.Filter = "All files (*.*)|*.*|File (*.mdb)|*.mdb";
               openFileDialog1.FilterIndex = 2;
               openFileDialog1.ShowDialog();
               if (openFileDialog1.FileName != "")
               {
                    //string ruta = openFileDialog1.FileName;

                   Configuration config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
                   config.AppSettings.Settings["miParametro"].Value = openFileDialog1.FileName;
                   config.Save(ConfigurationSaveMode.Modified);
               }
               else
               { 
                   MessageBox.Show("You didn't select the file!"); 
               }
               */
          }

     }
}
