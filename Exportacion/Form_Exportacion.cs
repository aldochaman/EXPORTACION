using Framework.DataBase;
using Framework.Enumerations;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

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
            string FechaInicio = string.Format(dtp_FechaInicio.Text,"MM/dd/yyyy");
            string FechaFinal = string.Format(dtp_FechaFinal.Text,"MM/dd/yyyy");


            int FechaInicioLength = FechaInicio.Length;
            int FechaFinLength = FechaFinal.Length;

            if (!(FechaInicioLength > 0) | !(FechaFinLength > 0))
                MessageBox.Show("Es necesario que introduzca una Fecha inicio y una Fecha final para la exportación del archivo");
            else
            {
                DataAccess conection = new DataAccess(@"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=C:\3D IXACHI.mdb", "", DatabaseEngines.OleDb);
                StringBuilder sql = new StringBuilder();
                StringBuilder sql2 = new StringBuilder();
                StringBuilder sql3 = new StringBuilder();
                StringBuilder sql4 = new StringBuilder();
                StringBuilder sql5 = new StringBuilder();
                StringBuilder sql6 = new StringBuilder();
                StringBuilder sql7 = new StringBuilder();

                if (cmbTipo.Text == "Todos Los Tipos")
                {
                    //SQL que crea la datatable ExportSPS_S para generar el Archivo S
                    
                    sql.Append("SELECT 'S' AS Inicial, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex, TReporte_Est.Tipo As Fuente, TReporte_Est.SourceDepth, IIf(Len([SourceUpholeTime])=1,' ' & [SourceUpholeTime],[SourceUpholeTime]) AS Uphole, Format$([WaterDepth], '0.0') AS WaterDep, Format$([Easting],'#.00') AS Este, Format$([Northing],'#.00') AS Norte, IIf(Len(Int([Elevation]))=2,' ' & Format$([Elevation],'0.00'),IIf(Len(Int([Elevation]))=1,'  ' & Format$([Elevation],'0.00'),Format$([Elevation],'0.00'))) AS Elevacion, IIf(Len(Format$([SourceEventDate],'y')) = 1, '  ' & Format$([SourceEventDate],'y'), IIf(Len(Format$([SourceEventDate],'y')) = 2, ' ' & Format$([SourceEventDate],'y'), Format$([SourceEventDate],'y'))) AS Dia, Format$([SourceEventTime],'HHnnss') AS Hora FROM TReporte_Est WHERE (((TReporte_Est.SourceEventDate)>=#" + FechaInicio + "# And (TReporte_Est.SourceEventDate)<=#" + FechaFinal + "#)) ORDER BY TReporte_Est.RecordNr;");
                    DataTable ExportSPS_S = conection.GetDataTable(sql);

                    //SQL que crea la datatable ExportSPS_X para generar el Archivo X

                    sql2.Append("SELECT 'X' AS Inicial, TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, 11 AS BaseLineIndex, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex, IIf(Len([Chann1])=1,'    ' & [Chann1],IIf(Len([Chann1])=2,'   ' & [Chann1],IIf(Len([Chann1])=3,'  ' & [Chann1], IIf(Len([Chann1])=4,' ' & [Chann1],[Chann1])))) AS FstCanal, IIf(Len([ChannU])=1,'    ' & [ChannU],IIf(Len([ChannU])=2,'   ' & [ChannU],IIf(Len([ChannU])=3,'  ' & [ChannU],IIf(Len([ChannU])=4,' ' & [ChannU], [ChannU])))) AS LastCanal, 1 AS LineIndex, [Spread-VT].RecLine As RecepLine, [Spread-VT].[1aSTN] As PSTN, [Spread-VT].LsSTN As USTN, [Spread-VT].IndexSTN AS VersionIdx FROM TReporte_Est INNER JOIN [Spread-VT] ON (TReporte_Est.ActPointNr = [Spread-VT].ActPointNr) AND (TReporte_Est.ActLineNr = [Spread-VT].ActLineNr) WHERE (((TReporte_Est.SourceEventDate) >= #" + FechaInicio + "# And (TReporte_Est.SourceEventDate) <= #" + FechaFinal + "#)) ORDER BY TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, [Spread-VT].Chann1;");
                    DataTable ExportSPS_X = conection.GetDataTable(sql2);
                    MessageBox.Show("correcto");
                }
                                
                else
                {
                    //SQL que crea la datatable ExportSPS_S para generar el Archivo S

                    sql.Append("SELECT 'S' AS Inicial, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex, TReporte_Est.Tipo As Fuente, TReporte_Est.SourceDepth, IIf(Len([SourceUpholeTime])=1,' ' & [SourceUpholeTime],[SourceUpholeTime]) AS Uphole, Format$([WaterDepth], '0.0') AS WaterDep, Format$([Easting],'#.00') AS Este, Format$([Northing],'#.00') AS Norte, IIf(Len(Int([Elevation]))=2,' ' & Format$([Elevation],'0.00'),IIf(Len(Int([Elevation]))=1,'  ' & Format$([Elevation],'0.00'),Format$([Elevation],'0.00'))) AS Elevacion, IIf(Len(Format$([SourceEventDate],'y')) = 1, '  ' & Format$([SourceEventDate],'y'), IIf(Len(Format$([SourceEventDate],'y')) = 2, ' ' & Format$([SourceEventDate],'y'), Format$([SourceEventDate],'y'))) AS Dia, Format$([SourceEventTime],'HHnnss') AS Hora FROM TReporte_Est WHERE (((TReporte_Est.SourceEventDate)>=#" + FechaInicio + "# And (TReporte_Est.SourceEventDate)<=#" + FechaFinal+ "#) And TReporte_Est.Tipo='" + cmbTipo.Text + "') ORDER BY TReporte_Est.RecordNr;");
                    DataTable ExportSPS_SArchivoS = conection.GetDataTable(sql);

                    //SQL que crea la datatable ExportSPS_X para generar el Archivo X

                    sql2.Append("SELECT 'X' AS Inicial, TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, 11 AS BaseLineIndex, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex, IIf(Len([Chann1])=1,'    ' & [Chann1],IIf(Len([Chann1])=2,'   ' & [Chann1],IIf(Len([Chann1])=3,'  ' & [Chann1], IIf(Len([Chann1])=4,' ' & [Chann1],[Chann1])))) AS FstCanal, IIf(Len([ChannU])=1,'    ' & [ChannU],IIf(Len([ChannU])=2,'   ' & [ChannU],IIf(Len([ChannU])=3,'  ' & [ChannU],IIf(Len([ChannU])=4,' ' & [ChannU], [ChannU])))) AS LastCanal, 1 AS LineIndex, [Spread-VT].RecLine As RecepLine, [Spread-VT].[1aSTN] As PSTN, [Spread-VT].LsSTN As USTN, [Spread-VT].IndexSTN AS VersionIdx FROM TReporte_Est INNER JOIN [Spread-VT] ON (TReporte_Est.ActPointNr = [Spread-VT].ActPointNr) AND (TReporte_Est.ActLineNr = [Spread-VT].ActLineNr) WHERE (((TReporte_Est.SourceEventDate) >= #" + FechaInicio + "# And (TReporte_Est.SourceEventDate) <= #" + FechaFinal + "#) And TReporte_Est.Tipo='" + cmbTipo.Text + "') ORDER BY TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, [Spread-VT].Chann1;");
                    DataTable ExportSPS_X = conection.GetDataTable(sql2);


                }

                //SQL que crea el datatable STNsSPSX
                sql3.Append("SELECT RecepLine, PSTN, USTN, VersionIdx FROM ExportSPS_X GROUP BY RecepLine, PSTN, USTN, VersionIdx");
                DataTable STNsSPSX = conection.GetDataTable(sql3);

                //SQL que crea el datatable LinkR
                sql4.Append("SELECT LReceptoras.BaseLineNr, LReceptoras.BasePointNr, LReceptoras.IndexSTN FROM LReceptoras  WHERE (((LReceptoras.BaseLineNr) Is Null));");
                DataTable LinkR = conection.GetDataTable(sql4);

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

                //SQL que crea la TABLA ExportSPS_R en base a la Tabla Exportada ExportSPS_X

                sql5.Append("SELECT 'R' AS Inicial, LinkR.BaseLineNr, LinkR.BasePointNr, LinkR.IndexSTN AS RecLineIndex, IIf(LReceptoras.Tipo is not null, LReceptoras.Tipo, [Preplot-R].Tipo) As Detector, IIf([LReceptoras].[Easting] Is Not Null,Format$([LReceptoras].[Easting],'#.00'),Format$([Preplot-R].[Easting],'#.00')) AS Este, IIf([LReceptoras].[Northing] Is Not Null,Format$([LReceptoras].[Northing],'#.00'),Format$([Preplot-R].[Northing],'#.00')) AS Norte, IIf(Int([Elevation])=0,'  ' & Format$([Elevation],'0.0'),IIf(Int([Elevation])=-1,' ' & Format$([Elevation],'0.0'),IIf(Len(Int([Elevation]))=2,' ' & Format$([Elevation],'#.00'),IIf(Len(Int([Elevation]))=1,'  ' & Format$([Elevation],'#.00'),Format$([Elevation],'#.00'))))) AS Elevacion, 1000001 AS BoxVersion FROM (LinkR LEFT JOIN LReceptoras ON (LinkR.IndexSTN = LReceptoras.IndexSTN) AND (LinkR.BaseLineNr = LReceptoras.BaseLineNr) AND (LinkR.BasePointNr = LReceptoras.BasePointNr)) INNER JOIN [Preplot-R] ON (LinkR.BaseLineNr = [Preplot-R].BaseLineNr) AND (LinkR.BasePointNr = [Preplot-R].BasePointNr) Group BY 'R', LinkR.BaseLineNr, LinkR.BasePointNr, LinkR.IndexSTN, IIf(LReceptoras.Tipo is not null, LReceptoras.Tipo, [Preplot-R].Tipo), LReceptoras.BaseLineNr, LReceptoras.BasePointNr, LReceptoras.IndexSTN, LReceptoras.Easting, LReceptoras.Northing, LReceptoras.Elevation, [Preplot-R].Easting, [Preplot-R].Northing ORDER BY LinkR.BaseLineNr, LinkR.BasePointNr;");
                DataTable ExportSPS_R = conection.GetDataTable(sql5);
            }
        }
        private void Form_Exportacion_Load(object sender, EventArgs e)
        {
            CultureInfo.CreateSpecificCulture("en-US");
            cmbTipo.Text = "Todos Los Tipos";
        }

        private void Carpeta_Click(object sender, EventArgs e)
        {
            FolderBrowserDialog Carpeta = new FolderBrowserDialog();
            string FechaInicio = dtp_FechaInicio.Text;
            string FechaFinal = dtp_FechaFinal.Text;


            int FechaInicioLength = FechaInicio.Length;
            int FechaFinLength = FechaFinal.Length;

            
            if (Carpeta.ShowDialog() == DialogResult.OK)
            
                if (!(FechaInicioLength > 0) | !(FechaFinLength > 0))
                    MessageBox.Show("Es necesario que introduzca una Fecha inicio y una Fecha final para la exportación del archivo");
                else
                {
                    txt_ruta.Text = string.Format("{0}\\SPS {1} a {2}", Carpeta.SelectedPath, dtp_FechaInicio.Text, dtp_FechaFinal.Text);
                }
            }
        }
    }

