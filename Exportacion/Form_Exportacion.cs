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
            string FechaInicio = string.Format(dtp_FechaInicio.Text, "MM/dd/yyyy");
            string FechaFinal = string.Format(dtp_FechaFinal.Text, "MM/dd/yyyy");



            string ruta = Path.GetFullPath("..\\..\\3D IXACHI.mdb");

            //MessageBox.Show(ruta);
            int FechaInicioLength = FechaInicio.Length;
            int FechaFinLength = FechaFinal.Length;

            if (!(FechaInicioLength > 0) | !(FechaFinLength > 0))
                MessageBox.Show("Es necesario que introduzca una Fecha inicio y una Fecha final para la exportación del archivo");
            else
            {

                DataAccess conection = new DataAccess(@"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + ruta + "", "", DatabaseEngines.OleDb);
                StringBuilder Export_Squery = new StringBuilder();
                StringBuilder Export_Xquery = new StringBuilder();
                StringBuilder STNsSPSX_query = new StringBuilder();
                StringBuilder LinkR_query = new StringBuilder();
                StringBuilder Export_Rquery = new StringBuilder();


                if (cmbTipo.Text == "Todos Los Tipos")
                {
                    //SQL que crea la datatable  para generar el Archivo S  Todos los tipos

                    Export_Squery.Append("SELECT 'S' AS Inicial, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex, TReporte_Est.Tipo As Fuente, TReporte_Est.SourceDepth, ");
                    Export_Squery.Append("IIf(Len([SourceUpholeTime])=1,' ' & [SourceUpholeTime],[SourceUpholeTime]) AS Uphole, Format$([WaterDepth], '0.0') AS WaterDep, Format$([Easting],'#.00') AS Este,");
                    Export_Squery.Append(" Format$([Northing],'#.00') AS Norte, IIf(Len(Int([Elevation]))=2,' ' & Format$([Elevation],'0.00'),IIf(Len(Int([Elevation]))=1,'  ' & Format$([Elevation],'0.00'),");
                    Export_Squery.Append("Format$([Elevation],'0.00'))) AS Elevacion, IIf(Len(Format$([SourceEventDate],'y')) = 1, '  ' & Format$([SourceEventDate],'y'), IIf(Len(Format$([SourceEventDate],'y')) = 2,");
                    Export_Squery.Append(" ' ' & Format$([SourceEventDate],'y'), Format$([SourceEventDate],'y'))) AS Dia, Format$([SourceEventTime],'HHnnss') AS Hora");
                    Export_Squery.Append(" FROM TReporte_Est ");
                    Export_Squery.Append("WHERE (((TReporte_Est.SourceEventDate)>=#" + FechaInicio + "# And (TReporte_Est.SourceEventDate)<=#" + FechaFinal + "#)) ORDER BY TReporte_Est.RecordNr;");

                    DataTable ExportSPS_SDatatable = conection.GetDataTable(Export_Squery);

                    //var engine = new FileHelperEngine<ArchivoS>();
                    //List< ArchivoS > ExportSPS_S = conection.GetGenericCollectionData<ArchivoS>(sql).ToList();
                    //var engine = new FixedFileEngine<ArchivoS>();

                    //engine.WriteFile(@"C:\Temp\output.s", ExportSPS_S);
                    //string fileName = @"C:\Temp\output.txt";
                    //StreamWriter writer = new StreamWriter(fileName);


                    //SQL que crea la datatable  para generar el Archivo X Todos los tipos

                    Export_Xquery.Append("SELECT 'X' AS Inicial, TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, 11 AS BaseLineIndex, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex,");
                    Export_Xquery.Append(" IIf(Len([Chann1])=1,'    ' & [Chann1],IIf(Len([Chann1])=2,'   ' & [Chann1],IIf(Len([Chann1])=3,'  ' & [Chann1], IIf(Len([Chann1])=4,' ' & [Chann1],[Chann1])))) AS FstCanal,");
                    Export_Xquery.Append(" IIf(Len([ChannU])=1,'    ' & [ChannU],IIf(Len([ChannU])=2,'   ' & [ChannU],IIf(Len([ChannU])=3,'  ' & [ChannU],IIf(Len([ChannU])=4,' ' & [ChannU], [ChannU])))) AS LastCanal,");
                    Export_Xquery.Append(" 1 AS LineIndex, [Spread-VT].RecLine As RecepLine, [Spread-VT].[1aSTN] As PSTN, [Spread-VT].LsSTN As USTN, [Spread-VT].IndexSTN AS VersionIdx ");
                    Export_Xquery.Append("FROM TReporte_Est INNER JOIN [Spread-VT] ON (TReporte_Est.ActPointNr = [Spread-VT].ActPointNr) AND (TReporte_Est.ActLineNr = [Spread-VT].ActLineNr)");
                    Export_Xquery.Append(" WHERE (((TReporte_Est.SourceEventDate) >= #" + FechaInicio + "# And (TReporte_Est.SourceEventDate) <= #" + FechaFinal + "#))");
                    Export_Xquery.Append(" ORDER BY TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, [Spread-VT].Chann1");
                    DataTable ExportSPS_XDatatable = conection.GetDataTable(Export_Xquery);
                    //MessageBox.Show("correcto");
                }

                else
                {
                    //SQL que crea la datatable  para generar el Archivo S  E1 y V1

                    Export_Squery.Append("SELECT 'S' AS Inicial, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex, TReporte_Est.Tipo As Fuente, TReporte_Est.SourceDepth,");
                    Export_Squery.Append(" IIf(Len([SourceUpholeTime])=1,' ' & [SourceUpholeTime],[SourceUpholeTime]) AS Uphole, Format$([WaterDepth], '0.0') AS WaterDep, Format$([Easting],'#.00') AS Este,");
                    Export_Squery.Append(" Format$([Northing],'#.00') AS Norte, IIf(Len(Int([Elevation]))=2,' ' & Format$([Elevation],'0.00'),IIf(Len(Int([Elevation]))=1,'  ' & Format$([Elevation],'0.00'),");
                    Export_Squery.Append("Format$([Elevation],'0.00'))) AS Elevacion, IIf(Len(Format$([SourceEventDate],'y')) = 1, '  ' & Format$([SourceEventDate],'y'), IIf(Len(Format$([SourceEventDate],'y')) = 2,");
                    Export_Squery.Append(" ' ' & Format$([SourceEventDate],'y'), Format$([SourceEventDate],'y'))) AS Dia, Format$([SourceEventTime],'HHnnss') AS Hora ");
                    Export_Squery.Append("FROM TReporte_Est ");
                    Export_Squery.Append("WHERE (((TReporte_Est.SourceEventDate)>=#" + FechaInicio + "# And (TReporte_Est.SourceEventDate)<=#" + FechaFinal + "#) And TReporte_Est.Tipo='" + cmbTipo.Text + "') ");
                    Export_Squery.Append("ORDER BY TReporte_Est.RecordNr;");

                    DataTable ExportSPS_SDatabla = conection.GetDataTable(Export_Squery);

                    //SQL que crea la datatable  para generar el Archivo X e1 y v1

                    Export_Xquery.Append("SELECT 'X' AS Inicial, TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, 11 AS BaseLineIndex, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex,");
                    Export_Xquery.Append(" IIf(Len([Chann1])=1,'    ' & [Chann1],IIf(Len([Chann1])=2,'   ' & [Chann1],IIf(Len([Chann1])=3,'  ' & [Chann1], IIf(Len([Chann1])=4,' ' & [Chann1],[Chann1])))) AS FstCanal,");
                    Export_Xquery.Append(" IIf(Len([ChannU])=1,'    ' & [ChannU],IIf(Len([ChannU])=2,'   ' & [ChannU],IIf(Len([ChannU])=3,'  ' & [ChannU],IIf(Len([ChannU])=4,' ' & [ChannU], [ChannU])))) AS LastCanal,");
                    Export_Xquery.Append(" 1 AS LineIndex, [Spread-VT].RecLine As RecepLine, [Spread-VT].[1aSTN] As PSTN, [Spread-VT].LsSTN As USTN, [Spread-VT].IndexSTN AS VersionIdx ");
                    Export_Xquery.Append("FROM TReporte_Est ");
                    Export_Xquery.Append("INNER JOIN [Spread-VT] ON (TReporte_Est.ActPointNr = [Spread-VT].ActPointNr) AND (TReporte_Est.ActLineNr = [Spread-VT].ActLineNr) ");
                    Export_Xquery.Append("WHERE (((TReporte_Est.SourceEventDate) >= #" + FechaInicio + "# And (TReporte_Est.SourceEventDate) <= #" + FechaFinal + "#) And TReporte_Est.Tipo='" + cmbTipo.Text + "') ");
                    Export_Xquery.Append("ORDER BY TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, [Spread-VT].Chann1");
                    DataTable ExportSPS_XDatatable = conection.GetDataTable(Export_Xquery);


                }

                //SQL que crea el datatable STNsSPSX
                STNsSPSX_query.Append("SELECT RecepLine, PSTN, USTN, VersionIdx ");
                STNsSPSX_query.AppendFormat(" FROM ({0}) ", Export_Xquery);
                STNsSPSX_query.Append(" GROUP BY RecepLine, PSTN, USTN, VersionIdx");
                DataTable STNsSPSX_Datatable = conection.GetDataTable(STNsSPSX_query);

                //SQL que crea el datatable LinkR
                LinkR_query.Append("SELECT LReceptoras.BaseLineNr, LReceptoras.BasePointNr, LReceptoras.IndexSTN");
                LinkR_query.Append(" FROM LReceptoras  WHERE ((LReceptoras.BaseLineNr) Is Null)");
                DataTable LinkR_Datatable = conection.GetDataTable(LinkR_query);


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

                /*sql7.Append("SELECT 'R' AS Inicial, LinkR.BaseLineNr, LinkR.BasePointNr, LinkR.IndexSTN AS RecLineIndex, IIf(LReceptoras.Tipo is not null, LReceptoras.Tipo, [Preplot-R].Tipo) As Detector,");
                sql7.Append(" IIf([LReceptoras].[Easting] Is Not Null,Format$([LReceptoras].[Easting],'#.00'),Format$([Preplot-R].[Easting],'#.00')) AS Este, IIf([LReceptoras].[Northing] Is Not Null,");
                sql7.Append("Format$([LReceptoras].[Northing],'#.00'),Format$([Preplot-R].[Northing],'#.00')) AS Norte, IIf(Int([Elevation])=0,'  ' & Format$([Elevation],'0.0'),IIf(Int([Elevation])=-1,");
                sql7.Append("' ' & Format$([Elevation],'0.0'),IIf(Len(Int([Elevation]))=2,' ' & Format$([Elevation],'#.00'),IIf(Len(Int([Elevation]))=1,'  ' & Format$([Elevation],'#.00'),");
                sql7.Append("Format$([Elevation],'#.00'))))) AS Elevacion, 1000001 AS BoxVersion ");
                sql7.Append("FROM (LinkR LEFT JOIN LReceptoras ON (LinkR.IndexSTN = LReceptoras.IndexSTN) AND (LinkR.BaseLineNr = LReceptoras.BaseLineNr) AND (LinkR.BasePointNr = LReceptoras.BasePointNr)) ");
                sql7.Append("INNER JOIN [Preplot-R] ON (LinkR.BaseLineNr = [Preplot-R].BaseLineNr) AND (LinkR.BasePointNr = [Preplot-R].BasePointNr) ");
                sql7.Append("Group BY 'R', LinkR.BaseLineNr, LinkR.BasePointNr, LinkR.IndexSTN, ");
                sql7.Append("IIf(LReceptoras.Tipo is not null, LReceptoras.Tipo, [Preplot-R].Tipo),");
                sql7.Append(" LReceptoras.BaseLineNr, LReceptoras.BasePointNr, LReceptoras.IndexSTN, LReceptoras.Easting, LReceptoras.Northing, LReceptoras.Elevation, [Preplot-R].Easting, [Preplot-R].Northing ");
                sql7.Append("ORDER BY LinkR.BaseLineNr, LinkR.BasePointNr;");
                DataTable ExportSPS_R = conection.GetDataTable(sql7);*/

                Export_Rquery.Append("SELECT 'R' AS Inicial, LinkR.BaseLineNr, LinkR.BasePointNr, LinkR.IndexSTN AS RecLineIndex, IIf(LReceptoras.Tipo is not null, LReceptoras.Tipo, [Preplot-R].Tipo) As Detector,");
                Export_Rquery.Append(" IIf([LReceptoras].[Easting] Is Not Null,Format$([LReceptoras].[Easting],'#.00'),Format$([Preplot-R].[Easting],'#.00')) AS Este, IIf([LReceptoras].[Northing] Is Not Null,");
                Export_Rquery.Append("Format$([LReceptoras].[Northing],'#.00'),Format$([Preplot-R].[Northing],'#.00')) AS Norte, IIf(Int([Elevation])=0,'  ' & Format$([Elevation],'0.0'),IIf(Int([Elevation])=-1,");
                Export_Rquery.Append("' ' & Format$([Elevation],'0.0'),IIf(Len(Int([Elevation]))=2,' ' & Format$([Elevation],'#.00'),IIf(Len(Int([Elevation]))=1,'  ' & Format$([Elevation],'#.00'),");
                Export_Rquery.AppendFormat(" FROM ({0}) as LinkR ", LinkR_query);
                Export_Rquery.Append("LEFT JOIN LReceptoras ON (LinkR.IndexSTN = LReceptoras.IndexSTN) AND (LinkR.BaseLineNr = LReceptoras.BaseLineNr) AND (LinkR.BasePointNr = LReceptoras.BasePointNr)) ");
                Export_Rquery.Append("INNER JOIN [Preplot-R] ON (LinkR.BaseLineNr = [Preplot-R].BaseLineNr) AND (LinkR.BasePointNr = [Preplot-R].BasePointNr) ");
                Export_Rquery.Append("Group BY 'R', LinkR.BaseLineNr, LinkR.BasePointNr, LinkR.IndexSTN, ");
                Export_Rquery.Append("IIf(LReceptoras.Tipo is not null, LReceptoras.Tipo, [Preplot-R].Tipo),");
                Export_Rquery.Append(" LReceptoras.BaseLineNr, LReceptoras.BasePointNr, LReceptoras.IndexSTN, LReceptoras.Easting, LReceptoras.Northing, LReceptoras.Elevation, [Preplot-R].Easting, [Preplot-R].Northing ");
                Export_Rquery.Append("ORDER BY LinkR.BaseLineNr, LinkR.BasePointNr;");
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
