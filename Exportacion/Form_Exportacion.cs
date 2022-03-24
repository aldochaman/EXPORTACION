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
using System.Diagnostics;

namespace Exportacion
{
     public partial class Form_Exportacion : Form
     {
          private Stopwatch _oTiempo;
          private BackgroundWorker _oWorker; 


          public Form_Exportacion()
          {
               InitializeComponent();
               CheckForIllegalCrossThreadCalls = false;
               _oTiempo = new Stopwatch();
          }

          private void btn_exportar_Click(object sender, EventArgs e)
          {
               try
               {
                    InicializaHilo();
               }
               catch (Exception ex)
               {
                    ManejaExcepcion(ex);

               }
          }
          public void Form_Exportacion_Load(object sender, EventArgs e)
          {
               try
               {
                    CultureInfo.CreateSpecificCulture("en-US");
                    cmbTipo.Text = "Todos Los Tipos";
               }
               catch (Exception ex)
               {
                    ManejaExcepcion(ex);
                    
               }
          }
          public void ManejaExcepcion(Exception poExcepcion)
          {               
               MessageBox.Show(string.Format("Ocurrió lo siguiente :\r\n{0}", poExcepcion.Message), "Mensaje del Sistema");
          }
          private void Carpeta_Click(object sender, EventArgs e)
          {
               try
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
                         txt_ruta.Text = string.Format("{0}\\SPS {1}", Carpeta.SelectedPath, "Archivo");
                    }
               }
               catch (Exception ex)
               {
                    ManejaExcepcion(ex);

               }
          }

          public void pictureBox1_Click(object sender, EventArgs e)
          {
               
          }

          private void tmrTiempo_Tick(object sender, EventArgs e)
          {
               if (_oTiempo != null && _oTiempo.IsRunning)
               {

                    TimeSpan ts = _oTiempo.Elapsed;
                    lblTiempo.Text = String.Format("{0:00}:{1:00}:{2:00}:{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
               }
          }
          private void HabilitaControles(bool pbHabilita)
          {
               foreach (Control loControl in this.Controls)
               {
                    if (loControl is System.Windows.Forms.Button)
                         ((System.Windows.Forms.Button)loControl).Enabled = pbHabilita;
                    if (loControl is System.Windows.Forms.RadioButton)
                         ((System.Windows.Forms.RadioButton)loControl).Enabled = pbHabilita;
                    if (loControl is System.Windows.Forms.ComboBox)
                         ((System.Windows.Forms.ComboBox)loControl).Enabled = pbHabilita;
                    if (loControl is System.Windows.Forms.GroupBox)
                    {
                         foreach (Control loControl1 in ((System.Windows.Forms.GroupBox)loControl).Controls)
                         {
                              if (loControl1 is System.Windows.Forms.Button)
                                   ((System.Windows.Forms.Button)loControl1).Enabled = pbHabilita;
                              if (loControl1 is System.Windows.Forms.RadioButton)
                                   ((System.Windows.Forms.RadioButton)loControl1).Enabled = pbHabilita;
                              if (loControl1 is System.Windows.Forms.ComboBox)
                                   ((System.Windows.Forms.ComboBox)loControl1).Enabled = pbHabilita;
                         }
                    }
               }               
          }

          private void InicializaHilo()
          {
               IniciaTiempo();
               HabilitaControles(false);
               prbProceso.Minimum = 0;
               prbProceso.Maximum = 100;
               if (_oWorker == null)
               {
                    _oWorker = new BackgroundWorker();
                    _oWorker.DoWork += worker_DoWork;
                    _oWorker.RunWorkerCompleted += worker_RunWorkerCompleted;
                    _oWorker.ProgressChanged += worker_ProgressChanged;
                    _oWorker.WorkerReportsProgress = true;
                    _oWorker.WorkerSupportsCancellation = true;
               }
               if (_oWorker.IsBusy != true)
               {
                    // Start the asynchronous operation.
                    _oWorker.RunWorkerAsync();
               }
          }

          private void IniciaTiempo()
          {
               if (_oTiempo.IsRunning)
               {
                    _oTiempo.Stop();
                    lblTiempo.Text = "00:00:00:00";
               }
               else
               {
                    lblTiempo.Text = "00:00:00:00";
                    _oTiempo.Reset();
                    _oTiempo.Start();

               }
          }

          void worker_ProgressChanged(object sender, ProgressChangedEventArgs e)
          {
               int liBarraPorcentaje;
               prbProceso.Value = e.ProgressPercentage;
               liBarraPorcentaje = Convert.ToInt32(Math.Floor(100.00 * e.ProgressPercentage / 100));
               lblProgreso.Text = "Processing......" + liBarraPorcentaje.ToString() + "%";
          }

          private void worker_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
          {
               lblProgreso.Text = "Task finished";
               HabilitaControles(true);
               _oTiempo.Stop();
          }

          private void worker_DoWork(object sender, DoWorkEventArgs e)
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
                    lblProgreso.Text = "Start";
                    if (cmbTipo.Text == "Todos Los Tipos")
                    {
                         //SQL que crea la datatable  para generar el Archivo S  Todos los tipos
                         Export_Squery.AppendFormat("SELECT 'S' AS Inicial, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex, TReporte_Est.Tipo As Fuente, TReporte_Est.SourceDepth, ");
                         Export_Squery.AppendFormat("IIf(Len([SourceUpholeTime])=1,' ' & [SourceUpholeTime],[SourceUpholeTime]) AS Uphole, Format$([WaterDepth], '0.0') AS WaterDep, Format$([Easting],'#.00') AS Este,");
                         Export_Squery.AppendFormat(" Format$([Northing],'#.00') AS Norte, IIf(Len(Int([Elevation]))=2,' ' & Format$([Elevation],'0.00'),IIf(Len(Int([Elevation]))=1,'  ' & Format$([Elevation],'0.00'),");
                         Export_Squery.AppendFormat("Format$([Elevation],'0.00'))) AS Elevacion, IIf(Len(Format$([SourceEventDate],'y')) = 1, '  ' & Format$([SourceEventDate],'y'), IIf(Len(Format$([SourceEventDate],'y')) = 2,");
                         Export_Squery.AppendFormat(" ' ' & Format$([SourceEventDate],'y'), Format$([SourceEventDate],'y'))) AS Dia, Format$([SourceEventTime],'HHnnss') AS Hora");
                         Export_Squery.AppendFormat(" FROM TReporte_Est ");
                         Export_Squery.AppendFormat("WHERE (((TReporte_Est.SourceEventDate)>={0} And (TReporte_Est.SourceEventDate)<={1})) ORDER BY TReporte_Est.RecordNr;", FechaInicio.DateSql(false, DatabaseEngines.OleDb), FechaFinal.DateSql(false, DatabaseEngines.OleDb));


                         //SQL que crea la datatable  para generar el Archivo X Todos los tipos

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
                    _oWorker.ReportProgress(10);
                    lblProgreso.Text = "Get Datatable S and X";
                    DataTable ExportSPS_SDatatable = conection.GetDataTable(Export_Squery);
                    DataTable ExportSPS_XDatatable = conection.GetDataTable(Export_Xquery);
                    lblProgreso.Text = "Get DataTable S and X created";
                    //SQL que crea el datatable STNsSPSX
                    lblProgreso.Text = "Get Line and Point from X....";
                    STNsSPSX_query.Append("SELECT RecepLine, PSTN, USTN, VersionIdx ");
                    STNsSPSX_query.AppendFormat(" FROM ({0}) ", Export_Xquery);
                    STNsSPSX_query.Append(" GROUP BY RecepLine, PSTN, USTN, VersionIdx");
                    DataTable STNsSPSX_Datatable = conection.GetDataTable(STNsSPSX_query);
                    lblProgreso.Text = "Get Line and Point from X.... Finished";

                    //SQL que crea el datatable LinkR

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
                    _oWorker.ReportProgress(20);
                    lblProgreso.Text = "Get Link_R.... Finished";
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
                    lblProgreso.Text = "Fill DataTable  LINKR";
                    List<LinkR> linkr = new List<LinkR>();
                    // foreach (DataRow rst in STNsSPSX_Datatable.Rows)
                    System.Threading.Tasks.Parallel.ForEach(STNsSPSX_Datatable.AsEnumerable(), rst =>
                    {
                         for (int estacion = rst.GetValue<int>("PSTN"); estacion <= rst.GetValue<int>("USTN"); estacion++)
                         {
                              //StringBuilder query = new StringBuilder();
                              //query.AppendFormat("Insert into LinkR (BaseLineNr, BasePointNr, IndexSTN) ");
                              //query.AppendFormat("values ({0},{1},{2})", rst.GetValue<string>("RecepLine"), estacion, rst.GetValue<string>("VersionIdx"));
                              //conection.ExecuteCommand(query);
                              linkr.Add(new LinkR() { BaseLineNr = rst.GetValue<string>("RecepLine"), BasePointNr = estacion, IndexSTN= rst.GetValue<string>("VersionIdx") });
                         }
                    });
                    conection.InsertBulkCopy<LinkR>(linkr, "LinkR");
                    lblProgreso.Text = "GET DataTable  R...";
                    _oWorker.ReportProgress(60);
                    //SQL que crea la TABLA ExportSPS_R en base a la Tabla Exportada ExportSPS_X

                    Export_Rquery.AppendFormat("SELECT 'R' AS Inicial, LinkR.BaseLineNr, LinkR.BasePointNr, LinkR.IndexSTN AS RecLineIndex, IIf(LReceptoras.Tipo is not null, LReceptoras.Tipo, [Preplot-R].Tipo) As Detector,");
                    Export_Rquery.AppendFormat(" IIf([LReceptoras].[Easting] Is Not Null,Format$([LReceptoras].[Easting],'#.00'),Format$([Preplot-R].[Easting],'#.00')) AS Este, IIf([LReceptoras].[Northing] Is Not Null,");
                    Export_Rquery.AppendFormat("Format$([LReceptoras].[Northing],'#.00'),Format$([Preplot-R].[Northing],'#.00')) AS Norte, IIf(Int([Elevation])=0,'  ' & Format$([Elevation],'0.0'),IIf(Int([Elevation])=-1,");
                    Export_Rquery.AppendFormat("' ' & Format$([Elevation],'0.0'),IIf(Len(Int([Elevation]))=2,' ' & Format$([Elevation],'#.00'),IIf(Len(Int([Elevation]))=1,'  ' & Format$([Elevation],'#.00'),Format$([Elevation],'#.00')))))");
                    Export_Rquery.AppendFormat(" AS Elevacion, 1000001 AS BoxVersion  ");
                    Export_Rquery.AppendFormat("FROM  (LinkR ");
                    Export_Rquery.AppendFormat("LEFT JOIN LReceptoras ON (LinkR.IndexSTN = LReceptoras.IndexSTN) AND (LinkR.BaseLineNr = LReceptoras.BaseLineNr) AND (LinkR.BasePointNr = LReceptoras.BasePointNr))");
                    Export_Rquery.AppendFormat(" INNER JOIN [Preplot-R] ON (LinkR.BaseLineNr = [Preplot-R].BaseLineNr) AND (LinkR.BasePointNr = [Preplot-R].BasePointNr) ");
                    Export_Rquery.AppendFormat("Group BY 'R', LinkR.BaseLineNr, LinkR.BasePointNr, LinkR.IndexSTN,");
                    Export_Rquery.AppendFormat(" IIf(LReceptoras.Tipo is not null, LReceptoras.Tipo, [Preplot-R].Tipo), LReceptoras.BaseLineNr, LReceptoras.BasePointNr, LReceptoras.IndexSTN, LReceptoras.Easting, LReceptoras.Northing,");
                    Export_Rquery.AppendFormat(" LReceptoras.Elevation, [Preplot-R].Easting, [Preplot-R].Northing ");
                    Export_Rquery.AppendFormat("ORDER BY LinkR.BaseLineNr, LinkR.BasePointNr;");
                    DataTable ExportSPS_RDatatable = conection.GetDataTable(Export_Rquery);
                    _oWorker.ReportProgress(70);
                    lblProgreso.Text = "GET DataTable  R... Finished  and start Exportation";
                    //EXPORTACION DE ARCHIVOS
                    string rutaArchivoS = string.Format("{0}{1} ", txt_ruta.Text, ".S");
                    string rutaArchivoR = string.Format("{0}{1} ", txt_ruta.Text, ".R");
                    string rutaArchivoX = string.Format("{0}{1} ", txt_ruta.Text, ".X");

                    _oWorker.ReportProgress(80);
                    ExportSPS_SDatatable.ToCSV(rutaArchivoS);
                    lblProgreso.Text = "Archivos S creado con exito";
                    //MessageBox.Show("Archivos S creado con exito");
                    _oWorker.ReportProgress(90);
                    ExportSPS_XDatatable.ToCSV(rutaArchivoX);
                    lblProgreso.Text = "Archivos X creado con exito";
                    //MessageBox.Show("Archivos X creado con exito");
                    ExportSPS_RDatatable.ToCSV(rutaArchivoR);
                    lblProgreso.Text = "Archivos R creado con exito";
                    _oWorker.ReportProgress(100);
                    //MessageBox.Show("Archivos creados con exito");
               }

          }

     }
}
