using Framework.DataBase;
using Framework.Enumerations;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
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
            string FechaInicio = dtp_FechaInicio.Text;
            string FechaFinal = dtp_FechaFinal.Text;


            int FechaInicioLength = FechaInicio.Length;
            int FechaFinLength = FechaFinal.Length;

            if (!(FechaInicioLength > 0) | !(FechaFinLength > 0))
                MessageBox.Show("Es necesario que introduzca una Fecha inicio y una Fecha final para la exportación del archivo");
            else
            {
                DataAccess conection = new DataAccess(@"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=C:\3D IXACHI.mdb", "", DatabaseEngines.OleDb);
                StringBuilder sql = new StringBuilder();
                sql.Append(" select * from TReporte_Est");
                //sql.Append("SELECT 'X' AS Inicial, TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, 11 AS BaseLineIndex, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, 1 AS BasePointIndex, IIf(Len([Chann1])=1,'    ' & [Chann1],IIf(Len([Chann1])=2,'   ' & [Chann1],IIf(Len([Chann1])=3,'  ' & [Chann1], IIf(Len([Chann1])=4,' ' & [Chann1],[Chann1])))) AS FstCanal, IIf(Len([ChannU])=1,'    ' & [ChannU],IIf(Len([ChannU])=2,'   ' & [ChannU],IIf(Len([ChannU])=3,'  ' & [ChannU],IIf(Len([ChannU])=4,' ' & [ChannU], [ChannU])))) AS LastCanal, 1 AS LineIndex, [Spread-VT].RecLine As RecepLine, [Spread-VT].[1aSTN] As PSTN, [Spread-VT].LsSTN As USTN, [Spread-VT].IndexSTN AS VersionIdx FROM TReporte_Est INNER JOIN [Spread-VT] ON (TReporte_Est.ActPointNr = [Spread-VT].ActPointNr) AND (TReporte_Est.ActLineNr = [Spread-VT].ActLineNr) WHERE (((TReporte_Est.SourceEventDate) >= #07/19/2021# And (TReporte_Est.SourceEventDate) <= #07/19/2021#)) ORDER BY TReporte_Est.FieldTapeNr, TReporte_Est.RecordNr, TReporte_Est.BaseLineNr, TReporte_Est.BasePointNr, [Spread - VT].Chann1; ");
                DataTable data = conection.GetDataTable(sql);
            }
        }
        private void Form_Exportacion_Load(object sender, EventArgs e)
        {
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

