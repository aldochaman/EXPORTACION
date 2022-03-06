
namespace Exportacion
{
    partial class Form_Exportacion
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.txt_ruta = new System.Windows.Forms.TextBox();
            this.Carpeta = new System.Windows.Forms.PictureBox();
            this.label1 = new System.Windows.Forms.Label();
            this.cmbTipo = new System.Windows.Forms.ComboBox();
            this.btn_exportar = new System.Windows.Forms.Button();
            this.label4 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.dtp_FechaInicio = new System.Windows.Forms.DateTimePicker();
            this.dtp_FechaFinal = new System.Windows.Forms.DateTimePicker();
            ((System.ComponentModel.ISupportInitialize)(this.Carpeta)).BeginInit();
            this.SuspendLayout();
            // 
            // txt_ruta
            // 
            this.txt_ruta.Font = new System.Drawing.Font("Microsoft Sans Serif", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.txt_ruta.Location = new System.Drawing.Point(100, 321);
            this.txt_ruta.Margin = new System.Windows.Forms.Padding(2);
            this.txt_ruta.Name = "txt_ruta";
            this.txt_ruta.Size = new System.Drawing.Size(658, 30);
            this.txt_ruta.TabIndex = 58;
            this.txt_ruta.Text = " ";
            // 
            // Carpeta
            // 
            this.Carpeta.Image = global::Exportacion.Properties.Resources.Folder;
            this.Carpeta.Location = new System.Drawing.Point(43, 299);
            this.Carpeta.Margin = new System.Windows.Forms.Padding(2);
            this.Carpeta.Name = "Carpeta";
            this.Carpeta.Size = new System.Drawing.Size(53, 76);
            this.Carpeta.SizeMode = System.Windows.Forms.PictureBoxSizeMode.Zoom;
            this.Carpeta.TabIndex = 57;
            this.Carpeta.TabStop = false;
            this.Carpeta.Click += new System.EventHandler(this.Carpeta_Click);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Microsoft Sans Serif", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.ForeColor = System.Drawing.SystemColors.HotTrack;
            this.label1.Location = new System.Drawing.Point(199, 229);
            this.label1.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(57, 25);
            this.label1.TabIndex = 56;
            this.label1.Text = "Tipo:";
            // 
            // cmbTipo
            // 
            this.cmbTipo.Font = new System.Drawing.Font("Microsoft Sans Serif", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.cmbTipo.FormattingEnabled = true;
            this.cmbTipo.ItemHeight = 25;
            this.cmbTipo.Items.AddRange(new object[] {
            "Todos Los Tipos",
            "E1",
            "V1"});
            this.cmbTipo.Location = new System.Drawing.Point(283, 221);
            this.cmbTipo.Name = "cmbTipo";
            this.cmbTipo.Size = new System.Drawing.Size(186, 33);
            this.cmbTipo.TabIndex = 55;
            // 
            // btn_exportar
            // 
            this.btn_exportar.BackColor = System.Drawing.SystemColors.ButtonFace;
            this.btn_exportar.Font = new System.Drawing.Font("Microsoft Sans Serif", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btn_exportar.Location = new System.Drawing.Point(331, 386);
            this.btn_exportar.Margin = new System.Windows.Forms.Padding(2);
            this.btn_exportar.Name = "btn_exportar";
            this.btn_exportar.Size = new System.Drawing.Size(138, 47);
            this.btn_exportar.TabIndex = 54;
            this.btn_exportar.Text = "Exportar";
            this.btn_exportar.UseVisualStyleBackColor = false;
            this.btn_exportar.Click += new System.EventHandler(this.btn_exportar_Click);
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Font = new System.Drawing.Font("Microsoft Sans Serif", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label4.ForeColor = System.Drawing.SystemColors.HotTrack;
            this.label4.Location = new System.Drawing.Point(164, 164);
            this.label4.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(120, 25);
            this.label4.TabIndex = 53;
            this.label4.Text = "Fecha Final:";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Font = new System.Drawing.Font("Microsoft Sans Serif", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label3.ForeColor = System.Drawing.SystemColors.HotTrack;
            this.label3.Location = new System.Drawing.Point(164, 101);
            this.label3.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(123, 25);
            this.label3.TabIndex = 52;
            this.label3.Text = "Fecha Inicio:";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Font = new System.Drawing.Font("Microsoft Sans Serif", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label2.ForeColor = System.Drawing.SystemColors.HotTrack;
            this.label2.Location = new System.Drawing.Point(126, 18);
            this.label2.Margin = new System.Windows.Forms.Padding(2, 0, 2, 0);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(300, 25);
            this.label2.TabIndex = 49;
            this.label2.Text = "EXPORTACION DE ARCHIVOS";
            // 
            // dtp_FechaInicio
            // 
            this.dtp_FechaInicio.Font = new System.Drawing.Font("Microsoft Sans Serif", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.dtp_FechaInicio.Format = System.Windows.Forms.DateTimePickerFormat.Short;
            this.dtp_FechaInicio.Location = new System.Drawing.Point(283, 96);
            this.dtp_FechaInicio.Name = "dtp_FechaInicio";
            this.dtp_FechaInicio.Size = new System.Drawing.Size(186, 30);
            this.dtp_FechaInicio.TabIndex = 59;
            // 
            // dtp_FechaFinal
            // 
            this.dtp_FechaFinal.Font = new System.Drawing.Font("Microsoft Sans Serif", 15F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.dtp_FechaFinal.Format = System.Windows.Forms.DateTimePickerFormat.Short;
            this.dtp_FechaFinal.Location = new System.Drawing.Point(283, 159);
            this.dtp_FechaFinal.Name = "dtp_FechaFinal";
            this.dtp_FechaFinal.Size = new System.Drawing.Size(186, 30);
            this.dtp_FechaFinal.TabIndex = 60;
            // 
            // Form_Exportacion
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.dtp_FechaFinal);
            this.Controls.Add(this.dtp_FechaInicio);
            this.Controls.Add(this.txt_ruta);
            this.Controls.Add(this.Carpeta);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.cmbTipo);
            this.Controls.Add(this.btn_exportar);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.label2);
            this.Name = "Form_Exportacion";
            this.Text = "Form_Exportacion";
            this.Load += new System.EventHandler(this.Form_Exportacion_Load);
            ((System.ComponentModel.ISupportInitialize)(this.Carpeta)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.TextBox txt_ruta;
        private System.Windows.Forms.PictureBox Carpeta;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.ComboBox cmbTipo;
        private System.Windows.Forms.Button btn_exportar;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.DateTimePicker dtp_FechaInicio;
        private System.Windows.Forms.DateTimePicker dtp_FechaFinal;
    }
}