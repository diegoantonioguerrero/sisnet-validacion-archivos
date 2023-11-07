using SisnetData;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Windows.Forms;

namespace SisnetExportador
{
    public partial class FrmDataExtractor : Form
    {
        private string fileContent = string.Empty;

        private string valueConsecutivo;

        private string valueArchivoName;

        private string valueArchivo;

        private string valueTable;

        private DataTable dataToExport;

        private Label label1;

        private ComboBox cmbTables;

        private ComboBox cmbConsecutivo;

        private Label label2;

        private ComboBox cmbArchivo;

        private Label label3;

        private Label label4;

        private OpenFileDialog openFileDialogCSV;

        private TextBox textBox1;

        private Button btnOpenFile;

        private DataGridView dataGridView1;

        private ComboBox cmbArchivoName;

        private Label label5;

        private Button button1;

        private Button btnExportar;

        private FolderBrowserDialog folderBrowserExport;

        private PictureBox pictureBox1;

        private Label lblExport;

        private ProgressBar pgb;

        private BackgroundWorker bgw;

        public FrmDataExtractor()
        {
            InitializeComponent();
            this.InitializeComponentTotal();
            this.bgw.DoWork += new DoWorkEventHandler(this.bgw_DoWork);
            this.dataToExport = new DataTable();
            this.dataToExport.Columns.Add(new DataColumn("Consecutivo", typeof(string)));
            this.dataToExport.Columns.Add(new DataColumn("NombreOriginal", typeof(string)));
            this.dataToExport.Columns.Add(new DataColumn("NombreExportar", typeof(string)));
            this.dataToExport.Columns.Add(new DataColumn("NombreExportado", typeof(string)));
            this.dataToExport.Columns.Add(new DataColumn("Tamaño", typeof(string)));
            this.dataToExport.Columns.Add(new DataColumn("Estado", typeof(string)));
            this.dataGridView1.DataSource = this.dataToExport;
        }

        private void InitializeComponentTotal()
        {
            ComponentResourceManager componentResourceManager = new ComponentResourceManager(typeof(FrmDataExtractor));
            this.label1 = new Label();
            this.cmbTables = new ComboBox();
            this.cmbConsecutivo = new ComboBox();
            this.label2 = new Label();
            this.cmbArchivo = new ComboBox();
            this.label3 = new Label();
            this.label4 = new Label();
            this.openFileDialogCSV = new OpenFileDialog();
            this.textBox1 = new TextBox();
            this.btnOpenFile = new Button();
            this.dataGridView1 = new DataGridView();
            this.cmbArchivoName = new ComboBox();
            this.label5 = new Label();
            this.button1 = new Button();
            this.btnExportar = new Button();
            this.folderBrowserExport = new FolderBrowserDialog();
            this.pictureBox1 = new PictureBox();
            this.lblExport = new Label();
            this.pgb = new ProgressBar();
            this.bgw = new BackgroundWorker();
            ((ISupportInitialize)this.dataGridView1).BeginInit();
            ((ISupportInitialize)this.pictureBox1).BeginInit();
            base.SuspendLayout();
            this.label1.AutoSize = true;
            this.label1.Location = new Point(13, 13);
            this.label1.Name = "label1";
            this.label1.Size = new Size(78, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "Tabla de datos";
            this.cmbTables.FormattingEnabled = true;
            this.cmbTables.Location = new Point(134, 13);
            this.cmbTables.Name = "cmbTables";
            this.cmbTables.Size = new Size(193, 21);
            this.cmbTables.TabIndex = 1;
            this.cmbTables.SelectedIndexChanged += new EventHandler(this.cmbTables_SelectedIndexChanged);
            this.cmbConsecutivo.Enabled = false;
            this.cmbConsecutivo.FormattingEnabled = true;
            this.cmbConsecutivo.Location = new Point(134, 40);
            this.cmbConsecutivo.Name = "cmbConsecutivo";
            this.cmbConsecutivo.Size = new Size(193, 21);
            this.cmbConsecutivo.TabIndex = 3;
            this.cmbConsecutivo.SelectedIndexChanged += new EventHandler(this.cmbField_SelectedIndexChanged);
            this.label2.AutoSize = true;
            this.label2.Location = new Point(13, 40);
            this.label2.Name = "label2";
            this.label2.Size = new Size(118, 13);
            this.label2.TabIndex = 2;
            this.label2.Text = "Campo del consecutivo";
            this.cmbArchivo.Enabled = false;
            this.cmbArchivo.FormattingEnabled = true;
            this.cmbArchivo.Location = new Point(134, 91);
            this.cmbArchivo.Name = "cmbArchivo";
            this.cmbArchivo.Size = new Size(193, 21);
            this.cmbArchivo.TabIndex = 5;
            this.cmbArchivo.SelectedIndexChanged += new EventHandler(this.cmbField_SelectedIndexChanged);
            this.label3.AutoSize = true;
            this.label3.Location = new Point(13, 91);
            this.label3.Name = "label3";
            this.label3.Size = new Size(95, 13);
            this.label3.TabIndex = 4;
            this.label3.Text = "Campo del binarioº";
            this.label4.AutoSize = true;
            this.label4.Location = new Point(13, 134);
            this.label4.Name = "label4";
            this.label4.Size = new Size(102, 13);
            this.label4.TabIndex = 6;
            this.label4.Text = "Archivo CSV mapeo";
            this.openFileDialogCSV.FileName = "openFileDialog1";
            this.textBox1.Location = new Point(134, 134);
            this.textBox1.Name = "textBox1";
            this.textBox1.Size = new Size(193, 20);
            this.textBox1.TabIndex = 7;
            this.btnOpenFile.Location = new Point(334, 130);
            this.btnOpenFile.Name = "btnOpenFile";
            this.btnOpenFile.Size = new Size(112, 23);
            this.btnOpenFile.TabIndex = 8;
            this.btnOpenFile.Text = "Cargar archivo";
            this.btnOpenFile.UseVisualStyleBackColor = true;
            this.btnOpenFile.Click += new EventHandler(this.btnOpenFile_Click);
            this.dataGridView1.ColumnHeadersHeightSizeMode = DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dataGridView1.Location = new Point(16, 169);
            this.dataGridView1.Name = "dataGridView1";
            this.dataGridView1.ReadOnly = true;
            this.dataGridView1.Size = new Size(753, 150);
            this.dataGridView1.TabIndex = 9;
            this.cmbArchivoName.Enabled = false;
            this.cmbArchivoName.FormattingEnabled = true;
            this.cmbArchivoName.Location = new Point(134, 67);
            this.cmbArchivoName.Name = "cmbArchivoName";
            this.cmbArchivoName.Size = new Size(193, 21);
            this.cmbArchivoName.TabIndex = 11;
            this.cmbArchivoName.SelectedIndexChanged += new EventHandler(this.cmbField_SelectedIndexChanged);
            this.label5.AutoSize = true;
            this.label5.Location = new Point(13, 67);
            this.label5.Name = "label5";
            this.label5.Size = new Size(116, 13);
            this.label5.TabIndex = 10;
            this.label5.Text = "Campo nombre archivo";
            this.button1.Location = new Point(452, 130);
            this.button1.Name = "button1";
            this.button1.Size = new Size(75, 23);
            this.button1.TabIndex = 12;
            this.button1.Text = "button1";
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Visible = false;
            this.button1.Click += new EventHandler(this.button1_Click);
            this.btnExportar.Location = new Point(657, 337);
            this.btnExportar.Name = "btnExportar";
            this.btnExportar.Size = new Size(112, 23);
            this.btnExportar.TabIndex = 15;
            this.btnExportar.Text = "Exportar datos";
            this.btnExportar.UseVisualStyleBackColor = true;
            this.btnExportar.Click += new EventHandler(this.btnExportar_Click);
            this.pictureBox1.Image = (Image)componentResourceManager.GetObject("pictureBox1.Image");
            this.pictureBox1.Location = new Point(334, 12);
            this.pictureBox1.Name = "pictureBox1";
            this.pictureBox1.Size = new Size(213, 100);
            this.pictureBox1.SizeMode = PictureBoxSizeMode.AutoSize;
            this.pictureBox1.TabIndex = 16;
            this.pictureBox1.TabStop = false;
            this.lblExport.AutoSize = true;
            this.lblExport.ForeColor = Color.Orange;
            this.lblExport.Location = new Point(409, 337);
            this.lblExport.Name = "lblExport";
            this.lblExport.Size = new Size(70, 13);
            this.lblExport.TabIndex = 17;
            this.lblExport.Text = "Exportando...";
            this.lblExport.Visible = false;
            this.pgb.Location = new Point(16, 337);
            this.pgb.Name = "pgb";
            this.pgb.Size = new Size(387, 23);
            this.pgb.Style = ProgressBarStyle.Continuous;
            this.pgb.TabIndex = 18;
            this.bgw.WorkerReportsProgress = true;
            this.bgw.WorkerSupportsCancellation = true;
            this.bgw.ProgressChanged += new ProgressChangedEventHandler(this.bgw_ProgressChanged);
            this.bgw.RunWorkerCompleted += new RunWorkerCompletedEventHandler(this.bgw_RunWorkerCompleted);
            base.AutoScaleDimensions = new SizeF(6f, 13f);
            base.AutoScaleMode = AutoScaleMode.Font;
            base.ClientSize = new Size(787, 372);
            base.Controls.Add(this.pgb);
            base.Controls.Add(this.lblExport);
            base.Controls.Add(this.pictureBox1);
            base.Controls.Add(this.btnExportar);
            base.Controls.Add(this.button1);
            base.Controls.Add(this.cmbArchivoName);
            base.Controls.Add(this.label5);
            base.Controls.Add(this.dataGridView1);
            base.Controls.Add(this.btnOpenFile);
            base.Controls.Add(this.textBox1);
            base.Controls.Add(this.label4);
            base.Controls.Add(this.cmbArchivo);
            base.Controls.Add(this.label3);
            base.Controls.Add(this.cmbConsecutivo);
            base.Controls.Add(this.label2);
            base.Controls.Add(this.cmbTables);
            base.Controls.Add(this.label1);
            base.Icon = (Icon)componentResourceManager.GetObject("$this.Icon");
            base.Name = "FrmDataExtractor";
            this.Text = "Extractor de archivos";
            base.Load += new EventHandler(this.FrmDataExtractor_Load);
            ((ISupportInitialize)this.dataGridView1).EndInit();
            ((ISupportInitialize)this.pictureBox1).EndInit();
            base.ResumeLayout(false);
            base.PerformLayout();
        }

        #region Methods
        private void bgw_DoWork(object sender, DoWorkEventArgs e)
        {
            this.ExportData(e);
            if (e.Cancel)
            {
                return;
            }
            this.bgw.ReportProgress(100);
        }


        private void bgw_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            this.pgb.Value = e.ProgressPercentage;
        }

        private void bgw_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            this.lblExport.Visible = false;
            this.btnExportar.Text = "Exportar datos";
            if (e.Cancelled)
            {
                MessageBox.Show("El proceso de exportación ha sido cancelado", "Sisnet", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                return;
            }
            if (e.Error != null)
            {
                String message = e.Error.Message;
                message += e.Error.InnerException != null ? "\r\n[" + e.Error.InnerException.Message + "]" : string.Empty;
                message += e.Error.InnerException != null ? "\r\n" + e.Error.InnerException.StackTrace + "\r\n\r\n" : string.Empty;
                message += "\r\n" + e.Error.StackTrace;
                MessageBox.Show("Error ejecutando la exporación.\r\n" + message, "Sisnet", MessageBoxButtons.OK, MessageBoxIcon.Hand);
                return;
            }
            foreach (DataGridViewRow row in (IEnumerable)this.dataGridView1.Rows)
            {
                if (row.Cells[5].Value != null && !Convert.ToString(row.Cells[5].Value).Contains("Ok"))
                {
                    row.DefaultCellStyle.BackColor = Color.Red;
                }
                if (row.Cells[5].Value == null || !Convert.ToString(row.Cells[5].Value).Equals("Ok"))
                {
                    if (row.Cells[5].Value == null || !Convert.ToString(row.Cells[5].Value).Contains("Renombrado"))
                    {
                        continue;
                    }
                    row.DefaultCellStyle.BackColor = Color.LightYellow;
                }
                else
                {
                    row.DefaultCellStyle.BackColor = Color.White;
                }
            }
            MessageBox.Show("Los archivos han sido exportados", "Sisnet", MessageBoxButtons.OK, MessageBoxIcon.Asterisk);
        }

        private void btnExportar_Click(object sender, EventArgs e)
        {
            if (this.btnExportar.Text == "Cancelar")
            {
                if (this.bgw.IsBusy)
                {
                    this.bgw.CancelAsync();
                }
                return;
            }
            this.folderBrowserExport.ShowNewFolderButton = true;
            if (this.folderBrowserExport.ShowDialog() != DialogResult.OK)
            {
                return;
            }
            this.btnExportar.Text = "Cancelar";
            this.lblExport.Visible = true;
            Environment.SpecialFolder rootFolder = this.folderBrowserExport.RootFolder;
            this.bgw.RunWorkerAsync();
        }

        private void btnOpenFile_Click(object sender, EventArgs e)
        {
            string empty = string.Empty;
            using (OpenFileDialog openFileDialog = new OpenFileDialog())
            {
                openFileDialog.Filter = "csv files (*.csv)|*.csv|txt files (*.txt)|*.txt|All files (*.*)|*.*";
                openFileDialog.FilterIndex = 1;
                openFileDialog.RestoreDirectory = true;
                if (openFileDialog.ShowDialog() == DialogResult.OK)
                {
                    empty = openFileDialog.FileName;
                    this.textBox1.Text = empty;
                    using (StreamReader streamReader = new StreamReader(openFileDialog.OpenFile()))
                    {
                        this.fileContent = streamReader.ReadToEnd();
                    }
                }
            }
            this.ChangeData();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            using (FileStream fileStream = new FileStream("E:\\Personal\\impuestoliquidacion.png", FileMode.Open, FileAccess.Read))
            {
                using (BinaryReader binaryReader = new BinaryReader(new BufferedStream(fileStream)))
                {
                    DBManager dBManager = DBManager.GetDBManager();
                    byte[] numArray = binaryReader.ReadBytes(Convert.ToInt32(fileStream.Length));
                    dBManager.Insert((new FileInfo(fileStream.Name)).Name, numArray);
                }
            }
        }

        private void ChangeData()
        {
            string str;
            this.dataToExport.Clear();
            if (string.IsNullOrEmpty(this.fileContent))
            {
                return;
            }
            string[] strArrays = this.fileContent.Split("\r\n".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
            for (int i = 0; i < (int)strArrays.Length; i++)
            {
                string[] strArrays1 = strArrays[i].Split(",".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
                DataRow dataRow = this.dataToExport.NewRow();
                dataRow["Consecutivo"] = strArrays1[0];
                dataRow["NombreExportar"] = strArrays1[1];
                this.dataToExport.Rows.Add(dataRow);
            }
            if (string.IsNullOrEmpty(this.valueConsecutivo))
            {
                return;
            }
            if (string.IsNullOrEmpty(this.valueArchivoName))
            {
                return;
            }
            if (string.IsNullOrEmpty(this.valueArchivo))
            {
                return;
            }
            List<ExportInfo> data = DBManager.GetDBManager().GetData(this.valueTable.ToString(), this.dataToExport, this.valueConsecutivo, this.valueArchivoName, this.valueArchivo);
            int num = 0;
            foreach (DataRow row in this.dataToExport.Rows)
            {
                ExportInfo exportInfo = (
                    from itemDB in data
                    where itemDB.Consecutivo == row["Consecutivo"].ToString()
                    select itemDB).FirstOrDefault<ExportInfo>();
                if (exportInfo != null)
                {
                    row["NombreOriginal"] = exportInfo.ArchivoName;
                    FileInfo fileInfo = new FileInfo(exportInfo.ArchivoName);
                    DataRow dataRow1 = row;
                    object item = row["NombreExportar"];
                    if (item != null)
                    {
                        str = item.ToString();
                    }
                    else
                    {
                        str = null;
                    }
                    dataRow1["NombreExportar"] = string.Concat(str, fileInfo.Extension);
                    row["Tamaño"] = exportInfo.ArchivoLength;
                    row["Estado"] = "Ok";
                }
                else
                {
                    row["Estado"] = "Consecutivo no existe";
                }
                num++;
            }
            bool flag = false;
            foreach (DataGridViewRow red in (IEnumerable)this.dataGridView1.Rows)
            {
                if (red.Cells[5].Value == null || Convert.ToString(red.Cells[5].Value).Contains("Ok"))
                {
                    continue;
                }
                red.DefaultCellStyle.BackColor = Color.Red;
                flag = true;
            }
            if (flag)
            {
                MessageBox.Show("El arhivo posee consecutivos que no existen en la base de datos.", "Sisnet", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
            }
        }

        private void cmbField_SelectedIndexChanged(object sender, EventArgs e)
        {
            KeyValuePair<string, string> selectedItem;
            if (this.cmbConsecutivo.SelectedItem != null)
            {
                selectedItem = (KeyValuePair<string, string>)this.cmbConsecutivo.SelectedItem;
                this.valueConsecutivo = selectedItem.Key;
            }
            if (this.cmbArchivoName.SelectedItem != null)
            {
                selectedItem = (KeyValuePair<string, string>)this.cmbArchivoName.SelectedItem;
                this.valueArchivoName = selectedItem.Key;
            }
            if (this.cmbArchivo.SelectedItem != null)
            {
                selectedItem = (KeyValuePair<string, string>)this.cmbArchivo.SelectedItem;
                this.valueArchivo = selectedItem.Key;
            }
            this.ChangeData();
        }

        private void cmbTables_SelectedIndexChanged(object sender, EventArgs e)
        {
            this.valueTable = ((KeyValuePair<string, string>)this.cmbTables.SelectedItem).Key;
            if (!string.IsNullOrEmpty(this.valueTable))
            {
                this.cmbConsecutivo.Enabled = true;
                this.cmbArchivoName.Enabled = true;
                this.cmbArchivo.Enabled = true;
                this.LoadFields(this.valueTable);
                return;
            }
            this.cmbConsecutivo.Enabled = false;
            this.cmbArchivoName.Enabled = false;
            this.cmbArchivo.Enabled = false;
            this.cmbConsecutivo.DataSource = null;
            this.cmbArchivoName.DataSource = null;
            this.cmbArchivo.DataSource = null;
        }


        private void ExportData(DoWorkEventArgs e)
        {
            string str1;
            if (string.IsNullOrEmpty(this.fileContent))
            {
                return;
            }
            if (string.IsNullOrEmpty(this.valueConsecutivo))
            {
                return;
            }
            if (string.IsNullOrEmpty(this.valueArchivoName))
            {
                return;
            }
            if (string.IsNullOrEmpty(this.valueArchivo))
            {
                return;
            }
            DBManager dBManager = DBManager.GetDBManager();
            List<string> strs = new List<string>();
            String archivoExportar = string.Empty;
            DataView dataViews = this.dataToExport.AsDataView();
            dataViews.RowFilter = "Estado like 'Ok%'";
            DataView dataViews1 = this.dataToExport.AsDataView();
            List<string> list = dataViews.Cast<DataRowView>().Select<DataRowView, string>((DataRowView drv) =>
            {
                string str;
                object item = drv["Consecutivo"];
                if (item != null)
                {
                    str = item.ToString();
                }
                else
                {
                    str = null;
                }
                return string.Concat("'", str, "'");
            }).ToList<string>();
            DirectoryInfo directoryInfo = new DirectoryInfo(this.folderBrowserExport.SelectedPath);
            int num = 0;
            while (list.Any<string>())
            {
                List<string> list1 = list.Take<string>(50).ToList<string>();
                string str2 = string.Join(",", list1.ToArray());
                foreach (ExportInfo dataFile in dBManager.GetDataFile(this.valueTable.ToString(), this.dataToExport, this.valueConsecutivo, this.valueArchivoName, this.valueArchivo, str2))
                {
                    try
                    {
                        if (!this.bgw.CancellationPending)
                        {
                            dataViews1.RowFilter = string.Concat("Consecutivo = '", dataFile.Consecutivo, "'");
                            DataRow archivoName = (
                                from DataRowView drv in dataViews1
                                select drv.Row).First<DataRow>();
                            archivoName["NombreOriginal"] = dataFile.ArchivoName;
                            archivoName["Tamaño"] = dataFile.ArchivoLength;
                            archivoName["Estado"] = "Ok";
                            if (dataFile.ArchivoData != null)
                            {
                                using (MemoryStream memoryStream = new MemoryStream(dataFile.ArchivoData))
                                {
                                    archivoName["NombreExportado"] = archivoName["NombreExportar"];
                                    archivoExportar = archivoName["NombreExportar"].ToString();
                                    FileInfo fileInfo = new FileInfo(archivoExportar);
                                    string str3 = fileInfo.Name.Replace(fileInfo.Extension, string.Concat("*", fileInfo.Extension));
                                    IEnumerable<FileInfo> fileInfos = directoryInfo.GetFiles(str3);
                                    if (fileInfos.Any<FileInfo>())
                                    {
                                        string name = fileInfo.Name;
                                        string extension = fileInfo.Extension;
                                        int num1 = fileInfos.Count<FileInfo>();
                                        str3 = name.Replace(extension, string.Concat("_", num1.ToString(), fileInfo.Extension));
                                        archivoName["NombreExportado"] = str3;
                                        archivoName["Estado"] = "Ok-Renombrado";
                                    }
                                    string selectedPath = this.folderBrowserExport.SelectedPath;
                                    object obj = archivoName["NombreExportado"];
                                    if (obj != null)
                                    {
                                        str1 = obj.ToString();
                                    }
                                    else
                                    {
                                        str1 = null;
                                    }
                                    using (FileStream fileStream = new FileStream(string.Concat(selectedPath, "\\", str1), FileMode.Create, FileAccess.Write))
                                    {
                                        byte[] numArray = new byte[memoryStream.Length];
                                        memoryStream.Read(numArray, 0, (int)memoryStream.Length);
                                        fileStream.Write(numArray, 0, (int)numArray.Length);
                                        memoryStream.Close();
                                    }
                                }
                                dataFile.ArchivoData = null;
                            }
                            int count = num * 100 / dataViews.Count;
                            num++;
                            this.bgw.ReportProgress(count);
                        }
                        else
                        {
                            e.Cancel = true;
                            return;
                        }
                    }
                    catch (Exception ex)
                    {
                        String message = ex.Message + "\r\n";
                        message += "Context {\r\n\tConsecutivo:" + dataFile.Consecutivo;
                        message += "\r\n\tFileName:" + dataFile.ArchivoName;
                        message += "\r\n\tFile:" + archivoExportar + "\r\n}";

                        throw new ApplicationException(message, ex);
                    }
                }
                list.RemoveRange(0, list1.Count);

            }
        }

        private void FrmDataExtractor_Load(object sender, EventArgs e)
        {
            this.LoadTables();
        }


        private void LoadFields(string tableName)
        {
            Dictionary<string, string> fields = DBManager.GetDBManager().GetFields(tableName);
            Dictionary<string, string> strs = new Dictionary<string, string>();
            Dictionary<string, string> strs1 = new Dictionary<string, string>();
            strs.Add(string.Empty, "-Seleccione-");
            strs1.Add(string.Empty, "-Seleccione-");
            Dictionary<string, string> strs2 = new Dictionary<string, string>()
            {
                { string.Empty, "-Seleccione-" }
            };
            foreach (KeyValuePair<string, string> field in fields)
            {
                if (string.Compare(field.Value, "bytea", true) != 0)
                {
                    strs.Add(field.Key, field.Key);
                    strs1.Add(field.Key, field.Key);
                }
                else
                {
                    strs2.Add(field.Key, field.Key);
                }
            }
            this.cmbArchivo.DataSource = new BindingSource(strs2, null);
            this.cmbArchivo.DisplayMember = "Value";
            this.cmbArchivo.ValueMember = "Key";
            this.cmbArchivoName.DataSource = new BindingSource(strs, null);
            this.cmbArchivoName.DisplayMember = "Value";
            this.cmbArchivoName.ValueMember = "Key";
            this.cmbConsecutivo.DataSource = new BindingSource(strs1, null);
            this.cmbConsecutivo.DisplayMember = "Value";
            this.cmbConsecutivo.ValueMember = "Key";
        }

        private void LoadTables()
        {
            List<string> tables = DBManager.GetDBManager().GetTables();
            Dictionary<string, string> strs = new Dictionary<string, string>()
            {
                { string.Empty, "-Seleccione una tabla-" }
            };
            foreach (string table in tables)
            {
                strs.Add(table, table);
            }
            this.cmbTables.DataSource = new BindingSource(strs, null);
            this.cmbTables.DisplayMember = "Value";
            this.cmbTables.ValueMember = "Key";
        }
        #endregion

    }
}
