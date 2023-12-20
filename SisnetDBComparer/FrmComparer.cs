using SisnetData;
using SisnetDBComparer.Dto;
using SisnetDBComparer.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.Drawing.Imaging;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Timers;
using System.Windows.Forms;
using static System.Windows.Forms.VisualStyles.VisualStyleElement;
using static System.Windows.Forms.VisualStyles.VisualStyleElement.Button;
using static System.Windows.Forms.VisualStyles.VisualStyleElement.ProgressBar;

namespace SisnetDBComparer
{
    public partial class FrmComparer : Form
    {

        private bool conexionExitosa1;
        private bool conexionExitosa2;
        string namebd1;
        string namebd2;

        private DateTime startTime;


        DBManager dbManager1 = DBManager.GetInstance();
        DBManager dbManager2 = DBManager.GetInstance();

        List<ItemDTO> totalData = new List<ItemDTO>();
        Hashtable ht = new Hashtable();

        ItemDTO itemDTOSelected1;
        ItemDTO itemDTOSelected2;

        DataTable filledTable1 = null;
        DataTable filledTable2 = null;

        DataTable resumedTable1 = null;
        DataTable resumedTable2 = null;

        private bool dataEquals;



        public FrmComparer()
        {
            InitializeComponent();
            ht.Add(Utils.Status.Negro.ToString(), "N/A");
            ht.Add(Utils.Status.Verde.ToString(), "Iguales");
            ht.Add(Utils.Status.Amarillo.ToString(), "Diferentes");
        }

        private void btnConectar_Click(object sender, EventArgs e)
        {
            this.btnConectar.Enabled = false;
            DBManager dBManager = DBManager.GetDBManager();
            try
            {
                this.conexionExitosa1 = false;
                this.conexionExitosa2 = false;

                this.lblStatus1.Text = "Conectando...";
                this.lblStatus1.ForeColor = Color.Blue;

                this.lblStatus2.Text = "Conectando...";
                this.lblStatus2.ForeColor = Color.Blue;

                dBManager.SetDBManager(this.txtHost1.Text, "", this.txtUser1.Text, this.txtPwd1.Text);
                this.conexionExitosa1 = true;
                this.lblStatus1.Text = "Conectado";
                this.lblStatus1.ForeColor = Color.Green;


                dBManager.SetDBManager(this.txtHost2.Text, "", this.txtUser2.Text, this.txtPwd2.Text);
                this.conexionExitosa2 = true;
                this.lblStatus2.Text = "Conectado";
                this.lblStatus2.ForeColor = Color.Green;

                // Limpiar el ListView
                this.cmbBD1.Items.Clear();
                this.cmbBD2.Items.Clear();

                List<string> dbs1 = this.GetDBManager(Conexion.Conexion1).GetBds();
                List<string> dbs2 = this.GetDBManager(Conexion.Conexion1).GetBds();

                this.cmbBD1.Items.AddRange(dbs1.ToArray());
                this.cmbBD2.Items.AddRange(dbs2.ToArray());

                this.cmbBD1.SelectedItem = "Fanapopequeweb";
                this.cmbBD2.SelectedItem = "Fanapofullweb";

                this.tbComparador.SelectedTab = this.tabPageComparer;

            }
            catch (Exception exception2)
            {
                if (!this.conexionExitosa1)
                {
                    this.lblStatus1.Text = "Error";
                    this.lblStatus1.ForeColor = Color.Red;
                }
                if (!this.conexionExitosa2)
                {
                    this.lblStatus2.Text = "Error" + (!this.conexionExitosa1 ? " (Resuelva conexión 1)" : "");
                    this.lblStatus2.ForeColor = Color.Red;
                }

                MessageBox.Show(exception2.Message, "Error de conexión", MessageBoxButtons.OK, MessageBoxIcon.Hand);
            }
            finally
            {
                if (dBManager != null)
                {
                    dBManager.CloseConnection();
                }
                this.btnConectar.Enabled = true;
            }
        }


        private DBManager GetDBManager(Conexion conexion, String database = "")
        {
            if (conexion == Conexion.Conexion1)
            {
                this.dbManager1.CloseConnection();
                this.dbManager1.SetDBManager(this.txtHost1.Text, database, this.txtUser1.Text, this.txtPwd1.Text);
                return this.dbManager1;
            }
            else
            {
                this.dbManager2.CloseConnection();
                this.dbManager2.SetDBManager(this.txtHost2.Text, database, this.txtUser2.Text, this.txtPwd2.Text);
                return this.dbManager2;
            }
        }

        // private ProgressBar pgb;


        private void bgw_DoWork(object sender, DoWorkEventArgs e)
        {
            this.FillCounters(e);
            if (e.Cancel)
            {
                return;
            }
            this.bgw.ReportProgress(100);
        }



        private void bgw_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            this.pgb.Value = e.ProgressPercentage;
            if (e.UserState == null)
                return;
            ItemDTO item = (ItemDTO)e.UserState;
            this.lblStatusProgress.Text = !string.IsNullOrEmpty(item.Table1) ? item.Table1 : item.Table2;
            //this.listView1.Items[item.Index].ImageIndex = (int)item.Status;
            ListViewItem viewItem = this.lvComparer.Items[item.Index];

            viewItem.SubItems[(int)ColIndexComparer.ImageComparer].Text = ht[item.Status.ToString()].ToString();
            viewItem.SubItems[(int)ColIndexComparer.Counter1].Text = item.CountTable1.ToString();
            viewItem.SubItems[(int)ColIndexComparer.Counter2].Text = item.CountTable2.ToString();
            //viewItem.EnsureVisible();
        }

        private void bgw_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {

            //this.lblExport.Visible = false;

            this.btnComparar.Text = "Comparar";
            if (e.Cancelled)
            {
                this.timerDuration.Stop();
                this.lblStatusProgress.Text = "Conteo cancelado";
                MessageBox.Show("El proceso de conteo ha sido cancelado", "Sisnet", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                return;
            }
            if (e.Error != null)
            {
                this.timerDuration.Stop();
                this.lblStatusProgress.Text = "Error conteo";
                String message = e.Error.Message;
                message += e.Error.InnerException != null ? "\r\n[" + e.Error.InnerException.Message + "]" : string.Empty;
                message += e.Error.InnerException != null ? "\r\n" + e.Error.InnerException.StackTrace + "\r\n\r\n" : string.Empty;
                message += "\r\n" + e.Error.StackTrace;
                MessageBox.Show("Error ejecutando el conteo.\r\n" + message, "Sisnet", MessageBoxButtons.OK, MessageBoxIcon.Hand);
                return;
            }


            this.lblStatusProgress.Text = "Conteo completado";
            // MessageBox.Show("Conteo completado", "Sisnet", MessageBoxButtons.OK, MessageBoxIcon.Asterisk);
            this.btnComparar.Text = "Cancelar analisis";

            this.bgwAnalyzer.RunWorkerAsync();
        }


        private void bgwAnalyzer_DoWork(object sender, DoWorkEventArgs e)
        {
            this.FillComparation(e);
            if (e.Cancel)
            {
                return;
            }
            this.bgwAnalyzer.ReportProgress(100);
        }


        private void bgwAnalyzer_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            this.pgb.Value = e.ProgressPercentage;
            if (e.UserState == null)
                return;
            ItemDTO item = (ItemDTO)e.UserState;
            this.lblStatusProgress.Text = !string.IsNullOrEmpty(item.Table1) ? item.Table1 : item.Table2;
            this.lvComparer.Items[item.Index].ImageIndex = item.EqualData.HasValue ? (item.EqualData.Value ? (int)Utils.Status.Equal : (int)Utils.Status.NotEqual) : (int)Utils.Status.Cargando;
            this.lvComparer.Items[item.Index].EnsureVisible();
        }

        private void bgwAnalyzer_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {

            //this.lblExport.Visible = false;
            this.timerDuration.Stop();
            this.btnComparar.Text = "Comparar";
            if (e.Cancelled)
            {
                this.lblStatusProgress.Text = "Comparación cancelada";
                MessageBox.Show("El proceso de comparación ha sido cancelado", "Sisnet", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                return;
            }
            if (e.Error != null)
            {
                this.lblStatusProgress.Text = "Error conteo";
                String message = e.Error.Message;
                message += e.Error.InnerException != null ? "\r\n[" + e.Error.InnerException.Message + "]" : string.Empty;
                message += e.Error.InnerException != null ? "\r\n" + e.Error.InnerException.StackTrace + "\r\n\r\n" : string.Empty;
                message += "\r\n" + e.Error.StackTrace;
                MessageBox.Show("Error ejecutando la comparación.\r\n" + message, "Sisnet", MessageBoxButtons.OK, MessageBoxIcon.Hand);
                return;
            }

            this.lblStatusProgress.Text = "Comparación completada";
            this.grpFiltro.Enabled = true;
            MessageBox.Show("Comparación completada", "Sisnet", MessageBoxButtons.OK, MessageBoxIcon.Asterisk);
        }


        private void FillCounters(DoWorkEventArgs e)
        {
            int num = 0;
            try
            {
                List<TableInfo> lsTables1 = this.totalData.Where(t => !string.IsNullOrEmpty(t.Table1))
                    .Select(tc => (new TableInfo() { Name = tc.Table1 })).ToList();

                List<TableInfo> lsTables2 = this.totalData.Where(t => !string.IsNullOrEmpty(t.Table2))
                    .Select(tc => (new TableInfo() { Name = tc.Table2 })).ToList();

                this.dbManager1.GetRecordCount(lsTables1);
                this.bgw.ReportProgress(50, null);
                this.dbManager2.GetRecordCount(lsTables2);


                foreach (ItemDTO row in this.totalData)
                {
                    if (!this.bgw.CancellationPending)
                    {
                        string tablaOrigen = row.Table1;
                        string tablaDestino = row.Table2;
                        long count1 =
                            string.IsNullOrEmpty(tablaOrigen) ? 0 :
                            (from t in lsTables1 where t.Name == tablaOrigen select t.Count).Single();

                        long count2 = string.IsNullOrEmpty(tablaDestino) ? 0 :
                            (from t in lsTables2 where t.Name == tablaDestino select t.Count).Single();

                        row.CountTable1 = count1;
                        row.CountTable2 = count2;

                        if (tablaOrigen != tablaDestino)
                        {
                            row.Status = Utils.Status.Negro;
                        }
                        else if (count1 == count2)
                        {
                            row.Status = Utils.Status.Verde;
                        }
                        else if (count1 != count2)
                        {
                            row.Status = Utils.Status.Amarillo;
                        }

                        int count = num * 100 / this.totalData.Count;
                        num++;
                        this.bgw.ReportProgress(count, row);
                    }
                    else
                    {
                        e.Cancel = true;
                        return;
                    }

                }

            }
            catch (Exception ex)
            {
                String message = ex.Message + "\r\n";
                //message += "Context {\r\n\tConsecutivo:" + dataFile.Consecutivo;
                //message += "\r\n\tFileName:" + dataFile.ArchivoName;
                //message += "\r\n\tFile:" + archivoExportar + "\r\n}";

                throw new ApplicationException(message, ex);
            }
        }


        private void FillComparation(DoWorkEventArgs e)
        {
            int num = 0;

            // Guardar el tiempo de inicio del proceso
            this.startTime = DateTime.Now;

            this.dbManager1.OpenConnection();
            this.dbManager2.OpenConnection();

            try
            {
                // Definir el tamaño del subciclo (10 en este caso)
                int subcycleSize = 10;
                //int paquete = 1;
                // Iterar sobre la lista de 10 en 10 utilizando Take
                for (int i = 0; i < this.totalData.Count; i += subcycleSize)
                {
                    // Utilizando ThreadPool para simular Parallel.ForEach
                    ManualResetEvent resetEvent = new ManualResetEvent(false);

                    // Utilizar Take para obtener el subconjunto de 10 elementos
                    IEnumerable<ItemDTO> subcycle = this.totalData.Skip(i).Take(subcycleSize);
                    int remainingTasks = subcycle.Count();

                    foreach (ItemDTO row in subcycle)
                    {
                        //ThreadPool.QueueUserWorkItem((state) =>
                        {
                            // Realiza la operación para cada elemento
                            Console.WriteLine(row);
                            if (this.bgwAnalyzer.CancellationPending)
                            {
                                e.Cancel = true;
                                return;
                            }

                            int percentageTotalAvance = num * 100 / this.totalData.Count;
                            this.bgwAnalyzer.ReportProgress(percentageTotalAvance, row);
                            string tablaOrigen = row.Table1;
                            string tablaDestino = row.Table2;

                            if (string.Compare(tablaOrigen, tablaDestino) != 0)
                            {
                                row.EqualData = false;
                            }
                            else if (row.CountTable1 != row.CountTable2)
                            {
                                row.EqualData = false;
                            }

                            if (row.EqualData.HasValue && !row.EqualData.Value)
                            {
                                this.bgwAnalyzer.ReportProgress(percentageTotalAvance, row);
                                continue;
                            }

                            //DBManager dbManagerTo1 = null;
                            //DBManager dbManagerTo2 = null;
                            
                            //Si es menos de 100 Megas se compara en memoria, de lo contrario en paquetes online
                            if (row.Table1SizeNum / 1000000 < 100)
                            {
                                this.CompareInMemory(e, row, ref num, percentageTotalAvance);
                            }
                            else
                            {
                                this.CompareOnline(e, row, ref num, percentageTotalAvance);
                            }

                        }
                        //);

                    }

                    // Realiza la operación para cada elemento
                    //Console.WriteLine("Hilos lanzados del paquete " + paquete);

                    // Espera a que todos los elementos se procesen
                    //resetEvent.WaitOne();

                    //paquete++;
                }
            }
            catch (Exception parallelException)
            {
                throw parallelException;
            }
            finally
            {
                this.dbManager1.CloseConnection();
                this.dbManager2.CloseConnection();
            }
        }

        private void CompareInMemory(DoWorkEventArgs e, ItemDTO row, ref int num, int percentageTotalAvance)
        {
            try
            {

                //dbManagerTo1 = DBManager.GetInstance();
                //dbManagerTo2 = DBManager.GetInstance();

                //dbManagerTo1.SetDBManager(this.txtHost1.Text, namebd1, this.txtUser1.Text, this.txtPwd1.Text, false);
                //dbManagerTo2.SetDBManager(this.txtHost2.Text, namebd2, this.txtUser2.Text, this.txtPwd2.Text, false);

                string tablaOrigen = row.Table1;
                string tablaDestino = row.Table2;
                DataTable tabla1 = null;
                DataTable tabla2 = null;
                if (!string.IsNullOrEmpty(tablaOrigen) && !string.IsNullOrEmpty(tablaDestino))
                {
                    //dbManagerTo1.OpenConnection();
                    tabla1 = dbManager1.GetTableData(row.Table1);
                    tabla1 = ReorderData(tabla1, row.Table1Keys);
                }

                if (!string.IsNullOrEmpty(tablaDestino) && !string.IsNullOrEmpty(tablaOrigen))
                {
                    //dbManagerTo2.OpenConnection();
                    tabla2 = dbManager2.GetTableData(row.Table2);
                    tabla2 = ReorderData(tabla2, row.Table2Keys);
                }

                this.bgwAnalyzer.ReportProgress(percentageTotalAvance, row);
                // Comparar DataTables
                row.EqualData = SonDataTablesIguales(tabla1, tabla2, false);

                this.bgwAnalyzer.ReportProgress(percentageTotalAvance, row);


            }
            catch (Exception ex)
            {
                Console.WriteLine("Error paralelo " + ex.Message);
                String message = ex.Message + "\r\n";
                message += "Context \r\nTabla:[" + row.Table1 + "] - [" + row.Table2 + "]";

                throw new ApplicationException(message, ex);
            }
            finally
            {
                num++;
                /*
                if (dbManagerTo1 != null)
                {
                    dbManagerTo1.CloseConnection();
                }
                if (dbManagerTo2 != null)
                {
                    dbManagerTo2.CloseConnection();
                }

                if (Interlocked.Decrement(ref remainingTasks) == 0)
                {
                    // Todos los elementos se han procesado
                    resetEvent.Set();
                }*/
            }

        }


        private void CompareOnline(DoWorkEventArgs e, ItemDTO row, ref int num, int percentageTotalAvance)
        {
            try
            {

                //dbManagerTo1 = DBManager.GetInstance();
                //dbManagerTo2 = DBManager.GetInstance();

                //dbManagerTo1.SetDBManager(this.txtHost1.Text, namebd1, this.txtUser1.Text, this.txtPwd1.Text, false);
                //dbManagerTo2.SetDBManager(this.txtHost2.Text, namebd2, this.txtUser2.Text, this.txtPwd2.Text, false);

                string tablaOrigen = row.Table1;
                string tablaDestino = row.Table2;
                DataTable tabla1 = null;
                DataTable tabla2 = null;
                if (!string.IsNullOrEmpty(tablaOrigen) && !string.IsNullOrEmpty(tablaDestino))
                {
                    //dbManagerTo1.OpenConnection();
                    tabla1 = dbManager1.GetTableData(row.Table1);
                    tabla1 = ReorderData(tabla1, row.Table1Keys);
                }

                if (!string.IsNullOrEmpty(tablaDestino) && !string.IsNullOrEmpty(tablaOrigen))
                {
                    //dbManagerTo2.OpenConnection();
                    tabla2 = dbManager2.GetTableData(row.Table2);
                    tabla2 = ReorderData(tabla2, row.Table2Keys);
                }

                this.bgwAnalyzer.ReportProgress(percentageTotalAvance, row);
                // Comparar DataTables
                row.EqualData = SonDataTablesIguales(tabla1, tabla2, false);

                this.bgwAnalyzer.ReportProgress(percentageTotalAvance, row);


            }
            catch (Exception ex)
            {
                Console.WriteLine("Error paralelo " + ex.Message);
                String message = ex.Message + "\r\n";
                message += "Context \r\nTabla:[" + row.Table1 + "] - [" + row.Table2 + "]";

                throw new ApplicationException(message, ex);
            }
            finally
            {
                num++;
                /*
                if (dbManagerTo1 != null)
                {
                    dbManagerTo1.CloseConnection();
                }
                if (dbManagerTo2 != null)
                {
                    dbManagerTo2.CloseConnection();
                }

                if (Interlocked.Decrement(ref remainingTasks) == 0)
                {
                    // Todos los elementos se han procesado
                    resetEvent.Set();
                }*/
            }

        }

        private void btnComparar_Click(object sender, EventArgs e)
        {
            try
            {

                if (this.btnComparar.Text == "Cancelar")
                {
                    if (this.bgw.IsBusy)
                    {
                        this.bgw.CancelAsync();
                    }
                    return;
                }

                if (this.btnComparar.Text == "Cancelar analisis")
                {
                    if (this.bgwAnalyzer.IsBusy)
                    {
                        this.bgwAnalyzer.CancelAsync();
                    }
                    return;
                }

                this.btnComparar.Text = "Cancelar";
                this.pgb.Value = 0;
                this.lblElapsed.Text = string.Empty;
                this.startTime = DateTime.Now;
                // Iniciar el Timer
                this.timerDuration.Start();

                //this.lblExport.Visible = true;
                this.grpFiltro.Enabled = false;

                lvComparer.Items.Clear();



                List<TableInfo> tables1Info = this.dbManager1.GetTables();
                List<TableInfo> tables2Info = this.dbManager2.GetTables();

                List<string> tables1 = tables1Info.Select(tableInfo => tableInfo.Name).ToList();
                List<string> tables2 = tables2Info.Select(tableInfo => tableInfo.Name).ToList();

                // Distinct y Union
                List<string> totalTables = tables1Info
                    .Union(tables2Info)
                    .Select(tableInfo => tableInfo.Name)
                    .Distinct()
                    .OrderBy(tableName => tableName)
                    .ToList();


                this.totalData.Clear();
                int index = 0;
                foreach (string s in totalTables)
                {
                    string table1 = string.Empty;
                    string table2 = string.Empty;
                    string table1Size = string.Empty;
                    string table2Size = string.Empty;
                    long table1SizeNum = 0;
                    long table2SizeNum = 0;
                    List<String> table1Keys = new List<string>();
                    List<String> table2Keys = new List<string>();

                    if (tables1.Contains(s))
                    {
                        table1 = s;
                        table1Size = tables1Info.Where(t => t.Name == s).Single().SizeInfo;
                        table1Keys = tables1Info.Where(t => t.Name == s).Single().Keys;
                        table1SizeNum = tables1Info.Where(t => t.Name == s).Single().Size;
                    }


                    if (tables2.Contains(s))
                    {
                        table2 = s;
                        table2Size = tables2Info.Where(t => t.Name == s).Single().SizeInfo;
                        table2Keys = tables2Info.Where(t => t.Name == s).Single().Keys;
                        table2SizeNum = tables2Info.Where(t => t.Name == s).Single().Size;
                    }


                    this.totalData.Add(new ItemDTO()
                    {
                        Index = index,
                        Status = 0,
                        Table1 = table1,
                        Table2 = table2,
                        Table1Size = table1Size,
                        Table2Size = table2Size,
                        Table1Keys = table1Keys,
                        Table2Keys = table2Keys,
                        Table1SizeNum = table1SizeNum,
                        Table2SizeNum = table2SizeNum,

                    });

                    string[] filaDatos = new string[] { null, null, (++index).ToString(), table1, "0", table1Size, table2, "0", table2Size };

                    var listItem = new ListViewItem(filaDatos, 0);
                    listItem = lvComparer.Items.Add(listItem);

                }

                this.lblStatusProgress.Text = "Contando registros...";
                this.bgw.RunWorkerAsync();

            }
            catch (Exception ex)
            {
                this.timerDuration.Stop();
                this.btnComparar.Text = "Comparar";
                MessageBox.Show("Error ejecutando la comparación.\r\n" + ex.Message, "Sisnet", MessageBoxButtons.OK, MessageBoxIcon.Hand);
            }
        }




        private void cmbBD1_SelectedIndexChanged(object sender, EventArgs e)
        {
            try
            {
                this.namebd1 = cmbBD1.SelectedItem.ToString();
                this.colHeaderBD1.Text = this.namebd1;
                this.lvDetailTables.Columns[2].Text = this.namebd1;
                this.dbManager1 = this.GetDBManager(Conexion.Conexion1, this.namebd1);
            }
            catch (Exception exception)
            {
                MessageBox.Show(exception.Message, "Error de conexión 1", MessageBoxButtons.OK, MessageBoxIcon.Hand);
                this.tbComparador.SelectedTab = this.tabPageConexion;
            }
        }

        private void cmbBD2_SelectedIndexChanged(object sender, EventArgs e)
        {
            try
            {
                this.namebd2 = cmbBD2.SelectedItem.ToString();
                this.colHeaderBD2.Text = this.namebd2;
                this.lvDetailTables.Columns[5].Text = this.namebd2;
                this.dbManager2 = this.GetDBManager(Conexion.Conexion2, this.namebd2);
            }
            catch (Exception exception)
            {
                MessageBox.Show(exception.Message, "Error de conexión 2", MessageBoxButtons.OK, MessageBoxIcon.Hand);
                this.tbComparador.SelectedTab = this.tabPageConexion;
            }
        }

        private void chkviewer_CheckedChanged(object sender, EventArgs e)
        {
            this.lvComparer.Items.Clear();
            int i = 1;
            foreach (ItemDTO item in this.totalData)
            {
                string countComparer = ht[item.Status.ToString()].ToString();
                string[] filaDatos = new string[] { null,  countComparer, (i++).ToString(), item.Table1,
                    item.CountTable1.ToString(), item.Table1Size, item.Table2, item.CountTable2.ToString() , item.Table2Size };

                int imageIndex = item.EqualData.HasValue ? (item.EqualData.Value ? (int)Utils.Status.Equal : (int)Utils.Status.NotEqual) : (int)Utils.Status.Cargando;


                if (this.chkCounter.Checked && item.Status == Utils.Status.Amarillo)
                {
                    ListViewItem lv = new ListViewItem(filaDatos, imageIndex);
                    lvComparer.Items.Add(lv);
                }
                else if (this.chkEquals.Checked && item.Status == Utils.Status.Verde)
                {
                    lvComparer.Items.Add(new ListViewItem(filaDatos, imageIndex));
                }
                else if (this.chkNoMatch.Checked && item.Status == Utils.Status.Negro)
                {
                    lvComparer.Items.Add(new ListViewItem(filaDatos, imageIndex));
                }
            }
        }

        private void tbComparador_SelectedIndexChanged(object sender, EventArgs e)
        {

            if (sender == this.tbComparador && this.tbComparador.SelectedTab == this.tabPageDetail)
            {
                if (!this.grpFiltro.Enabled)
                {
                    MessageBox.Show("Aún no están analizados los datos", "Aviso analisís", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                    this.tbComparador.SelectedTab = this.tabPageComparer;
                    return;
                }

                this.lblInfoTable1.Text = this.namebd1 + "(N/A)";
                this.lblInfoTable2.Text = this.namebd2 + "(N/A)";
                // fire change
                this.chkDataEqual.Checked = true;
                EventArgs checkEvent = EventArgs.Empty; // Puedes usar EventArgs.Empty o crear una instancia específica según tus necesidades
                chkDataFilter_CheckedChanged(this.chkDataEqual, checkEvent);
            }
        }

        private void lvDetailTables_SelectedIndexChanged(object sender, EventArgs e)
        {
            if (this.bgwDetails.IsBusy || this.bgwSync.IsBusy)
            {
                return;
            }

            this.filledTable1 = null;
            this.filledTable2 = null;

            this.resumedTable1 = null;
            this.resumedTable2 = null;

            this.dataGridViewer1.DataSource = null;
            this.dataGridViewer2.DataSource = null;

            this.dataGridViewer1.Refresh();
            this.dataGridViewer2.Refresh();

            this.lblInfoTable1.Text = this.namebd1 + "(N/A)";
            this.lblInfoTable2.Text = this.namebd2 + "(N/A)";

            this.lblInfoTable1.ForeColor = Color.Black;
            this.lblInfoTable2.ForeColor = Color.Black;

            this.lblRecords1.Text = "Registros: 0";
            this.lblRecords2.Text = "Registros: 0";

            if (lvDetailTables.SelectedItems.Count == 0)
            {
                return;
            }


            ListViewItem item = lvDetailTables.SelectedItems[0];

            this.itemDTOSelected1 = string.IsNullOrEmpty(item.SubItems[(int)ColIndexDetail.Tabla1].Text) ? null : this.totalData.Where(i => i.Table1 == item.SubItems[(int)ColIndexDetail.Tabla1].Text).SingleOrDefault();
            this.itemDTOSelected2 = string.IsNullOrEmpty(item.SubItems[(int)ColIndexDetail.Tabla2].Text) ? null : this.totalData.Where(i => i.Table2 == item.SubItems[(int)ColIndexDetail.Tabla2].Text).SingleOrDefault();

            this.lblInfoTable1.Text = this.namebd1 + (itemDTOSelected1 != null ? $"  ({itemDTOSelected1.Table1})" : "");
            this.lblInfoTable2.Text = this.namebd2 + (itemDTOSelected2 != null ? $"  ({itemDTOSelected2.Table2})" : "");

            this.bgwDetails.RunWorkerAsync();

        }

        private void FillDetails(DoWorkEventArgs e)
        {
            this.bgwDetails.ReportProgress(0, StatusDetails.CargandoData1);
            if (this.itemDTOSelected1 != null)
            {
                filledTable1 = this.dbManager1.GetTableData(itemDTOSelected1.Table1);
                filledTable1 = ReorderData(filledTable1, itemDTOSelected1.Table1Keys);
                /*
                             if (this.bgwAnalyzer.CancellationPending)
            {
                e.Cancel = true;
                return;
            }
                 */

            }

            this.bgwDetails.ReportProgress(25, StatusDetails.CargadoData1);
            this.bgwDetails.ReportProgress(50, StatusDetails.CargandoData2);

            if (this.itemDTOSelected2 != null)
            {
                filledTable2 = this.dbManager2.GetTableData(itemDTOSelected2.Table2);
                filledTable2 = ReorderData(filledTable2, itemDTOSelected2.Table2Keys);
            }

            this.bgwDetails.ReportProgress(75, StatusDetails.CargaCompleta);

            this.bgwDetails.ReportProgress(100, StatusDetails.RefreshGrid);

            // Comparar DataTables
            dataEquals = SonDataTablesIguales(filledTable1, filledTable2, true);

            this.bgwDetails.ReportProgress(100, StatusDetails.ComparacionFinished);

        }

        private List<ItemDTO> GetTablesToSync()
        {

            List<ItemDTO> tablesToSync = (from item in this.totalData
                                          where
                                          !string.IsNullOrEmpty(item.Table1) &&
                                          item.EqualData.HasValue && !item.EqualData.Value
                                          select item).ToList();

            return tablesToSync;


        }
        private void FillSyncData(DoWorkEventArgs e)
        {
            this.bgwSync.ReportProgress(0, StatusSync.SyncDataStart);

            this.DeleteAllFK();
            List<ItemDTO> tablesToSync = this.GetTablesToSync();
            int num = 0;


            foreach (var tableToSync in tablesToSync)
            {

                int count = num * 100 / tablesToSync.Count;

                if (this.bgwSync.CancellationPending)
                {
                    e.Cancel = true;
                    return;
                }

                this.bgwSync.ReportProgress(count, new SyncResult
                {
                    StatusSync = StatusSync.LoadingTables,
                    //Index = i,
                    Item = tableToSync
                });

                tableToSync.LoadedOnlySchema = false;
                List<DataTable> dataTablesToSync = this.LoadTablesToSync(tableToSync);

                if (this.bgwSync.CancellationPending)
                {
                    e.Cancel = true;
                    return;
                }

                this.bgwSync.ReportProgress(count, new SyncResult
                {
                    StatusSync = StatusSync.SyncMovingData,
                    //Index = i,
                    Item = tableToSync
                });

                this.SyncDataFromTable(tableToSync, dataTablesToSync[0], dataTablesToSync[1], true);

                this.bgwSync.ReportProgress(count, new SyncResult
                {
                    StatusSync = StatusSync.SyncFinishTable,
                    //Index = i,
                    Item = tableToSync
                });

                num++;

            }

            // despues de sincronizadas las tablas se crean las foreign keys

            this.CreateAllFK();

            this.bgwSync.ReportProgress(100, StatusSync.SyncDataFinished);

        }

        private void DeleteAllFK()
        {
            DataTable foreignKeys = this.dbManager2.GetForeginKeys(null);

            List<string> deleteFKs = (from DataRow row in foreignKeys.Rows
                                      select "ALTER TABLE " + row["table_origin"] +
                                      " DROP CONSTRAINT " + row["foreign_key_name"] + ";"
                ).ToList();

            string deleteFK = string.Join($"{Environment.NewLine}", deleteFKs.ToArray());
            if (!string.IsNullOrEmpty(deleteFK))
            {
                this.dbManager2.ExecuteSentence(deleteFK);
            }
        }

        private void CreateAllFK()
        {
            DataTable foreignKeys = this.dbManager1.GetForeginKeys(null);

            List<string> createFKs = (from DataRow row in foreignKeys.Rows
                                      select "ALTER TABLE " + row["table_origin"] +
                                      " ADD CONSTRAINT " + row["foreign_key_name"] +
                                      " FOREIGN KEY (" + row["column_origin"] + ")" +
                                      " REFERENCES " + row["table_destination"] +
                                      "(" + row["column_destination"] + ") " +
                                      (
                                      " ON UPDATE" +
                                        (

                                        !string.IsNullOrEmpty(row["action_update"].ToString())
                                            && row["action_update"].ToString() == "c"

                                        ? " CASCADE" : " NO ACTION"
                                        )
                                      )
                                          +
                                        (
                                        " ON DELETE" +
                                        (
                                            !string.IsNullOrEmpty(row["action_update"].ToString())
                                            && row["action_update"].ToString() == "c"

                                            ? " CASCADE" : " NO ACTION"
                                        )
                                        ) + ";"
                                        ).ToList();

            string createFK = string.Join($"{Environment.NewLine}", createFKs.ToArray());
            if (!string.IsNullOrEmpty(createFK))
            {
                this.dbManager2.ExecuteSentence(createFK);
            }
        }

        private List<DataTable> LoadTablesToSync(ItemDTO tableToSync)
        {
            // Utilizando ThreadPool para simular Parallel.ForEach
            ManualResetEvent resetEvent = new ManualResetEvent(false);

            Hashtable hsDt = new Hashtable();
            hsDt.Add("table1", tableToSync.Table1);
            hsDt.Add("table2", tableToSync.Table2);
            DataTable tabla1 = null;
            DataTable tabla2 = null;
            int remainingTasks = hsDt.Count;

            if (tableToSync.CountTable2 == 0)
            {
                tableToSync.LoadedOnlySchema = true;
            }
            foreach (DictionaryEntry itemTable in hsDt)
            {
                string table = itemTable.Value.ToString();
                bool isOrigin = ("table1" == itemTable.Key.ToString());
                ThreadPool.QueueUserWorkItem((state) =>
                {
                    // Realiza la operación para cada elemento
                    Console.WriteLine($"Getting table from [" + (isOrigin ? "source" : "target") + "] to sync : " + table);
                    DBManager dbManagerTo1 = null;
                    DBManager dbManagerTo2 = null;

                    try
                    {
                        if (isOrigin)
                        {
                            dbManagerTo1 = DBManager.GetInstance();
                            dbManagerTo1.SetDBManager(this.txtHost1.Text, namebd1, this.txtUser1.Text, this.txtPwd1.Text, false);
                            tabla1 = dbManagerTo1.GetTableData(table, tableToSync.LoadedOnlySchema);
                        }
                        else if (!string.IsNullOrEmpty(table))
                        {
                            dbManagerTo2 = DBManager.GetInstance();
                            dbManagerTo2.SetDBManager(this.txtHost2.Text, namebd2, this.txtUser2.Text, this.txtPwd2.Text, false);
                            tabla2 = dbManagerTo2.GetTableData(table, tableToSync.LoadedOnlySchema);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Error getting table to sync " + ex.Message);
                        String message = ex.Message + "\r\n";
                        message += "Context \r\nTabla:[" + table + "] - [" + isOrigin + "]";

                        throw new ApplicationException(message, ex);
                    }
                    finally
                    {
                        dbManagerTo1 = null;
                        dbManagerTo2 = null;
                        if (Interlocked.Decrement(ref remainingTasks) == 0)
                        {
                            // Todos los elementos se han procesado
                            resetEvent.Set();
                        }
                    }
                }
                );

            }

            // Realiza la operación para cada elemento
            Console.WriteLine("Hilos lanzados de obtener data");

            // Espera a que todos los elementos se procesen
            resetEvent.WaitOne();

            List<DataTable> dataTables = new List<DataTable>() {
                tabla1, tabla2
            };


            return dataTables;

        }

        DataTable ReorderData(DataTable tabla, List<String> keysTable)
        {
            tabla.PrimaryKey = (from key in keysTable
                                select tabla.Columns[key]).ToArray();

            if (tabla.Rows.Count == 0 || !tabla.PrimaryKey.Any())
                return tabla;

            string sort = $"{string.Join(" ASC, ", keysTable.ToArray())}";
            sort += (tabla.PrimaryKey.Any() ? " ASC" : string.Empty);
            // Ordenar el DataTable por la clave primaria 
            DataRow[] rows = tabla.Select(string.Empty, sort);

            // Crear un nuevo DataTable con las filas ordenadas
            DataTable tablaCopy = rows.CopyToDataTable();
            foreach (DataColumn column in tabla.Columns)
            {
                foreach (DictionaryEntry extended in column.ExtendedProperties)
                {
                    if (!tablaCopy.Columns[column.ColumnName].ExtendedProperties.ContainsKey(extended.Key))
                    {
                        tablaCopy.Columns[column.ColumnName].ExtendedProperties.Add(extended.Key, extended.Value);
                    }
                }
            }
            return tablaCopy;
        }

        // Método para comparar dos DataTables
        bool SonDataTablesIguales(DataTable tabla1, DataTable tabla2, bool paintGridView)
        {
            if ((tabla1 != null && tabla2 == null) || (tabla1 == null && tabla2 != null) || (tabla1 == null && tabla2 == null))
            {
                return false;
            }
            // Verificar si tienen la misma cantidad de columnas
            if (tabla1.Columns.Count != tabla2.Columns.Count)
            {
                return false;
            }

            // Verificar si tienen las mismas columnas y en el mismo orden
            for (int i = 0; i < tabla1.Columns.Count; i++)
            {
                if (tabla1.Columns[i].ColumnName != tabla2.Columns[i].ColumnName ||
                    tabla1.Columns[i].DataType != tabla2.Columns[i].DataType)
                {
                    return false;
                }
            }

            // Verificar si tienen la misma cantidad de filas
            if (tabla1.Rows.Count != tabla2.Rows.Count)
            {
                return false;
            }

            List<int> badRows = new List<int>();
            int num = 0;
            // Verificar si los datos de las columnas son iguales
            for (int i = 0; i < tabla1.Rows.Count; i++)
            {
                int count = num * 100 / tabla1.Rows.Count;

                for (int j = 0; j < tabla1.Columns.Count; j++)
                {
                    if (tabla1.Rows[i][j] is byte[])
                    {
                        if (!((byte[])tabla1.Rows[i][j]).SequenceEqual((byte[])tabla2.Rows[i][j]))
                        {
                            badRows.Add(i);
                            break;
                        }
                    }
                    else if (tabla1.Rows[i][j] is DateTime && tabla1.Columns[j].ExtendedProperties.ContainsKey("ProviderType"))
                    {
                        DateTime timeTable1 = (DateTime)tabla1.Rows[i][j];
                        TimeSpan onlyHour1 = timeTable1.TimeOfDay;
                        if (tabla2.Rows[i][j] == DBNull.Value)
                        {
                            badRows.Add(i);
                            break;
                        }
                        DateTime timeTable2 = (DateTime)tabla2.Rows[i][j];
                        TimeSpan onlyHour2 = timeTable2.TimeOfDay;
                        if (!onlyHour1.Equals(onlyHour2))
                        {
                            badRows.Add(i);
                            break;
                        }
                    }
                    else if (!tabla1.Rows[i][j].Equals(tabla2.Rows[i][j]))
                    {
                        badRows.Add(i);
                        break;
                    }

                }

                if (!paintGridView && badRows.Any())
                {
                    return false;
                }
                else if (!paintGridView)
                {
                    continue;
                }

                //painting function
                if (badRows.Contains(i))
                {
                    this.bgwDetails.ReportProgress(count,
                            new ComparationResult
                            {
                                StatusDetails = StatusDetails.ComparacionBad,
                                Index = i
                            }
                        );

                }
                else
                {
                    this.bgwDetails.ReportProgress(count,
                            new ComparationResult
                            {
                                StatusDetails = StatusDetails.ComparacionOk,
                                Index = i
                            }
                        );
                }

                num++;
            }

            return !badRows.Any();

        }

        private string GetFilterCondition(DataColumn column, object value)
        {
            if (value == DBNull.Value)
            {
                return $"{column.ColumnName} IS NULL";
            }
            else if (column.DataType == typeof(string) || column.DataType == typeof(DateTime))
            {
                // Tratar el valor como una cadena o fecha
                return $"{column.ColumnName} = '{value}'";
            }
            else
            {
                // Otros tipos de datos numéricos
                return $"{column.ColumnName} = {value}";
            }
        }


        // Método para comparar dos DataTables
        bool SyncDataFromTable(ItemDTO item, DataTable tabla1, DataTable tabla2, bool paintGridView)
        {
            string tableName = item.Table1;

            // si la tabla no existe se crea en el otro servidor.
            if (tabla2 == null)
            {
                string createTableSentence = this.dbManager1.GetCreateSchemaTable(tableName);
                this.dbManager2.ExecuteSentence(createTableSentence);
                tabla2 = this.dbManager2.GetTableData(tableName);
            }

            // Verificar si tienen la misma cantidad de columnas
            if (tabla1.Columns.Count != tabla2.Columns.Count)
            {
                throw new ApplicationException("Columnas diferentes");
            }

            // Verificar si tienen las mismas columnas y en el mismo orden
            for (int i = 0; i < tabla1.Columns.Count; i++)
            {
                if (tabla1.Columns[i].ColumnName != tabla2.Columns[i].ColumnName ||
                    tabla1.Columns[i].DataType != tabla2.Columns[i].DataType)
                {
                    throw new ApplicationException("Columnas en orden diferente");
                }
            }


            tabla2.PrimaryKey =
                (from DataColumn column in tabla2.Columns
                 where item.Table1Keys.Contains(column.ColumnName)
                 select column).ToArray();


            string[] columns = (from DataColumn column in tabla2.Columns
                                select column.ColumnName).ToArray();

            string prepareInsert = $"INSERT INTO {tableName} ({Environment.NewLine}"
                + string.Join($",{Environment.NewLine}", columns)
                + $" {Environment.NewLine}) "
                + $" VALUES({Environment.NewLine}"
                + string.Join($",{Environment.NewLine}", columns.Select((localValue, indice) => $"@p{indice}").ToArray())
                + ");";

            string[] colsToDelete = (from DataColumn column in tabla2.PrimaryKey
                                     select column.ColumnName).ToArray();


            string prepareDelete = $"DELETE FROM {tableName} {Environment.NewLine} WHERE {Environment.NewLine}"
                + string.Join($",{Environment.NewLine}", colsToDelete.Select((colName, indice) => $" {colName} = @p{indice}").ToArray())
                + ";";

            List<int> badRows = new List<int>();
            int percentajeLocalTable = 0;

            try
            {
                this.dbManager1.OpenConnection();
                this.dbManager2.OpenConnection();

                for (int i = 0; i < tabla1.Rows.Count; i++)
                {
                    //coco
                    int count = percentajeLocalTable * 100 / tabla1.Rows.Count;
                    DataRow rowTable1 = tabla1.Rows[i];

                    // Definir los valores de las columnas que conforman la clave primaria
                    object[] valuesWithPrimaryKey;

                    DataRow rowData;

                    // si la tabla no tiene llave primaria se busca todos los datos del registro
                    if (tabla2.PrimaryKey.Any())
                    {
                        valuesWithPrimaryKey = (from DataColumn column in tabla2.PrimaryKey
                                                select rowTable1[column.ColumnName]).ToArray();
                        // Encontrar la fila con la clave primaria compuesta
                        rowData = tabla2.Rows.Find(valuesWithPrimaryKey);
                    }
                    else
                    {
                        valuesWithPrimaryKey = (from DataColumn column in tabla2.Columns
                                                select rowTable1[column.ColumnName]).ToArray();

                        string[] filterValues = (from DataColumn column in tabla2.Columns
                                                 select GetFilterCondition(column, rowTable1[column.ColumnName])).ToArray();

                        string filter = string.Join($"{Environment.NewLine} AND ", filterValues);



                        rowData = tabla2.Select(filter).FirstOrDefault();
                    }




                    // si no encuentra el registro, lo inserta
                    if (rowData == null)
                    {
                        object[] dataToInsert = (from DataColumn column in tabla2.Columns
                                                 select
                                                 column.DataType == typeof(DateTime) && column.ExtendedProperties.Count > 0
                                                 ? ExtractTime(rowTable1[column.ColumnName])
                                                  :
                                                 rowTable1[column.ColumnName]
                                                     ).ToArray();


                        this.dbManager2.ExecuteRecord(prepareInsert, dataToInsert);

                    }
                    // se comparan los datos y si alguno no coincide se borra el registro y se inserta
                    else
                    {
                        bool equalDataRecord = true;

                        for (int j = 0; j < tabla1.Columns.Count; j++)
                        {
                            if (rowTable1[j] is byte[])
                            {
                                if (!((byte[])rowTable1[j]).SequenceEqual((byte[])rowData[j]))
                                {
                                    equalDataRecord = false;
                                    break;
                                }
                            }
                            else if (rowTable1[j] is DateTime && tabla1.Columns[j].ExtendedProperties.ContainsKey("ProviderType"))
                            {

                                DateTime timeTable1 = (DateTime)rowTable1[j];
                                TimeSpan onlyHour1 = timeTable1.TimeOfDay;
                                if (rowData[j] == DBNull.Value)
                                {
                                    equalDataRecord = false;
                                    break;
                                }
                                DateTime timeTable2 = (DateTime)rowData[j];
                                TimeSpan onlyHour2 = timeTable2.TimeOfDay;
                                if (!onlyHour1.Equals(onlyHour2))
                                {
                                    equalDataRecord = false;
                                    break;
                                }


                            }
                            else if (!rowTable1[j].Equals(rowData[j]))
                            {
                                equalDataRecord = false;
                                break;
                            }
                        }

                        if (!equalDataRecord)
                        {
                            object[] dataToInsert = (from DataColumn column in tabla2.Columns
                                                     select
                                                     column.DataType == typeof(DateTime) && column.ExtendedProperties.Count > 0
                                                     ? ExtractTime(rowTable1[column.ColumnName])
                                                      :
                                                     rowTable1[column.ColumnName]
                                                     ).ToArray();

                            this.dbManager2.ExecuteRecord(prepareDelete, valuesWithPrimaryKey);
                            this.dbManager2.ExecuteRecord(prepareInsert, dataToInsert);

                        }

                    }

                    // para aumentar rendimiento se grafica cada 1000 registros
                    if (paintGridView && i % 1000 == 0)
                    {
                        this.bgwSync.ReportProgress(count,
                                new SyncResult
                                {
                                    Item = item,
                                    StatusSync = StatusSync.SyncRow,
                                    Index = i
                                }
                            );
                    }

                    percentajeLocalTable++;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                this.dbManager1.CloseConnection();
                this.dbManager2.CloseConnection();

            }

            return !badRows.Any();

        }

        private object ExtractTime(object theTime)
        {
            try
            {
                if (theTime == DBNull.Value)
                    return theTime;
                //14960
                DateTime time = (DateTime)theTime;
                TimeSpan onlyHour = time.TimeOfDay;
                return onlyHour;
            }
            catch (Exception)
            {
                Console.WriteLine("Error en registro ");
                throw;
            }

        }

        private void btnSync_Click(object sender, EventArgs e)
        {
            if (this.btnSync.Text == "Cancelar")
            {
                if (this.bgwSync.IsBusy)
                {
                    this.bgwSync.CancelAsync();
                }
                return;
            }

            this.btnSync.Text = "Cancelar";
            this.bgwSync.RunWorkerAsync();
        }

        private void SyncData()
        {

            try
            {

                ListViewItem item = lvDetailTables.SelectedItems[0];

                //this.lblRecords1.Text = "Registros: 0";
                //this.lblRecords2.Text = "Registros: 0";

                ItemDTO itemDTO1 = string.IsNullOrEmpty(item.SubItems[(int)ColIndexComparer.Tabla1].Text) ? null : this.totalData.Where(i => i.Table1 == item.SubItems[(int)ColIndexComparer.Tabla1].Text).SingleOrDefault();
                ItemDTO itemDTO2 = string.IsNullOrEmpty(item.SubItems[(int)ColIndexComparer.Tabla2].Text) ? null : this.totalData.Where(i => i.Table2 == item.SubItems[(int)ColIndexComparer.Tabla2].Text).SingleOrDefault();

                DataTable tabla1 = null;
                //DataTable tabla2 = null;
                if (itemDTO1 != null)
                {
                    tabla1 = this.dbManager1.GetTableData(itemDTO1.Table1);
                    this.dbManager2.SincronizeTable(tabla1, itemDTO1.Table1);
                    //this.lblRecords1.Text = "Registros: " + tabla1.Rows.Count;
                }
                /*
                if (itemDTO2 != null)
                {
                    tabla2 = this.dbManager2.GetTableData(itemDTO2.Table2);
                    this.lblRecords2.Text = "Registros: " + tabla2.Rows.Count;
                }
                */
                //this.dataGridViewer1.DataSource = tabla1;
                //this.dataGridViewer2.DataSource = tabla2;
            }
            catch (Exception ex)
            {
                MessageBox.Show("Error sincronizando " + ex.Message, "Error de sincronización", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }

        }


        private void dataGridViewer1_DataError(object sender, DataGridViewDataErrorEventArgs e)
        {
            if (e.Exception is ArgumentException)
                Debug.Print(e.Exception.Message);
            else
                throw e.Exception;
        }

        private void dataGridViewer2_DataError(object sender, DataGridViewDataErrorEventArgs e)
        {
            if (e.Exception is ArgumentException)
                Debug.Print(e.Exception.Message);
            else
                throw e.Exception;
        }

        private void chkDataFilter_CheckedChanged(object sender, EventArgs e)
        {
            this.lvDetailTables.Items.Clear();
            int i = 1;
            foreach (ItemDTO item in this.totalData)
            {
                string[] filaDatos = new string[] { null, (i++).ToString(), item.Table1,
                    item.CountTable1.ToString(), item.Table1Size, item.Table2, item.CountTable2.ToString() , item.Table2Size };

                if (this.chkDataEqual.Checked && item.EqualData.HasValue && item.EqualData.Value)
                {
                    ListViewItem lv = new ListViewItem(filaDatos, (int)Utils.Status.Equal);
                    lv.Checked = true;
                    lvDetailTables.Items.Add(lv);
                }
                else if (this.chkDataNotEqual.Checked && item.EqualData.HasValue && !item.EqualData.Value)
                {
                    ListViewItem lv = new ListViewItem(filaDatos, (int)Utils.Status.NotEqual);
                    lv.Checked = true;
                    lvDetailTables.Items.Add(lv);
                }
                else if (!item.EqualData.HasValue)
                {
                    ListViewItem lv = new ListViewItem(filaDatos, (int)Utils.Status.Cargando);
                    lv.Checked = true;
                    lvDetailTables.Items.Add(lv);
                }
            }

        }

        private void timerDuration_Tick(object sender, EventArgs e)
        {
            // Calcular el tiempo transcurrido
            TimeSpan elapsed = DateTime.Now - startTime;
            string time = $"Tiempo transcurrido: {elapsed.Hours:D2}:{elapsed.Minutes:D2}:{elapsed.Seconds:D2}";

            if (this.bgwSync.IsBusy)
            {
                // Actualizar la interfaz de usuario con el tiempo transcurrido
                // Por ejemplo, mostrar en un Label
                this.lblElapsedSync.Visible = true;
                this.lblElapsedSync.Text = time;

            }
            else
            {
                this.lblElapsed.Visible = true;
                this.lblElapsed.Text = time;

            }

        }

        private void bgwDetails_DoWork(object sender, DoWorkEventArgs e)
        {
            this.FillDetails(e);

            if (e.Cancel)
            {
                return;
            }
            this.bgwDetails.ReportProgress(100);
        }




        private void bgwDetails_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            this.pgbDetails.Value = e.ProgressPercentage;
            if (e.UserState == null)
                return;

            this.lblStatusDetails.Visible = true;

            if (e.UserState is StatusDetails)
            {
                StatusDetails statusDetails = (StatusDetails)e.UserState;
                switch (statusDetails)
                {
                    case StatusDetails.CargandoData1:
                        this.resumedTable1 = null;
                        this.resumedTable2 = null;
                        this.lblStatusDetails.Text = "Cargando tabla origen...";
                        break;
                    case StatusDetails.CargadoData1:
                        this.lblRecords1.Text = "Registros: " + (this.filledTable1 == null ? 0 : this.filledTable1.Rows.Count);
                        break;
                    case StatusDetails.CargandoData2:
                        this.lblStatusDetails.Text = "Cargando tabla destino...";
                        break;
                    case StatusDetails.CargaCompleta:
                        this.lblRecords2.Text = "Registros: " + (this.filledTable2 == null ? 0 : this.filledTable2.Rows.Count);
                        break;
                    case StatusDetails.RefreshGrid:
                        this.lblStatusDetails.Text = "Datos cargados";
                        if (filledTable1 != null && filledTable1.Rows.Count <= 1000)
                        {
                            this.resumedTable1 = null;
                            this.dataGridViewer1.DataSource = filledTable1;
                        }
                        else if (filledTable1 != null && this.resumedTable1 == null)
                        {
                            this.resumedTable1 = this.filledTable1.Clone();
                            this.dataGridViewer1.DataSource = this.resumedTable1;
                        }

                        if (filledTable2 != null && filledTable2.Rows.Count <= 1000)
                        {
                            this.resumedTable1 = null;
                            this.dataGridViewer2.DataSource = filledTable2;
                        }
                        else if (filledTable2 != null && this.resumedTable2 == null)
                        {
                            this.resumedTable2 = this.filledTable2.Clone();
                            this.dataGridViewer2.DataSource = this.resumedTable2;
                        }

                        this.dataGridViewer1.Refresh();
                        this.dataGridViewer2.Refresh();
                        break;
                    case StatusDetails.ComparacionFinished:
                        this.lblStatusDetails.Text = "Comparación finalizada";
                        break;
                    default:
                        break;
                }
            }
            else if (e.UserState is ComparationResult)
            {
                ComparationResult comparationResult = (ComparationResult)e.UserState;
                switch (comparationResult.StatusDetails)
                {
                    case StatusDetails.ComparacionBad:
                        if (filledTable1 != null && filledTable1.Rows.Count <= 1000)
                        {
                            this.dataGridViewer1.Rows[comparationResult.Index].DefaultCellStyle.BackColor = Color.Orange;
                        }
                        else if (this.resumedTable1 != null && this.resumedTable1.Rows.Count < 1000)
                        {
                            DataRow newRow = this.resumedTable1.NewRow();
                            newRow.ItemArray = filledTable1.Rows[comparationResult.Index].ItemArray; // copia los valores de la fila
                            this.resumedTable1.Rows.Add(newRow);

                            this.dataGridViewer1.Rows[comparationResult.Index].DefaultCellStyle.BackColor = Color.Orange;
                        }

                        if (filledTable2 != null && filledTable2.Rows.Count <= 1000)
                        {
                            this.dataGridViewer2.Rows[comparationResult.Index].DefaultCellStyle.BackColor = Color.Orange;
                        }
                        else if (this.resumedTable2 != null && this.resumedTable2.Rows.Count < 1000)
                        {
                            DataRow newRow = this.resumedTable2.NewRow();
                            newRow.ItemArray = filledTable2.Rows[comparationResult.Index].ItemArray; // copia los valores de la fila
                            this.resumedTable2.Rows.Add(newRow);

                            this.dataGridViewer2.Rows[comparationResult.Index].DefaultCellStyle.BackColor = Color.Orange;
                        }

                        break;
                    case StatusDetails.ComparacionOk:
                        if (filledTable1 != null && filledTable1.Rows.Count <= 1000)
                        {
                            this.dataGridViewer1.Rows[comparationResult.Index].DefaultCellStyle.BackColor = Color.Green;
                            this.dataGridViewer1.Rows[comparationResult.Index].DefaultCellStyle.BackColor = Color.Green;
                            this.dataGridViewer1.Rows[comparationResult.Index].DefaultCellStyle.BackColor = Color.Green;
                        }
                        else if (this.resumedTable1 != null && this.resumedTable1.Rows.Count < 1000)
                        {
                            DataRow newRow = this.resumedTable1.NewRow();
                            newRow.ItemArray = filledTable1.Rows[comparationResult.Index].ItemArray; // copia los valores de la fila
                            this.resumedTable1.Rows.Add(newRow);

                            this.dataGridViewer1.Rows[comparationResult.Index].DefaultCellStyle.BackColor = Color.Green;
                            this.dataGridViewer1.Rows[comparationResult.Index].DefaultCellStyle.BackColor = Color.Green;
                            this.dataGridViewer1.Rows[comparationResult.Index].DefaultCellStyle.BackColor = Color.Green;

                        }

                        if (filledTable2 != null && filledTable2.Rows.Count <= 1000)
                        {
                            this.dataGridViewer2.Rows[comparationResult.Index].DefaultCellStyle.BackColor = Color.Green;
                            this.dataGridViewer2.Rows[comparationResult.Index].DefaultCellStyle.BackColor = Color.Green;
                            this.dataGridViewer2.Rows[comparationResult.Index].DefaultCellStyle.BackColor = Color.Green;
                        }
                        else if (this.resumedTable2 != null && this.resumedTable2.Rows.Count < 1000)
                        {
                            DataRow newRow = this.resumedTable2.NewRow();
                            newRow.ItemArray = filledTable2.Rows[comparationResult.Index].ItemArray; // copia los valores de la fila
                            this.resumedTable2.Rows.Add(newRow);

                            this.dataGridViewer2.Rows[comparationResult.Index].DefaultCellStyle.BackColor = Color.Green;
                            this.dataGridViewer2.Rows[comparationResult.Index].DefaultCellStyle.BackColor = Color.Green;
                            this.dataGridViewer2.Rows[comparationResult.Index].DefaultCellStyle.BackColor = Color.Green;

                        }
                        break;
                    default:
                        break;
                }
            }
        }

        private void bgwDetails_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {

            //this.timerDuration.Stop();
            //this.btnComparar.Text = "Comparar";
            if (e.Cancelled)
            {
                //this.lblStatusProgress.Text = "Comparación cancelada";
                MessageBox.Show("El proceso de comparación ha sido cancelado", "Sisnet", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                return;
            }
            if (e.Error != null)
            {
                //this.lblStatusProgress.Text = "Error conteo";
                String message = e.Error.Message;
                /*message += e.Error.InnerException != null ? "\r\n[" + e.Error.InnerException.Message + "]" : string.Empty;
                message += e.Error.InnerException != null ? "\r\n" + e.Error.InnerException.StackTrace + "\r\n\r\n" : string.Empty;
                message += "\r\n" + e.Error.StackTrace;
                */
                MessageBox.Show("Error ejecutando la comparación.\r\n" + message, "Sisnet", MessageBoxButtons.OK, MessageBoxIcon.Hand);
                return;
            }

            this.lblInfoTable1.ForeColor = this.dataEquals ? Color.Green : Color.Red;
            this.lblInfoTable2.ForeColor = this.dataEquals ? Color.Green : Color.Red;

        }


        private void bgwSync_DoWork(object sender, DoWorkEventArgs e)
        {
            this.FillSyncData(e);

            if (e.Cancel)
            {
                return;
            }
            this.bgwSync.ReportProgress(100);
        }




        private void bgwSync_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            this.pgbDetails.Value = e.ProgressPercentage;
            if (e.UserState == null)
                return;

            this.lblStatusDetails.Visible = true;

            if (e.UserState is StatusSync)
            {
                StatusSync statusSync = (StatusSync)e.UserState;
                switch (statusSync)
                {
                    case StatusSync.SyncDataStart:
                        this.lblStatusDetails.Text = "Iniciando sincronizaciión";
                        this.pgbDetails.Value = 0;
                        this.lblElapsedSync.Text = string.Empty;
                        this.startTime = DateTime.Now;
                        // Iniciar el Timer
                        this.timerDuration.Start();

                        break;
                    case StatusSync.SyncDataFinished:
                        this.lblStatusDetails.Text = "Sincronización finalizada";
                        break;
                    case StatusSync.NoDataToSync:
                        MessageBox.Show("No hay diferencias para sincronizar", "Sincronización", MessageBoxButtons.OK, MessageBoxIcon.Information);
                        break;
                    default:
                        break;
                }
            }
            else if (e.UserState is SyncResult)
            {
                SyncResult syncResult = (SyncResult)e.UserState;
                switch (syncResult.StatusSync)
                {
                    case StatusSync.LoadingTables:
                        this.lblStatusDetails.Text = $"Cargando tabla {syncResult.Item.Table1}";
                        break;
                    case StatusSync.SyncMovingData:
                        this.lblStatusDetails.Text = $"Moviendo datos tabla {syncResult.Item.Table1}";
                        break;
                    case StatusSync.SyncRow:
                        this.lblStatusDetails.Text = $"{syncResult.Item.Table1} {syncResult.Index}";
                        break;
                    case StatusSync.SyncFinishTable:
                        this.lblStatusDetails.Text = $"Tabla sincronizada {syncResult.Item.Table1}";
                        this.EnsureVisibleDeitalSync(syncResult.Index);
                        break;
                    default:
                        break;
                }
            }
        }

        private void EnsureVisibleDeitalSync(int index) {
            ListViewItem viewItem = (from ListViewItem item
                in this.lvComparer.Items
                                     where string.Compare(index.ToString(), item.SubItems[(int)ColIndexDetail.Index].Text) == 0
                                     select item).Single();

            viewItem.ImageIndex = (int)Utils.Status.Verde;
            viewItem.EnsureVisible();

        }

        private void bgwSync_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {

            this.timerDuration.Stop();
            this.btnSync.Text = "Sincronizar";

            if (e.Cancelled)
            {
                //this.lblStatusProgress.Text = "Comparación cancelada";
                MessageBox.Show("El proceso de sincronización ha sido cancelado", "Sisnet", MessageBoxButtons.OK, MessageBoxIcon.Exclamation);
                return;
            }
            if (e.Error != null)
            {
                //this.lblStatusProgress.Text = "Error conteo";
                String message = e.Error.Message;
                /*message += e.Error.InnerException != null ? "\r\n[" + e.Error.InnerException.Message + "]" : string.Empty;
                message += e.Error.InnerException != null ? "\r\n" + e.Error.InnerException.StackTrace + "\r\n\r\n" : string.Empty;
                message += "\r\n" + e.Error.StackTrace;
                */
                MessageBox.Show("Error ejecutando la sincronización.\r\n" + message, "Sisnet", MessageBoxButtons.OK, MessageBoxIcon.Hand);
                return;
            }

            MessageBox.Show("Sincronización completada", "Sisnet", MessageBoxButtons.OK, MessageBoxIcon.Information);

        }

        private void lvComparer_ColumnClick(object sender, ColumnClickEventArgs e)
        {
            // Obtener el comparador actual
            ListViewItemComparer comparer = (ListViewItemComparer)((System.Windows.Forms.ListView)sender).ListViewItemSorter;

            // Si es null o es para una columna diferente, crear un nuevo comparador
            if (comparer == null || comparer.Column != e.Column)
            {
                comparer = new ListViewItemComparer(e.Column, this.totalData, SortOrder.Ascending);
            }
            else
            {
                // Si es para la misma columna, cambiar el orden
                comparer.ToggleSortOrder();
            }

            // Asignar el comparador al ListView
            ((System.Windows.Forms.ListView)sender).ListViewItemSorter = null;
            // Actualizar la vista
            //((System.Windows.Forms.ListView)sender).Refresh();
            ((System.Windows.Forms.ListView)sender).ListViewItemSorter = comparer;
        }
    }

}

