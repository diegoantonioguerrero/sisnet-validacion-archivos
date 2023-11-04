using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using SisnetData;

namespace SisnetExportador
{
    public partial class FrmConexionG : Form
    {
        private bool conexionExitosa;

        private Label label1;

        private Label label2;

        private Label label3;

        private Label label4;

        private TextBox txtIp;

        private TextBox txtBD;

        private TextBox txtUsuario;

        private TextBox txtClave;

        private Button btnConexion;

        private PictureBox pictureBox1;


        public FrmConexionG()
        {
            InitializeComponent();
            InitializeComponentTotal();
        }

        private void InitializeComponentTotal()
        {
            ComponentResourceManager componentResourceManager = new ComponentResourceManager(typeof(FrmConexionG));
            this.label1 = new Label();
            this.label2 = new Label();
            this.label3 = new Label();
            this.label4 = new Label();
            this.txtIp = new TextBox();
            this.txtBD = new TextBox();
            this.txtUsuario = new TextBox();
            this.txtClave = new TextBox();
            this.btnConexion = new Button();
            this.pictureBox1 = new PictureBox();
            ((ISupportInitialize)this.pictureBox1).BeginInit();
            base.SuspendLayout();
            this.label1.AutoSize = true;
            this.label1.Location = new Point(12, 137);
            this.label1.Name = "label1";
            this.label1.Size = new Size(107, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "Nombre/Direccion IP";
            this.label2.AutoSize = true;
            this.label2.Location = new Point(44, 165);
            this.label2.Name = "label2";
            this.label2.Size = new Size(75, 13);
            this.label2.TabIndex = 1;
            this.label2.Text = "Base de datos";
            this.label3.AutoSize = true;
            this.label3.Location = new Point(76, 193);
            this.label3.Name = "label3";
            this.label3.Size = new Size(43, 13);
            this.label3.TabIndex = 2;
            this.label3.Text = "Usuario";
            this.label4.AutoSize = true;
            this.label4.Location = new Point(58, 221);
            this.label4.Name = "label4";
            this.label4.Size = new Size(61, 13);
            this.label4.TabIndex = 3;
            this.label4.Text = "Contraseña";
            this.txtIp.Location = new Point(126, 137);
            this.txtIp.Name = "txtIp";
            this.txtIp.Size = new Size(152, 20);
            this.txtIp.TabIndex = 4;
            this.txtIp.Text = "localhost";
            this.txtBD.Location = new Point(126, 165);
            this.txtBD.Name = "txtBD";
            this.txtBD.Size = new Size(152, 20);
            this.txtBD.TabIndex = 5;
            this.txtBD.Text = "sisnetappweb";
            this.txtUsuario.Location = new Point(126, 193);
            this.txtUsuario.Name = "txtUsuario";
            this.txtUsuario.Size = new Size(152, 20);
            this.txtUsuario.TabIndex = 6;
            this.txtUsuario.Text = "postgres";
            this.txtClave.Location = new Point(126, 221);
            this.txtClave.Name = "txtClave";
            this.txtClave.PasswordChar = '*';
            this.txtClave.Size = new Size(152, 20);
            this.txtClave.TabIndex = 7;
            this.txtClave.Text = "postgres";
            this.btnConexion.Location = new Point(202, 264);
            this.btnConexion.Name = "btnConexion";
            this.btnConexion.Size = new Size(75, 23);
            this.btnConexion.TabIndex = 8;
            this.btnConexion.Text = "Conectar";
            this.btnConexion.UseVisualStyleBackColor = true;
            this.btnConexion.Click += new EventHandler(this.btnConexion_Click);
            this.pictureBox1.Image = (Image)componentResourceManager.GetObject("pictureBox1.Image");
            this.pictureBox1.Location = new Point(40, 13);
            this.pictureBox1.Name = "pictureBox1";
            this.pictureBox1.Size = new Size(213, 100);
            this.pictureBox1.SizeMode = PictureBoxSizeMode.AutoSize;
            this.pictureBox1.TabIndex = 9;
            this.pictureBox1.TabStop = false;
            base.AcceptButton = this.btnConexion;
            base.AutoScaleDimensions = new SizeF(6f, 13f);
            base.AutoScaleMode = AutoScaleMode.Font;
            base.ClientSize = new Size(293, 315);
            base.Controls.Add(this.pictureBox1);
            base.Controls.Add(this.btnConexion);
            base.Controls.Add(this.txtClave);
            base.Controls.Add(this.txtUsuario);
            base.Controls.Add(this.txtBD);
            base.Controls.Add(this.txtIp);
            base.Controls.Add(this.label4);
            base.Controls.Add(this.label3);
            base.Controls.Add(this.label2);
            base.Controls.Add(this.label1);
            base.Icon = (Icon)componentResourceManager.GetObject("$this.Icon");
            base.MaximizeBox = false;
            base.Name = "FrmConexion";
            base.StartPosition = FormStartPosition.CenterScreen;
            ((ISupportInitialize)this.pictureBox1).EndInit();
            base.ResumeLayout(false);
            base.PerformLayout();
        }

        #region methods

        private void btnConexion_Click(object sender, EventArgs e)
        {
            DBManager dBManager = DBManager.GetDBManager();
            try
            {
                dBManager.SetDBManager(this.txtIp.Text, this.txtBD.Text, this.txtUsuario.Text, this.txtClave.Text);
                this.conexionExitosa = true;
                base.Hide();
                try
                {
                    (new FrmDataExtractor()).ShowDialog();
                    base.Close();
                }
                catch (Exception exception1)
                {
                    Exception exception = exception1;
                    base.Show();
                    MessageBox.Show(string.Concat("Ocurrio un error inesperado ", exception.Message), "Error inseperado", MessageBoxButtons.OK, MessageBoxIcon.Hand);
                }
            }
            catch (Exception exception2)
            {
                MessageBox.Show(exception2.Message, "Error de conexión", MessageBoxButtons.OK, MessageBoxIcon.Hand);
            }
        }

        private void FrmConexion_FormClosing(object sender, FormClosingEventArgs e)
        {
            if (this.conexionExitosa)
            {
                return;
            }
            if (e.CloseReason == CloseReason.UserClosing)
            {
                DialogResult obj = MessageBox.Show("Realmente desea salir?", "Sisnet - Extractor de archivos", MessageBoxButtons.YesNo, MessageBoxIcon.Question);
                if (obj == DialogResult.Yes)
                {
                    Application.Exit();
                }
                if (obj == DialogResult.No)
                {
                    e.Cancel = true;
                }
            }
        }

        #endregion



    }
}
