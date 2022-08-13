using SisnetServiceConversor;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SisnetValidacionArchivos
{
    public partial class FileConversorService : ServiceBase
    {
        // Fields
        private Thread thread;
        private FileConversor ff;
        private int SecondsToReadDatabase;
        //private IContainer components;
        // Properties
        protected bool StopSignal { get; private set; }
        public FileConversorService()
        {
            InitializeComponent();
            this.ff = new FileConversor(base.ServiceHandle);
            this.SecondsToReadDatabase = int.Parse(ConfigurationManager.AppSettings["secondsToReadDatabase"]);
        }

        protected override void OnStart(string[] args)
        {
            ThreadStart start = new ThreadStart(this.Run);
            this.thread = new Thread(start);
            this.thread.Start();

        }

        protected override void OnStop()
        {
            this.StopSignal = true;
            this.ff.StopSignal = true;

        }
        private void Run()
        {
            while (!this.StopSignal)
            {
                this.ff.ChangeData();
                Thread.Sleep((int)(this.SecondsToReadDatabase * 0x3e8));
            }
        }
        public void Start(string[] args)
        {
            this.Run();
        }
    }
}
