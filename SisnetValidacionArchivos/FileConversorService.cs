using System;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Configuration;
using System.Runtime.CompilerServices;
using System.ServiceProcess;
using System.Threading;

namespace SisnetServiceConversor
{
    public partial class FileConversorService : ServiceBase
    {
        private Thread thread;

        private FileConversor ff;

        private int SecondsToReadDatabase;

        //private IContainer components;

        protected bool StopSignal
        {
            get;
            private set;
        }

        public FileConversorService()
        {
            this.InitializeComponent();
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
                Thread.Sleep(this.SecondsToReadDatabase * 1000);
            }
        }

        public void Start(string[] args)
        {
            this.Run();
        }
    }
}