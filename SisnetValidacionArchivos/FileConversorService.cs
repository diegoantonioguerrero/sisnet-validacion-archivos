using SisnetValidacionArchivos;
using System;
using System.Configuration;
using System.IO;
using System.ServiceProcess;
using System.Threading;

namespace SisnetServiceConversor
{
    public partial class FileConversorService : ServiceBase
    {
        private Thread thread;

        private FileConversor ff;

        private int SecondsToReadDatabase;

        private bool log;

        //private IContainer components;

        protected bool StopSignal
        {
            get;
            private set;
        }

        public FileConversorService()
        {
            //this.InitializeComponent();
            this.ff = new FileConversor(base.ServiceHandle);
            this.SecondsToReadDatabase = int.Parse(ConfigurationManager.AppSettings["secondsToReadDatabase"]);
            this.log = bool.Parse(ConfigurationManager.AppSettings["log"]);
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
            try
            {
                while (!this.StopSignal)
                {
                    var databaseSettings = (DatabaseSettings)ConfigurationManager.GetSection("databaseSettings");

                    foreach (var database in databaseSettings.Databases)
                    {
                        this.ff.SetDbManager(database);
                        this.ff.ChangeData();
                    }


                    Thread.Sleep(this.SecondsToReadDatabase * 1000);

                }
            }
            catch (Exception ex) {
                this.WriteLog("Error fatal inesperado: " + ex.Message, ex);
            }
        }

        public void Start(string[] args)
        {
            this.Run();
        }

        private void WriteLog(string text, Exception ex = null)
        {
            if (!this.log)
            {
                return;
            }
            string currentDirectory = FileConversor.GetCurrentDirectory();
            currentDirectory = string.Concat(currentDirectory, "log.txt");
            DateTime now = DateTime.Now;
            File.AppendAllText(currentDirectory, string.Concat(now.ToString("yyyy-MM-dd HH:mm:ss"), " ", text, "\r\n"));
            if (ex != null)
            {
                now = DateTime.Now;
                File.AppendAllText(currentDirectory, string.Concat(now.ToString("yyyy-MM-dd HH:mm:ss"), " ", ex.StackTrace, "\r\n"));
            }
        }
    }
}