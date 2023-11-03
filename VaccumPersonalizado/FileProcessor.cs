using SisnetData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Configuration;

namespace vaccumPersonalizado
{
    internal class FileProcessor
    {
        private String LeerArchivo(String filePath) {

            try
            {
                string currentDirectory = this.GetCurrentDirectory();
                currentDirectory = string.Concat(currentDirectory, "filePath");

                if (File.Exists(filePath))
                {
                    string fileContent = File.ReadAllText(filePath);
                    Console.WriteLine($"Contenido del archivo {filePath}:\n{fileContent}");
                    return fileContent;
                }
                else
                {
                    throw new ApplicationException($"El archivo {filePath} no existe.");
                }
            }
            catch (Exception ex)
            {
                throw new ApplicationException($"Error al leer el archivo o conectar a la base de datos: {ex.Message}");
            }
        }

        internal void ProcesarArchivo(string databaseName, string filePath)
        {


            try
            {
                DBManager dBManager = DBManager.GetDBManager();

                try
                {
                    string appSetting1 = ConfigurationManager.AppSettings["server"];
                    string appSetting2 = databaseName;
                    string appSetting3 = ConfigurationManager.AppSettings["user"];
                    string appSetting4 = ConfigurationManager.AppSettings["password"];
                    dBManager.SetDBManager(appSetting1, appSetting2, appSetting3, appSetting4);
                }
                catch (Exception ex)
                {
#if DEBUG
                    /*if (ex.Message.Contains("Failed"))
                    {
                        testLocal = true;
                    }*/
#else
                this.WriteLog(ex.Message);
                throw ex;
#endif
                }

                String fileContent = this.LeerArchivo(filePath);
                String[] tablas = fileContent.Split("\r\n".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
                foreach (String table in tablas)
                {
                    dBManager.RecreateTable(table);
                }


            }
            catch (Exception ex)
            {
                this.WriteLog(ex.Message, ex);
            }

        }
        private bool log;

        private void WriteLog(string text, Exception ex = null)
        {
            if (!this.log || !text.EndsWith("ms"))
            {
                return;
            }
            string currentDirectory = this.GetCurrentDirectory();
            currentDirectory = string.Concat(currentDirectory, "log.txt");
            DateTime now = DateTime.Now;
            File.AppendAllText(currentDirectory, string.Concat(now.ToString("yyyy-MM-dd HH:mm:ss"), " ", text, "\r\n"));
            if (ex != null)
            {
                now = DateTime.Now;
                File.AppendAllText(currentDirectory, string.Concat(now.ToString("yyyy-MM-dd HH:mm:ss"), " ", ex.StackTrace, "\r\n"));
            }
        }

        private string GetCurrentDirectory()
        {
            return AppDomain.CurrentDomain.BaseDirectory;
        }
    }
}
