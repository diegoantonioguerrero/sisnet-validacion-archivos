// Decompiled with JetBrains decompiler
// Type: SisnetServiceConversor.FileConversor
// Assembly: SisnetValidacionArchivos, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null
// MVID: 1C982BFB-D950-4E36-96EC-B870E1BEF3A5
// Assembly location: C:\tempStore\sisnetdataexe\SisnetValidacionArchivos.exe

using SisnetData;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace SisnetServiceConversor
{
    public class FileConversor
    {
        private string tableToValidate;
        private string imgLogo;
        private string cmdWaterMark;
        private string PdfExe;
        private string PdfCompresorExe;
        private int secondsToPressContinue2PDF;
        private List<SisnetData.ProcessInfo> globalData;
        private string WorkingPath;
        private string cmdConvertPDF;
        private string cmdCompresorPDF;
        private string cmdTextWaterMark1;
        private string cmdTextWaterMark2;
        private string cmdTextWaterMark3;
        private string cmdTextWaterMark4;
        private string pdfexeclauncher;
        private string extensiones;
        private bool log;
        public bool StopSignal;
        private IntPtr ServiceHandle;
        private Process proc;
#if DEBUG
        bool testLocal;
#endif
        [DllImport("user32.dll", SetLastError = true)]
        public static extern IntPtr SetParent(IntPtr hWndChild, IntPtr hWndNewParent);

        [DllImport("user32.dll")]
        public static extern bool SetForegroundWindow(IntPtr hWnd);

        public FileConversor(IntPtr ServiceHandle)
        {
            this.ServiceHandle = ServiceHandle;
            DBManager dbManager = DBManager.GetDBManager();

            try
            {
                string appSetting1 = ConfigurationManager.AppSettings["server"];
                string appSetting2 = ConfigurationManager.AppSettings["database"];
                string appSetting3 = ConfigurationManager.AppSettings["user"];
                string appSetting4 = ConfigurationManager.AppSettings["password"];
                dbManager.SetDBManager(appSetting1, appSetting2, appSetting3, appSetting4);
            }
            catch (Exception ex)
            {
#if DEBUG
                if (ex.Message.Contains("Failed"))
                {
                    testLocal = true;
                }
#else
                this.WriteLog(ex.Message);
                throw ex;
#endif


            }
            this.WorkingPath = this.GetCurrentDirectory() + "temp";
            this.tableToValidate = ConfigurationManager.AppSettings[nameof(tableToValidate)];
            this.PdfExe = ConfigurationManager.AppSettings["2pdfexe"];
            this.PdfCompresorExe = ConfigurationManager.AppSettings["pdfcompresorexe"];
            this.imgLogo = ConfigurationManager.AppSettings["logoWaterMark"];
            this.imgLogo = this.GetCurrentDirectory() + this.imgLogo;
            this.cmdConvertPDF = ConfigurationManager.AppSettings[nameof(cmdConvertPDF)];
            this.cmdCompresorPDF = ConfigurationManager.AppSettings[nameof(cmdCompresorPDF)];
            this.cmdWaterMark = ConfigurationManager.AppSettings[nameof(cmdWaterMark)];
            this.cmdTextWaterMark1 = ConfigurationManager.AppSettings[nameof(cmdTextWaterMark1)];
            this.cmdTextWaterMark2 = ConfigurationManager.AppSettings[nameof(cmdTextWaterMark2)];
            this.cmdTextWaterMark3 = ConfigurationManager.AppSettings[nameof(cmdTextWaterMark3)];
            this.cmdTextWaterMark4 = ConfigurationManager.AppSettings[nameof(cmdTextWaterMark4)];
            this.pdfexeclauncher = ConfigurationManager.AppSettings["2pdfexeclauncher"];
            this.secondsToPressContinue2PDF = int.Parse(ConfigurationManager.AppSettings[nameof(secondsToPressContinue2PDF)]);
            this.log = bool.Parse(ConfigurationManager.AppSettings["log"]);
            this.extensiones = ConfigurationManager.AppSettings["extensiones"];
        }

        public void ChangeData()
        {
            try
            {
                this.globalData = DBManager.GetDBManager().GetPendingFiles(this.tableToValidate.ToString());
                if (this.globalData.Count == 0)
                {
                    this.WriteLog("No hay archivos con estado PE");
                }
                else {
                    this.WriteLog("Se procesan " + this.globalData.Count + " archivos con estado PE");
                }
                this.ExportData();
            }
            catch (Exception ex)
            {
                this.WriteLog(ex.Message, ex);
            }
        }

        private void ExportData()
        {
            if (this.globalData.Count == 0)
                return;
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            DBManager dbManager = DBManager.GetDBManager();
            List<string> stringList = new List<string>();
            List<string> list1 = this.globalData.Select<SisnetData.ProcessInfo, string>((Func<SisnetData.ProcessInfo, string>)(drv => "'" + drv.fldidvalidacionarchivos.ToString() + "'")).ToList<string>();
            DirectoryInfo directoryInfo = new DirectoryInfo(this.WorkingPath);
            if (!directoryInfo.Exists)
            {
                this.WriteLog("Creating directory " + this.WorkingPath);
                directoryInfo.Create();
            }
            int num1 = 0;
            while (list1.Any<string>())
            {
                this.ClearFolder();
                List<string> list2 = list1.Take<string>(50).ToList<string>();
                string fldidvalidacionarchivos = string.Join(",", (IEnumerable<string>)list2);
                foreach (SisnetData.ProcessInfo processInfo1 in dbManager.GetDataFileToConvert(this.tableToValidate.ToString(), fldidvalidacionarchivos))
                {
                    SisnetData.ProcessInfo itemExportFile = processInfo1;
                    if (this.StopSignal)
                        return;
                    SisnetData.ProcessInfo processInfo2 = this.globalData.Where<SisnetData.ProcessInfo>((Func<SisnetData.ProcessInfo, bool>)(toProcess => toProcess.fldidvalidacionarchivos == itemExportFile.fldidvalidacionarchivos)).Single<SisnetData.ProcessInfo>();
                    processInfo2.ArchivoData = itemExportFile.ArchivoData;
                    this.WriteLog("Processing " + processInfo2.fldidvalidacionarchivos.ToString() + " action:" + processInfo2.accion);
                    if (!this.IsValidExtension(processInfo2))
                    {
                        continue;
                    }
                    if (string.IsNullOrEmpty(processInfo2.accion))
                    {
                        this.CorregirNombre(processInfo2);
                        processInfo2.estado = "OK";
                        processInfo2.archivoresultante = processInfo2.ArchivoData;
                        dbManager.UpdateValidacionarchivos(processInfo2);
                        processInfo2.ArchivoData = (byte[])null;
                        processInfo2.archivoresultante = (byte[])null;
                    }
                    else
                    {
                        if (processInfo2.accion == "A" && processInfo2.ArchivoData != null && processInfo2.ArchivoData.Length != 0)
                        {
                            this.CorregirNombre(processInfo2);
                            FileInfo fileInfo = new FileInfo(processInfo2.nombrearchivoarchivoresultante);
                            this.CrearArchivoBase(processInfo2);
                            string fullpath = this.WorkingPath + "\\" + fileInfo.Name;
                            int errorCode = 0;
                            if (!processInfo2.nombrearchivoarchivo.EndsWith(".pdf") && processInfo2.ProcesarExcel)
                                errorCode = this.ConvertToPDF(fullpath, processInfo2, (string)null);
                            if (errorCode == 0)
                            {
                                processInfo2.archivoresultante = this.GetFileData(processInfo2);
                                processInfo2.estado = "OK";
                                dbManager.UpdateValidacionarchivos(processInfo2);
                            }
                            else
                                this.ReportError(processInfo2, errorCode);
                            processInfo2.ArchivoData = (byte[])null;
                            processInfo2.archivoresultante = (byte[])null;
                        }
                        else if (processInfo2.accion == "A" && (processInfo2.ArchivoData == null || processInfo2.ArchivoData.Length == 0))
                        {
                            processInfo2.estado = "ER";
                            processInfo2.mensajeerror = "NO SE PUEDE CONVERTIR A PDF";
                            dbManager.UpdateValidacionarchivos(processInfo2);
                        }
                        if (processInfo2.accion == "B" && processInfo2.ArchivoData != null && processInfo2.ArchivoData.Length != 0)
                        {
                            this.CorregirNombre(processInfo2);
                            if (!processInfo2.nombrearchivoarchivo.EndsWith(".pdf"))
                            {
                                processInfo2.archivoresultante = processInfo2.ArchivoData;
                                processInfo2.estado = "ER";
                                processInfo2.mensajeerror = "ARCHIVO DEBE SER PDF";
                                dbManager.UpdateValidacionarchivos(processInfo2);
                                processInfo2.archivoresultante = (byte[])null;
                                processInfo2.ArchivoData = (byte[])null;
                                continue;
                            }
                            FileInfo fileInfo = new FileInfo(processInfo2.nombrearchivoarchivoresultante);
                            this.CrearArchivoBase(processInfo2);
                            string str = this.WorkingPath + "\\" + fileInfo.Name;
                            int pdf = this.ConvertToPDF(str, processInfo2, "waterMark");
                            if (pdf != 0)
                            {
                                this.ReportError(processInfo2, pdf);
                                continue;
                            }
                            if (this.AddTextMark(processInfo2, str) == 0)
                            {
                                processInfo2.archivoresultante = this.GetFileData(processInfo2);
                                processInfo2.estado = "OK";
                                dbManager.UpdateValidacionarchivos(processInfo2);
                            }
                            processInfo2.ArchivoData = (byte[])null;
                            processInfo2.archivoresultante = (byte[])null;
                        }
                        else if (processInfo2.accion == "B" && (processInfo2.ArchivoData == null || processInfo2.ArchivoData.Length == 0))
                        {
                            processInfo2.estado = "ER";
                            processInfo2.mensajeerror = "ARCHIVO DEBE SER PDF";
                            dbManager.UpdateValidacionarchivos(processInfo2);
                        }
                        if (processInfo2.accion == "C" && processInfo2.ArchivoData != null && processInfo2.ArchivoData.Length != 0)
                        {
                            this.CorregirNombre(processInfo2);
                            if (!processInfo2.nombrearchivoarchivo.EndsWith(".pdf"))
                            {
                                processInfo2.archivoresultante = processInfo2.ArchivoData;
                                processInfo2.estado = "ER";
                                processInfo2.mensajeerror = "ARCHIVO DEBE SER PDF";
                                dbManager.UpdateValidacionarchivos(processInfo2);
                                processInfo2.archivoresultante = (byte[])null;
                                processInfo2.ArchivoData = (byte[])null;
                                continue;
                            }
                            FileInfo fileInfo = new FileInfo(processInfo2.nombrearchivoarchivoresultante);
                            this.CrearArchivoBase(processInfo2);
                            if (this.CompressFile(this.WorkingPath + "\\" + fileInfo.Name, processInfo2) == 0)
                            {
                                processInfo2.archivoresultante = this.GetFileData(processInfo2);
                                processInfo2.estado = "OK";
                                dbManager.UpdateValidacionarchivos(processInfo2);
                            }
                            processInfo2.ArchivoData = (byte[])null;
                            processInfo2.archivoresultante = (byte[])null;
                        }
                        else if (processInfo2.accion == "C" && (processInfo2.ArchivoData == null || processInfo2.ArchivoData.Length == 0))
                        {
                            processInfo2.estado = "ER";
                            processInfo2.mensajeerror = "ARCHIVO DEBE SER PDF";
                            dbManager.UpdateValidacionarchivos(processInfo2);
                        }
                        if (processInfo2.accion == "AB" && processInfo2.ArchivoData != null && processInfo2.ArchivoData.Length != 0)
                        {
                            this.CorregirNombre(processInfo2);
                            FileInfo fileInfo = new FileInfo(processInfo2.nombrearchivoarchivoresultante);
                            this.CrearArchivoBase(processInfo2);
                            stopwatch.Stop();
                            TimeSpan elapsed = stopwatch.Elapsed;
                            this.WriteLog(string.Format("Archivo base Time: {0}h {1}m {2}s {3}ms", (object)elapsed.Hours, (object)elapsed.Minutes, (object)elapsed.Seconds, (object)elapsed.Milliseconds));
                            stopwatch.Restart();
                            string str = this.WorkingPath + "\\" + fileInfo.Name;
                            int errorCode = 0;
                            if (!processInfo2.nombrearchivoarchivo.EndsWith(".pdf") && processInfo2.ProcesarExcel)
                            {
                                errorCode = this.ConvertToPDF(str, processInfo2, (string)null);
                                elapsed = stopwatch.Elapsed;
                                this.WriteLog(string.Format("ConvertToPDF Time: {0}h {1}m {2}s {3}ms", (object)elapsed.Hours, (object)elapsed.Minutes, (object)elapsed.Seconds, (object)elapsed.Milliseconds));
                                stopwatch.Restart();
                            }
                            if (errorCode != 0)
                            {
                                this.ReportError(processInfo2, errorCode);
                                continue;
                            }
                            int pdf = this.ConvertToPDF(str, processInfo2, "waterMark");
                            elapsed = stopwatch.Elapsed;
                            this.WriteLog(string.Format("marca de agua Time: {0}h {1}m {2}s {3}ms", (object)elapsed.Hours, (object)elapsed.Minutes, (object)elapsed.Seconds, (object)elapsed.Milliseconds));
                            stopwatch.Restart();
                            if (pdf != 0)
                            {
                                this.ReportError(processInfo2, pdf);
                                continue;
                            }
                            int num2 = this.AddTextMark(processInfo2, str);
                            elapsed = stopwatch.Elapsed;
                            this.WriteLog(string.Format("AddTextMark Time: {0}h {1}m {2}s {3}ms", (object)elapsed.Hours, (object)elapsed.Minutes, (object)elapsed.Seconds, (object)elapsed.Milliseconds));
                            stopwatch.Restart();
                            if (num2 == 0)
                            {
                                processInfo2.archivoresultante = this.GetFileData(processInfo2);
                                processInfo2.estado = "OK";
                                dbManager.UpdateValidacionarchivos(processInfo2);
                                elapsed = stopwatch.Elapsed;
                                this.WriteLog(string.Format("Save DB Time: {0}h {1}m {2}s {3}ms", (object)elapsed.Hours, (object)elapsed.Minutes, (object)elapsed.Seconds, (object)elapsed.Milliseconds));
                                stopwatch.Restart();
                            }
                            processInfo2.ArchivoData = (byte[])null;
                            processInfo2.archivoresultante = (byte[])null;
                        }
                        else if (processInfo2.accion == "AB" && (processInfo2.ArchivoData == null || processInfo2.ArchivoData.Length == 0))
                        {
                            processInfo2.estado = "ER";
                            processInfo2.mensajeerror = "NO SE PUEDE CONVERTIR A PDF";
                            dbManager.UpdateValidacionarchivos(processInfo2);
                        }
                        if (processInfo2.accion == "AC" && processInfo2.ArchivoData != null && processInfo2.ArchivoData.Length != 0)
                        {
                            this.CorregirNombre(processInfo2);
                            FileInfo fileInfo = new FileInfo(processInfo2.nombrearchivoarchivoresultante);
                            this.CrearArchivoBase(processInfo2);
                            string fullpath = this.WorkingPath + "\\" + fileInfo.Name;
                            int errorCode = 0;
                            if (!processInfo2.nombrearchivoarchivo.EndsWith(".pdf") && processInfo2.ProcesarExcel)
                                errorCode = this.ConvertToPDF(fullpath, processInfo2, (string)null);
                            if (errorCode != 0)
                            {
                                this.ReportError(processInfo2, errorCode);
                                continue;
                            }
                            if (this.CompressFile(fullpath, processInfo2) == 0)
                            {
                                processInfo2.archivoresultante = this.GetFileData(processInfo2);
                                processInfo2.estado = "OK";
                                dbManager.UpdateValidacionarchivos(processInfo2);
                            }
                            processInfo2.ArchivoData = (byte[])null;
                            processInfo2.archivoresultante = (byte[])null;
                        }
                        else if (processInfo2.accion == "AC" && (processInfo2.ArchivoData == null || processInfo2.ArchivoData.Length == 0))
                        {
                            processInfo2.estado = "ER";
                            processInfo2.mensajeerror = "NO SE PUEDE CONVERTIR A PDF";
                            dbManager.UpdateValidacionarchivos(processInfo2);
                        }
                        if (processInfo2.accion == "BC" && processInfo2.ArchivoData != null && processInfo2.ArchivoData.Length != 0)
                        {
                            this.CorregirNombre(processInfo2);
                            if (!processInfo2.nombrearchivoarchivo.EndsWith(".pdf"))
                            {
                                processInfo2.archivoresultante = processInfo2.ArchivoData;
                                processInfo2.estado = "ER";
                                processInfo2.mensajeerror = "ARCHIVO DEBE SER PDF";
                                dbManager.UpdateValidacionarchivos(processInfo2);
                                processInfo2.archivoresultante = (byte[])null;
                                processInfo2.ArchivoData = (byte[])null;
                                continue;
                            }
                            FileInfo fileInfo = new FileInfo(processInfo2.nombrearchivoarchivoresultante);
                            this.CrearArchivoBase(processInfo2);
                            string str = this.WorkingPath + "\\" + fileInfo.Name;
                            int pdf = this.ConvertToPDF(str, processInfo2, "waterMark");
                            if (pdf != 0)
                            {
                                this.ReportError(processInfo2, pdf);
                                continue;
                            }
                            if (this.AddTextMark(processInfo2, str) == 0)
                            {
                                if (this.CompressFile(str, processInfo2) == 0)
                                {
                                    processInfo2.archivoresultante = this.GetFileData(processInfo2);
                                    processInfo2.estado = "OK";
                                    dbManager.UpdateValidacionarchivos(processInfo2);
                                }
                                processInfo2.ArchivoData = (byte[])null;
                                processInfo2.archivoresultante = (byte[])null;
                            }
                            else
                                continue;
                        }
                        else if (processInfo2.accion == "BC" && (processInfo2.ArchivoData == null || processInfo2.ArchivoData.Length == 0))
                        {
                            processInfo2.estado = "ER";
                            processInfo2.mensajeerror = "ARCHIVO DEBE SER PDF";
                            dbManager.UpdateValidacionarchivos(processInfo2);
                        }
                        if (processInfo2.accion == "ABC" && processInfo2.ArchivoData != null && processInfo2.ArchivoData.Length != 0)
                        {
                            this.CorregirNombre(processInfo2);
                            FileInfo fileInfo = new FileInfo(processInfo2.nombrearchivoarchivoresultante);
                            this.CrearArchivoBase(processInfo2);
                            string str = this.WorkingPath + "\\" + fileInfo.Name;
                            int errorCode = 0;
                            if (!processInfo2.nombrearchivoarchivo.EndsWith(".pdf") && processInfo2.ProcesarExcel)
                                errorCode = this.ConvertToPDF(str, processInfo2, (string)null);
                            if (errorCode != 0)
                            {
                                this.ReportError(processInfo2, errorCode);
                                continue;
                            }
                            int pdf = this.ConvertToPDF(str, processInfo2, "waterMark");
                            if (pdf != 0)
                            {
                                this.ReportError(processInfo2, pdf);
                                continue;
                            }
                            if (this.AddTextMark(processInfo2, str) == 0)
                            {
                                if (this.CompressFile(str, processInfo2) == 0)
                                {
                                    processInfo2.archivoresultante = this.GetFileData(processInfo2);
                                    processInfo2.estado = "OK";
                                    dbManager.UpdateValidacionarchivos(processInfo2);
                                }
                                processInfo2.ArchivoData = (byte[])null;
                                processInfo2.archivoresultante = (byte[])null;
                            }
                            else
                                continue;
                        }
                        else if (processInfo2.accion == "ABC" && (processInfo2.ArchivoData == null || processInfo2.ArchivoData.Length == 0))
                        {
                            processInfo2.estado = "ER";
                            processInfo2.mensajeerror = "NO SE PUEDE CONVERTIR A PDF";
                            dbManager.UpdateValidacionarchivos(processInfo2);
                        }
                        int num3 = num1 * 100 / this.globalData.Count;
                        ++num1;
                    }

                }
                list1.RemoveRange(0, list2.Count);
            }
            stopwatch.Stop();
            TimeSpan elapsed1 = stopwatch.Elapsed;
            this.WriteLog(string.Format("Time: {0}h {1}m {2}s {3}ms", (object)elapsed1.Hours, (object)elapsed1.Minutes, (object)elapsed1.Seconds, (object)elapsed1.Milliseconds));
            this.ClearFolder();
        }

        private bool IsValidExtension(SisnetData.ProcessInfo itemExport)
        {
            string str = new Regex("[^A-Za-z0-9á-úÁ-Ó ._-]", RegexOptions.IgnoreCase).Replace(itemExport.nombrearchivoarchivo.Normalize(NormalizationForm.FormD), "");
            string mensajeError = null;

            if (!str.Contains("."))
            {
                mensajeError = "EL ARCHIVO NO TIENE EXTENSION";
            }

            string[] extensionesPermitidas = this.extensiones.ToLower().Split(",".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
            string extensionArchivo = itemExport.nombrearchivoarchivo.Normalize(NormalizationForm.FormD);
            extensionArchivo = extensionArchivo.Substring(extensionArchivo.LastIndexOf(".") + 1).ToLower();

            if (!extensionesPermitidas.Contains(extensionArchivo))
            {
                mensajeError = "EXTENSION DEL ARCHIVO NO PERMITIDA";
            }

            if (String.IsNullOrEmpty(mensajeError))
            {
                return true;
            }

            itemExport.nombrearchivoarchivoresultante = str;
            itemExport.estado = "ER";
            itemExport.mensajeerror = mensajeError;
            itemExport.archivoresultante = itemExport.ArchivoData;
            DBManager.GetDBManager().UpdateValidacionarchivos(itemExport);
            itemExport.ArchivoData = (byte[])null;
            itemExport.archivoresultante = (byte[])null;
            return false;
        }

        private void WriteLog(string text, Exception ex = null)
        {
            if (!this.log || !text.EndsWith("ms"))
                return;
            string str = this.GetCurrentDirectory() + "log.txt";
            string path1 = str;
            DateTime now = DateTime.Now;
            string contents1 = now.ToString("yyyy-MM-dd HH:mm:ss") + " " + text + "\r\n";
            File.AppendAllText(path1, contents1);
            if (ex == null)
                return;
            string path2 = str;
            now = DateTime.Now;
            string contents2 = now.ToString("yyyy-MM-dd HH:mm:ss") + " " + ex.StackTrace + "\r\n";
            File.AppendAllText(path2, contents2);
        }

        private string GetCurrentDirectory() => AppDomain.CurrentDomain.BaseDirectory;

        private int AddTextMark(SisnetData.ProcessInfo itemExport, string fullPath)
        {
            int errorCode = 0;
            if (!string.IsNullOrEmpty(itemExport.etiqueta1))
                errorCode = this.ConvertToPDF(fullPath, itemExport, "TextWaterMark1");
            if (errorCode != 0)
            {
                this.ReportError(itemExport, errorCode);
                return errorCode;
            }
            if (!string.IsNullOrEmpty(itemExport.etiqueta2))
                errorCode = this.ConvertToPDF(fullPath, itemExport, "TextWaterMark2");
            if (errorCode != 0)
            {
                this.ReportError(itemExport, errorCode);
                return errorCode;
            }
            if (!string.IsNullOrEmpty(itemExport.etiqueta3))
                errorCode = this.ConvertToPDF(fullPath, itemExport, "TextWaterMark3");
            if (errorCode != 0)
            {
                this.ReportError(itemExport, errorCode);
                return errorCode;
            }
            if (!string.IsNullOrEmpty(itemExport.etiqueta4))
                errorCode = this.ConvertToPDF(fullPath, itemExport, "TextWaterMark4");
            if (errorCode == 0)
                return 0;
            this.ReportError(itemExport, errorCode);
            return errorCode;
        }

        private void ReportError(SisnetData.ProcessInfo itemExport, int errorCode)
        {
            DBManager dbManager = DBManager.GetDBManager();
            itemExport.extensionarchivoresultant = itemExport.extensionarchivo;
            itemExport.estado = "ER";
            itemExport.mensajeerror = "NO SE PUEDE CONVERTIR A PDF";
            itemExport.archivoresultante = itemExport.ArchivoData;
            itemExport.ArchivoData = (byte[])null;
            itemExport.nombrearchivoarchivoresultante = itemExport.nombrearchivoarchivo;
            SisnetData.ProcessInfo processInfo = itemExport;
            dbManager.UpdateValidacionarchivos(processInfo);
        }

        private void CrearArchivoBase(SisnetData.ProcessInfo itemExport)
        {
            using (MemoryStream memoryStream = new MemoryStream(itemExport.ArchivoData))
            {
                using (FileStream fileStream = new FileStream(this.WorkingPath + "\\" + new FileInfo(itemExport.nombrearchivoarchivoresultante).Name, FileMode.Create, FileAccess.Write))
                {
                    byte[] buffer = new byte[memoryStream.Length];
                    memoryStream.Read(buffer, 0, (int)memoryStream.Length);
                    fileStream.Write(buffer, 0, buffer.Length);
                    memoryStream.Close();
                }
            }
        }

        private void CorregirNombre(SisnetData.ProcessInfo itemExport)
        {
            Regex regex = new Regex("[^A-Za-z0-9á-úÁ-Ó ._-]", RegexOptions.IgnoreCase);
            itemExport.nombrearchivoarchivoresultante = itemExport.nombrearchivoarchivo.Normalize(NormalizationForm.FormD);
            itemExport.nombrearchivoarchivoresultante = regex.Replace(itemExport.nombrearchivoarchivoresultante, "");
            if (!itemExport.nombrearchivoarchivoresultante.Contains("."))
                return;
            string nombrearchivoarchivoresultante = itemExport.nombrearchivoarchivoresultante;
            string str1 = nombrearchivoarchivoresultante.Substring(0, nombrearchivoarchivoresultante.LastIndexOf("."));
            string str2 = nombrearchivoarchivoresultante.Substring(nombrearchivoarchivoresultante.LastIndexOf("."));
            string str3 = str1.Replace(".", "_");
            itemExport.nombrearchivoarchivoresultante = str3 + str2.ToLower();
            itemExport.extensionarchivoresultant = str2.ToUpper();
            itemExport.nombrearchivoarchivo = itemExport.nombrearchivoarchivoresultante;
            itemExport.extensionarchivo = str2.ToUpper();
        }

        private byte[] GetFileData(SisnetData.ProcessInfo itemExport)
        {
            string str = this.WorkingPath + "\\" + itemExport.nombrearchivoarchivoresultante;
            FileInfo fileInfo = new FileInfo(str);
            byte[] buffer = (byte[])null;
            using (FileStream fileStream = new FileStream(str, FileMode.Open, FileAccess.Read))
            {
                buffer = new byte[fileStream.Length];
                int length1 = (int)fileStream.Length;
                int offset = 0;
                int num;
                for (; length1 > 0; length1 -= num)
                {
                    num = fileStream.Read(buffer, offset, length1);
                    if (num != 0)
                        offset += num;
                    else
                        break;
                }
                int length2 = buffer.Length;
            }
            return buffer;
        }

        private void ClearFolder()
        {
            IEnumerable<FileInfo> source = new DirectoryInfo(this.WorkingPath).EnumerateFiles("*.*");
            if (!source.Any<FileInfo>())
                return;
            foreach (FileInfo fileInfo in source)
            {
                if (!fileInfo.Extension.ToLower().Contains("bat") && !fileInfo.Extension.ToLower().Contains("exe"))
                    fileInfo.Delete();
            }
        }

        private int ConvertToPDF(string fullpath, SisnetData.ProcessInfo itemExport, string mode)
        {
            this.WriteLog("Start ConvertToPDF " + itemExport.fldidvalidacionarchivos.ToString() + " action:" + itemExport.accion);
            string str = itemExport.accion;
            if (str.Contains("A") && mode == null)
            {
                str = "A";
                if (new FileInfo(fullpath).Extension.Contains(".pdf"))
                    return 0;
            }
            else if (str.Contains("B") && mode != null)
            {
                str = "B";
                FileInfo fileInfo = new FileInfo(fullpath);
                if (!fileInfo.Extension.Contains(".pdf"))
                    fullpath = fileInfo.DirectoryName + "\\" + fileInfo.Name.Replace(fileInfo.Extension, ".pdf");
            }
            FileInfo fileInfo1 = new FileInfo(fullpath);
            string text = string.Empty;
            if (!(str == "A"))
            {
                if (str == "B")
                {
                    if (!(mode == "waterMark"))
                    {
                        if (!(mode == "TextWaterMark1"))
                        {
                            if (!(mode == "TextWaterMark2"))
                            {
                                if (!(mode == "TextWaterMark3"))
                                {
                                    if (mode == "TextWaterMark4")
                                        text = string.Format(this.cmdTextWaterMark4, (object)fileInfo1.FullName, (object)fileInfo1.DirectoryName, (object)itemExport.etiqueta4);
                                }
                                else
                                    text = string.Format(this.cmdTextWaterMark3, (object)fileInfo1.FullName, (object)fileInfo1.DirectoryName, (object)itemExport.etiqueta3);
                            }
                            else
                                text = string.Format(this.cmdTextWaterMark2, (object)fileInfo1.FullName, (object)fileInfo1.DirectoryName, (object)itemExport.etiqueta2);
                        }
                        else
                            text = string.Format(this.cmdTextWaterMark1, (object)fileInfo1.FullName, (object)fileInfo1.DirectoryName, (object)itemExport.etiqueta1);
                    }
                    else
                        text = string.Format(this.cmdWaterMark, (object)fileInfo1.FullName, (object)fileInfo1.DirectoryName, (object)this.imgLogo);
                }
            }
            else
                text = string.Format(this.cmdConvertPDF, (object)fileInfo1.FullName, (object)fileInfo1.DirectoryName);
            ProcessStartInfo startInfo = new ProcessStartInfo();
            startInfo.Arguments = text;
            startInfo.FileName = this.PdfExe;
            startInfo.WindowStyle = ProcessWindowStyle.Normal;
            startInfo.CreateNoWindow = true;
            startInfo.UseShellExecute = false;
            this.WriteLog("Launching  " + startInfo.FileName);
            Stopwatch stopwatch = new Stopwatch();
            int exitCode;
            using (this.proc = Process.Start(startInfo))
            {
                Thread.Sleep(this.secondsToPressContinue2PDF * 1000);
                this.WriteLog("WaitForExit");
                stopwatch.Start();
                this.proc.WaitForExit();
                stopwatch.Stop();
                TimeSpan elapsed = stopwatch.Elapsed;
                this.WriteLog(string.Format("WaitForExit Time: {0}h {1}m {2}s {3}ms", (object)elapsed.Hours, (object)elapsed.Minutes, (object)elapsed.Seconds, (object)elapsed.Milliseconds));
                exitCode = this.proc.ExitCode;
            }
            string[] strArray = new string[6];
            strArray[0] = "End ConvertToPDF ";
            int fldidvalidacionarchivos = itemExport.fldidvalidacionarchivos;
            strArray[1] = fldidvalidacionarchivos.ToString();
            strArray[2] = " action:";
            strArray[3] = itemExport.accion;
            strArray[4] = " with exitCode: ";
            strArray[5] = exitCode.ToString();
            this.WriteLog(string.Concat(strArray));
            if (exitCode == 0)
            {
                if (fileInfo1.Extension.ToLower().Contains(".pdf"))
                {
                    fileInfo1.Delete();
                    File.Move(fileInfo1.FullName.Replace(".pdf", " (1).pdf"), fileInfo1.FullName);
                }
                itemExport.nombrearchivoarchivoresultante = fileInfo1.Name.Replace(fileInfo1.Extension, ".pdf");
                itemExport.extensionarchivoresultant = ".PDF";
                if (!File.Exists(fileInfo1.DirectoryName + "\\" + itemExport.nombrearchivoarchivoresultante))
                {
                    this.WriteLog("Invalid 2PDF execution [" + fileInfo1.DirectoryName + "\\" + itemExport.nombrearchivoarchivoresultante + "] no existe!");
                    this.WriteLog(text);
                    return -1;
                }
            }
            fldidvalidacionarchivos = itemExport.fldidvalidacionarchivos;
            this.WriteLog("End ConvertToPDF " + fldidvalidacionarchivos.ToString() + " action:" + itemExport.accion);
            return exitCode;
        }

        private int ConvertToPDF_BKP(string fullpath, SisnetData.ProcessInfo itemExport, string mode)
        {
            this.WriteLog("Start ConvertToPDF " + itemExport.fldidvalidacionarchivos.ToString() + " action:" + itemExport.accion);
            string str = itemExport.accion;
            if (str.Contains("A") && mode == null)
            {
                str = "A";
                if (new FileInfo(fullpath).Extension.Contains(".pdf"))
                    return 0;
            }
            else if (str.Contains("B") && mode != null)
            {
                str = "B";
                FileInfo fileInfo = new FileInfo(fullpath);
                if (!fileInfo.Extension.Contains(".pdf"))
                    fullpath = fileInfo.DirectoryName + "\\" + fileInfo.Name.Replace(fileInfo.Extension, ".pdf");
            }
            FileInfo fileInfo1 = new FileInfo(fullpath);
            string text = string.Empty;
            if (!(str == "A"))
            {
                if (str == "B")
                {
                    if (!(mode == "waterMark"))
                    {
                        if (!(mode == "TextWaterMark1"))
                        {
                            if (!(mode == "TextWaterMark2"))
                            {
                                if (!(mode == "TextWaterMark3"))
                                {
                                    if (mode == "TextWaterMark4")
                                        text = string.Format(this.cmdTextWaterMark4, (object)fileInfo1.FullName, (object)fileInfo1.DirectoryName, (object)itemExport.etiqueta4);
                                }
                                else
                                    text = string.Format(this.cmdTextWaterMark3, (object)fileInfo1.FullName, (object)fileInfo1.DirectoryName, (object)itemExport.etiqueta3);
                            }
                            else
                                text = string.Format(this.cmdTextWaterMark2, (object)fileInfo1.FullName, (object)fileInfo1.DirectoryName, (object)itemExport.etiqueta2);
                        }
                        else
                            text = string.Format(this.cmdTextWaterMark1, (object)fileInfo1.FullName, (object)fileInfo1.DirectoryName, (object)itemExport.etiqueta1);
                    }
                    else
                        text = string.Format(this.cmdWaterMark, (object)fileInfo1.FullName, (object)fileInfo1.DirectoryName, (object)this.imgLogo);
                }
            }
            else
                text = string.Format(this.cmdConvertPDF, (object)fileInfo1.FullName, (object)fileInfo1.DirectoryName);
            ProcessStartInfo startInfo = new ProcessStartInfo();
            startInfo.Arguments = text;
            startInfo.FileName = this.PdfExe;
            startInfo.WindowStyle = ProcessWindowStyle.Normal;
            startInfo.CreateNoWindow = true;
            startInfo.UseShellExecute = false;
            this.WriteLog("Launching  " + startInfo.FileName);
            int exitCode;
            using (this.proc = Process.Start(startInfo))
            {
                Thread.Sleep(this.secondsToPressContinue2PDF * 1000);
                this.WriteLog("WaitForExit");
                this.proc.WaitForExit();
                exitCode = this.proc.ExitCode;
            }
            string[] strArray = new string[6];
            strArray[0] = "End ConvertToPDF ";
            int fldidvalidacionarchivos = itemExport.fldidvalidacionarchivos;
            strArray[1] = fldidvalidacionarchivos.ToString();
            strArray[2] = " action:";
            strArray[3] = itemExport.accion;
            strArray[4] = " with exitCode: ";
            strArray[5] = exitCode.ToString();
            this.WriteLog(string.Concat(strArray));
            if (exitCode == 0)
            {
                if (fileInfo1.Extension.ToLower().Contains(".pdf"))
                {
                    fileInfo1.Delete();
                    File.Move(fileInfo1.FullName.Replace(".pdf", " (1).pdf"), fileInfo1.FullName);
                }
                itemExport.nombrearchivoarchivoresultante = fileInfo1.Name.Replace(fileInfo1.Extension, ".pdf");
                itemExport.extensionarchivoresultant = ".PDF";
                if (!File.Exists(fileInfo1.DirectoryName + "\\" + itemExport.nombrearchivoarchivoresultante))
                {
                    this.WriteLog("Invalid 2PDF execution [" + fileInfo1.DirectoryName + "\\" + itemExport.nombrearchivoarchivoresultante + "] no existe!");
                    this.WriteLog(text);
                    return -1;
                }
            }
            fldidvalidacionarchivos = itemExport.fldidvalidacionarchivos;
            this.WriteLog("End ConvertToPDF " + fldidvalidacionarchivos.ToString() + " action:" + itemExport.accion);
            return exitCode;
        }

        private int CompressFile(string fullpath, SisnetData.ProcessInfo itemExport)
        {
            FileInfo fileInfo1 = new FileInfo(fullpath);
            if (!fileInfo1.Extension.Contains(".pdf"))
                fullpath = fileInfo1.DirectoryName + "\\" + fileInfo1.Name.Replace(fileInfo1.Extension, ".pdf");
            FileInfo fileInfo2 = new FileInfo(fullpath);
            string str1 = fileInfo2.Name.Replace(".pdf", " CP.pdf");
            string str2 = fileInfo2.DirectoryName + "\\" + str1;
            string str3 = string.Format(this.cmdCompresorPDF, (object)fileInfo2.FullName, (object)str2);
            int num;
            using (this.proc = Process.Start(new ProcessStartInfo()
            {
                Arguments = str3,
                FileName = this.PdfCompresorExe,
                WindowStyle = ProcessWindowStyle.Hidden,
                CreateNoWindow = true,
                UseShellExecute = false
            }))
            {
                this.proc.WaitForExit();
                num = this.proc.ExitCode;
            }
            if (num == 0 || num == 1)
            {
                FileInfo fileInfo3 = new FileInfo(str2);
                if (fileInfo3.Length < fileInfo2.Length)
                {
                    fileInfo2.Delete();
                    File.Move(str2, fileInfo2.FullName);
                }
                else
                    fileInfo3.Delete();
                itemExport.nombrearchivoarchivoresultante = fileInfo2.Name.Replace(fileInfo2.Extension, ".pdf");
                itemExport.extensionarchivoresultant = ".PDF";
                num = 0;
            }
            return num;
        }
    }
}
