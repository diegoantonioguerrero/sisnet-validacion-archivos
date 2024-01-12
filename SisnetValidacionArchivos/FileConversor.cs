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
        private List<ProcessInfo> globalData;
        private string WorkingPath;
        private string cmdConvertPDF;
        private string cmdCompresorPDF;
        private string cmdTextWaterMark1;
        private string cmdTextWaterMark2;
        private string cmdTextWaterMark3;
        private string cmdTextWaterMark4;
        private string pdfexeclauncher;
        private bool log;
        public bool StopSignal;
        private IntPtr ServiceHandle;
        private Process proc;
#if DEBUG
        bool testLocal;
#endif
        [DllImport("user32.dll", CharSet = CharSet.None, ExactSpelling = false, SetLastError = true)]
        public static extern IntPtr SetParent(IntPtr hWndChild, IntPtr hWndNewParent);

        [DllImport("user32.dll", CharSet = CharSet.None, ExactSpelling = false)]
        public static extern bool SetForegroundWindow(IntPtr hWnd);

        public FileConversor(IntPtr ServiceHandle)
        {
            this.ServiceHandle = ServiceHandle;
            DBManager dBManager = DBManager.GetDBManager();
            try
            {
                string appSetting1 = ConfigurationManager.AppSettings["server"];
                string appSetting2 = ConfigurationManager.AppSettings["database"];
                string appSetting3 = ConfigurationManager.AppSettings["user"];
                string appSetting4 = ConfigurationManager.AppSettings["password"];
                dBManager.SetDBManager(appSetting1, "5432", appSetting2, appSetting3, appSetting4);
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
            this.tableToValidate = ConfigurationManager.AppSettings["tableToValidate"];
            this.PdfExe = ConfigurationManager.AppSettings["2pdfexe"];
            this.PdfCompresorExe = ConfigurationManager.AppSettings["pdfcompresorexe"];
            this.imgLogo = ConfigurationManager.AppSettings["logoWaterMark"];
            this.imgLogo = this.GetCurrentDirectory() + this.imgLogo;
            this.cmdConvertPDF = ConfigurationManager.AppSettings["cmdConvertPDF"];
            this.cmdCompresorPDF = ConfigurationManager.AppSettings["cmdCompresorPDF"];
            this.cmdWaterMark = ConfigurationManager.AppSettings["cmdWaterMark"];
            this.cmdTextWaterMark1 = ConfigurationManager.AppSettings["cmdTextWaterMark1"];
            this.cmdTextWaterMark2 = ConfigurationManager.AppSettings["cmdTextWaterMark2"];
            this.cmdTextWaterMark3 = ConfigurationManager.AppSettings["cmdTextWaterMark3"];
            this.cmdTextWaterMark4 = ConfigurationManager.AppSettings["cmdTextWaterMark4"];
            this.pdfexeclauncher = ConfigurationManager.AppSettings["2pdfexeclauncher"];
            this.secondsToPressContinue2PDF = int.Parse(ConfigurationManager.AppSettings["secondsToPressContinue2PDF"]);
            this.log = bool.Parse(ConfigurationManager.AppSettings["log"]);
        }

        public void ChangeData()
        {
            try
            {

                DBManager dBManager = DBManager.GetDBManager();
                /*
                using (FileStream fileStream = new FileStream("C:\\Users\\USUARIO.DESKTOP-DIEGO\\Downloads\\Sin confirmar 46635.crdownload", FileMode.Open, FileAccess.Read))
                {
                    using (BinaryReader binaryReader = new BinaryReader(new BufferedStream(fileStream)))
                    {
                        
                        byte[] numArray = binaryReader.ReadBytes(Convert.ToInt32(fileStream.Length));
                        dBManager.InsertValidacionarchivos("Sin confirmar 46635.crdownload", numArray);
                        numArray = null;
                        // Collect all generations of memory.
                        GC.Collect();
                    }
                }
                */

                this.globalData = dBManager.GetPendingFiles(this.tableToValidate.ToString());
                this.ExportData();
            }
            catch (Exception ex)
            {
                this.WriteLog(ex.Message, ex);
            }
        }

        private void ExportData()
        {
            TimeSpan elapsed;
            FileInfo fileInfo;
            string str;
            if (this.globalData.Count == 0)
            {
                return;
            }
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            DBManager dBManager = DBManager.GetDBManager();
            List<string> strs = new List<string>();
            List<string> list = (
                from drv in this.globalData
                select string.Concat("'", drv.fldidvalidacionarchivos.ToString(), "'")).ToList<string>();
            DirectoryInfo directoryInfo = new DirectoryInfo(this.WorkingPath);
            if (!directoryInfo.Exists)
            {
                this.WriteLog("Creating directory " + this.WorkingPath);
                directoryInfo.Create();
            }
            int num = 0;
            while (true)
            {
                if (list.Any<string>())
                {
                    this.ClearFolder();
                    List<string> list1 = list.Take<string>(50).ToList<string>();
                    string str1 = string.Join(",", list1.ToArray());
                    foreach (ProcessInfo dataFileToConvert in dBManager.GetDataFileToConvert(this.tableToValidate.ToString(), str1))
                    {
                        if (!this.StopSignal)
                        {
                            ProcessInfo archivoData = (
                                from toProcess in this.globalData
                                where toProcess.fldidvalidacionarchivos == dataFileToConvert.fldidvalidacionarchivos
                                select toProcess).Single<ProcessInfo>();
                            archivoData.ArchivoData = dataFileToConvert.ArchivoData;
                            int num1 = archivoData.fldidvalidacionarchivos;
                            this.WriteLog(string.Concat("Processing ", num1.ToString(), " action:", archivoData.accion), null);
                            if (!this.IsValidExtension(archivoData))
                            {
                                continue;
                            }
                            if (!string.IsNullOrEmpty(archivoData.accion))
                            {
                                if (archivoData.accion == "A" && archivoData.ArchivoData != null && archivoData.ArchivoData.Length != 0)
                                {
                                    this.CorregirNombre(archivoData);
                                    fileInfo = new FileInfo(archivoData.nombrearchivoarchivoresultante);
                                    this.CrearArchivoBase(archivoData);
                                    str = string.Concat(this.WorkingPath, "\\", fileInfo.Name);
                                    int pDF = 0;
                                    if (!archivoData.nombrearchivoarchivo.EndsWith(".pdf") && archivoData.ProcesarExcel)
                                    {
                                        pDF = this.ConvertToPDF(str, archivoData, null);
                                    }
                                    if (pDF != 0)
                                    {
                                        this.ReportError(archivoData, pDF);
                                    }
                                    else
                                    {
                                        archivoData.archivoresultante = this.GetFileData(archivoData);
                                        archivoData.estado = "OK";
                                        dBManager.UpdateValidacionarchivos(archivoData);
                                    }
                                    archivoData.ArchivoData = null;
                                    archivoData.archivoresultante = null;
                                }
                                else if (archivoData.accion == "A" && (archivoData.ArchivoData == null || archivoData.ArchivoData.Length == 0))
                                {
                                    archivoData.estado = "ER";
                                    archivoData.mensajeerror = "NO SE PUEDE CONVERTIR A PDF";
                                    dBManager.UpdateValidacionarchivos(archivoData);
                                }
                                if (archivoData.accion == "B" && archivoData.ArchivoData != null && archivoData.ArchivoData.Length != 0)
                                {
                                    this.CorregirNombre(archivoData);
                                    if (archivoData.nombrearchivoarchivo.EndsWith(".pdf"))
                                    {
                                        fileInfo = new FileInfo(archivoData.nombrearchivoarchivoresultante);
                                        this.CrearArchivoBase(archivoData);
                                        str = string.Concat(this.WorkingPath, "\\", fileInfo.Name);
                                        int pDF1 = this.ConvertToPDF(str, archivoData, "waterMark");
                                        if (pDF1 == 0)
                                        {
                                            pDF1 = this.AddTextMark(archivoData, str);
                                            if (pDF1 == 0)
                                            {
                                                archivoData.archivoresultante = this.GetFileData(archivoData);
                                                archivoData.estado = "OK";
                                                dBManager.UpdateValidacionarchivos(archivoData);
                                            }
                                            archivoData.ArchivoData = null;
                                            archivoData.archivoresultante = null;
                                        }
                                        else
                                        {
                                            this.ReportError(archivoData, pDF1);
                                            continue;
                                        }
                                    }
                                    else
                                    {
                                        archivoData.archivoresultante = archivoData.ArchivoData;
                                        archivoData.estado = "ER";
                                        archivoData.mensajeerror = "ARCHIVO DEBE SER PDF";
                                        dBManager.UpdateValidacionarchivos(archivoData);
                                        archivoData.archivoresultante = null;
                                        archivoData.ArchivoData = null;
                                        continue;
                                    }
                                }
                                else if (archivoData.accion == "B" && (archivoData.ArchivoData == null || archivoData.ArchivoData.Length == 0))
                                {
                                    archivoData.estado = "ER";
                                    archivoData.mensajeerror = "ARCHIVO DEBE SER PDF";
                                    dBManager.UpdateValidacionarchivos(archivoData);
                                }
                                if (archivoData.accion == "C" && archivoData.ArchivoData != null && archivoData.ArchivoData.Length != 0)
                                {
                                    this.CorregirNombre(archivoData);
                                    if (archivoData.nombrearchivoarchivo.EndsWith(".pdf"))
                                    {
                                        fileInfo = new FileInfo(archivoData.nombrearchivoarchivoresultante);
                                        this.CrearArchivoBase(archivoData);
                                        str = string.Concat(this.WorkingPath, "\\", fileInfo.Name);
                                        if (this.CompressFile(str, archivoData) == 0)
                                        {
                                            archivoData.archivoresultante = this.GetFileData(archivoData);
                                            archivoData.estado = "OK";
                                            dBManager.UpdateValidacionarchivos(archivoData);
                                        }
                                        archivoData.ArchivoData = null;
                                        archivoData.archivoresultante = null;
                                    }
                                    else
                                    {
                                        archivoData.archivoresultante = archivoData.ArchivoData;
                                        archivoData.estado = "ER";
                                        archivoData.mensajeerror = "ARCHIVO DEBE SER PDF";
                                        dBManager.UpdateValidacionarchivos(archivoData);
                                        archivoData.archivoresultante = null;
                                        archivoData.ArchivoData = null;
                                        continue;
                                    }
                                }
                                else if (archivoData.accion == "C" && (archivoData.ArchivoData == null || archivoData.ArchivoData.Length == 0))
                                {
                                    archivoData.estado = "ER";
                                    archivoData.mensajeerror = "ARCHIVO DEBE SER PDF";
                                    dBManager.UpdateValidacionarchivos(archivoData);
                                }
                                if (archivoData.accion == "AB" && archivoData.ArchivoData != null && archivoData.ArchivoData.Length != 0)
                                {
                                    this.CorregirNombre(archivoData);
                                    fileInfo = new FileInfo(archivoData.nombrearchivoarchivoresultante);
                                    this.CrearArchivoBase(archivoData);
                                    stopwatch.Stop();
                                    elapsed = stopwatch.Elapsed;
                                    this.WriteLog(string.Format("Archivo base Time: {0}h {1}m {2}s {3}ms", new object[] { elapsed.Hours, elapsed.Minutes, elapsed.Seconds, elapsed.Milliseconds }), null);
                                    stopwatch.Reset();
                                    str = string.Concat(this.WorkingPath, "\\", fileInfo.Name);
                                    int pDF2 = 0;
                                    if (!archivoData.nombrearchivoarchivo.EndsWith(".pdf") && archivoData.ProcesarExcel)
                                    {
                                        pDF2 = this.ConvertToPDF(str, archivoData, null);
                                        elapsed = stopwatch.Elapsed;
                                        this.WriteLog(string.Format("ConvertToPDF Time: {0}h {1}m {2}s {3}ms", new object[] { elapsed.Hours, elapsed.Minutes, elapsed.Seconds, elapsed.Milliseconds }), null);
                                        stopwatch.Reset();
                                    }
                                    if (pDF2 == 0)
                                    {
                                        pDF2 = this.ConvertToPDF(str, archivoData, "waterMark");
                                        elapsed = stopwatch.Elapsed;
                                        this.WriteLog(string.Format("marca de agua Time: {0}h {1}m {2}s {3}ms", new object[] { elapsed.Hours, elapsed.Minutes, elapsed.Seconds, elapsed.Milliseconds }), null);
                                        stopwatch.Reset();
                                        if (pDF2 == 0)
                                        {
                                            pDF2 = this.AddTextMark(archivoData, str);
                                            elapsed = stopwatch.Elapsed;
                                            this.WriteLog(string.Format("AddTextMark Time: {0}h {1}m {2}s {3}ms", new object[] { elapsed.Hours, elapsed.Minutes, elapsed.Seconds, elapsed.Milliseconds }), null);
                                            stopwatch.Reset();
                                            if (pDF2 == 0)
                                            {
                                                archivoData.archivoresultante = this.GetFileData(archivoData);
                                                archivoData.estado = "OK";
                                                dBManager.UpdateValidacionarchivos(archivoData);
                                                elapsed = stopwatch.Elapsed;
                                                this.WriteLog(string.Format("Save DB Time: {0}h {1}m {2}s {3}ms", new object[] { elapsed.Hours, elapsed.Minutes, elapsed.Seconds, elapsed.Milliseconds }), null);
                                                stopwatch.Reset();
                                            }
                                            archivoData.ArchivoData = null;
                                            archivoData.archivoresultante = null;
                                        }
                                        else
                                        {
                                            this.ReportError(archivoData, pDF2);
                                            continue;
                                        }
                                    }
                                    else
                                    {
                                        this.ReportError(archivoData, pDF2);
                                        continue;
                                    }
                                }
                                else if (archivoData.accion == "AB" && (archivoData.ArchivoData == null || archivoData.ArchivoData.Length == 0))
                                {
                                    archivoData.estado = "ER";
                                    archivoData.mensajeerror = "NO SE PUEDE CONVERTIR A PDF";
                                    dBManager.UpdateValidacionarchivos(archivoData);
                                }
                                if (archivoData.accion == "AC" && archivoData.ArchivoData != null && archivoData.ArchivoData.Length != 0)
                                {
                                    this.CorregirNombre(archivoData);
                                    fileInfo = new FileInfo(archivoData.nombrearchivoarchivoresultante);
                                    this.CrearArchivoBase(archivoData);
                                    str = string.Concat(this.WorkingPath, "\\", fileInfo.Name);
                                    int num2 = 0;
                                    if (!archivoData.nombrearchivoarchivo.EndsWith(".pdf") && archivoData.ProcesarExcel)
                                    {
                                        num2 = this.ConvertToPDF(str, archivoData, null);
                                    }
                                    if (num2 == 0)
                                    {
                                        num2 = this.CompressFile(str, archivoData);
                                        if (num2 == 0)
                                        {
                                            archivoData.archivoresultante = this.GetFileData(archivoData);
                                            archivoData.estado = "OK";
                                            dBManager.UpdateValidacionarchivos(archivoData);
                                        }
                                        archivoData.ArchivoData = null;
                                        archivoData.archivoresultante = null;
                                    }
                                    else
                                    {
                                        this.ReportError(archivoData, num2);
                                        continue;
                                    }
                                }
                                else if (archivoData.accion == "AC" && (archivoData.ArchivoData == null || archivoData.ArchivoData.Length == 0))
                                {
                                    archivoData.estado = "ER";
                                    archivoData.mensajeerror = "NO SE PUEDE CONVERTIR A PDF";
                                    dBManager.UpdateValidacionarchivos(archivoData);
                                }
                                if (archivoData.accion == "BC" && archivoData.ArchivoData != null && archivoData.ArchivoData.Length != 0)
                                {
                                    this.CorregirNombre(archivoData);
                                    if (archivoData.nombrearchivoarchivo.EndsWith(".pdf"))
                                    {
                                        fileInfo = new FileInfo(archivoData.nombrearchivoarchivoresultante);
                                        this.CrearArchivoBase(archivoData);
                                        str = string.Concat(this.WorkingPath, "\\", fileInfo.Name);
                                        int pDF3 = this.ConvertToPDF(str, archivoData, "waterMark");
                                        if (pDF3 == 0)
                                        {
                                            pDF3 = this.AddTextMark(archivoData, str);
                                            if (pDF3 != 0)
                                            {
                                                continue;
                                            }
                                            pDF3 = this.CompressFile(str, archivoData);
                                            if (pDF3 == 0)
                                            {
                                                archivoData.archivoresultante = this.GetFileData(archivoData);
                                                archivoData.estado = "OK";
                                                dBManager.UpdateValidacionarchivos(archivoData);
                                            }
                                            archivoData.ArchivoData = null;
                                            archivoData.archivoresultante = null;
                                        }
                                        else
                                        {
                                            this.ReportError(archivoData, pDF3);
                                            continue;
                                        }
                                    }
                                    else
                                    {
                                        archivoData.archivoresultante = archivoData.ArchivoData;
                                        archivoData.estado = "ER";
                                        archivoData.mensajeerror = "ARCHIVO DEBE SER PDF";
                                        dBManager.UpdateValidacionarchivos(archivoData);
                                        archivoData.archivoresultante = null;
                                        archivoData.ArchivoData = null;
                                        continue;
                                    }
                                }
                                else if (archivoData.accion == "BC" && (archivoData.ArchivoData == null || archivoData.ArchivoData.Length == 0))
                                {
                                    archivoData.estado = "ER";
                                    archivoData.mensajeerror = "ARCHIVO DEBE SER PDF";
                                    dBManager.UpdateValidacionarchivos(archivoData);
                                }
                                if (archivoData.accion == "ABC" && archivoData.ArchivoData != null && archivoData.ArchivoData.Length != 0)
                                {
                                    this.CorregirNombre(archivoData);
                                    fileInfo = new FileInfo(archivoData.nombrearchivoarchivoresultante);
                                    this.CrearArchivoBase(archivoData);
                                    str = string.Concat(this.WorkingPath, "\\", fileInfo.Name);
                                    int num3 = 0;
                                    if (!archivoData.nombrearchivoarchivo.EndsWith(".pdf") && archivoData.ProcesarExcel)
                                    {
                                        num3 = this.ConvertToPDF(str, archivoData, null);
                                    }
                                    if (num3 == 0)
                                    {
                                        num3 = this.ConvertToPDF(str, archivoData, "waterMark");
                                        if (num3 == 0)
                                        {
                                            num3 = this.AddTextMark(archivoData, str);
                                            if (num3 != 0)
                                            {
                                                continue;
                                            }
                                            num3 = this.CompressFile(str, archivoData);
                                            if (num3 == 0)
                                            {
                                                archivoData.archivoresultante = this.GetFileData(archivoData);
                                                archivoData.estado = "OK";
                                                dBManager.UpdateValidacionarchivos(archivoData);
                                            }
                                            archivoData.ArchivoData = null;
                                            archivoData.archivoresultante = null;
                                        }
                                        else
                                        {
                                            this.ReportError(archivoData, num3);
                                            continue;
                                        }
                                    }
                                    else
                                    {
                                        this.ReportError(archivoData, num3);
                                        continue;
                                    }
                                }
                                else if (archivoData.accion == "ABC" && (archivoData.ArchivoData == null || archivoData.ArchivoData.Length == 0))
                                {
                                    archivoData.estado = "ER";
                                    archivoData.mensajeerror = "NO SE PUEDE CONVERTIR A PDF";
                                    dBManager.UpdateValidacionarchivos(archivoData);
                                }
                                int count = num * 100 / this.globalData.Count;
                                num++;
                            }
                            else
                            {
                                this.CorregirNombre(archivoData);
                                archivoData.estado = "OK";
                                archivoData.archivoresultante = archivoData.ArchivoData;
                                dBManager.UpdateValidacionarchivos(archivoData);
                                archivoData.ArchivoData = null;
                                archivoData.archivoresultante = null;
                            }
                        }
                        else
                        {
                            return;
                        }
                    }
                    list.RemoveRange(0, list1.Count);
                }
                else
                {
                    stopwatch.Stop();
                    elapsed = stopwatch.Elapsed;
                    this.WriteLog(string.Format("Time: {0}h {1}m {2}s {3}ms", new object[] { elapsed.Hours, elapsed.Minutes, elapsed.Seconds, elapsed.Milliseconds }), null);
                    this.ClearFolder();
                    break;
                }
            }
        }

        private bool IsValidExtension(ProcessInfo itemExport)
        {
            Regex regex = new Regex("[^A-Za-z0-9á-úÁ-Ó ._-]", RegexOptions.IgnoreCase);
            string nombrearchivoarchivo = itemExport.nombrearchivoarchivo.Normalize(NormalizationForm.FormD);

            FileInfo ff;
            bool isValidName = true;
            string errorMessage = "EL ARCHIVO NO TIENE EXTENSION";
            try
            {
                ff = new FileInfo(nombrearchivoarchivo);
                if (String.IsNullOrEmpty(ff.Extension))
                {
                    isValidName = false;
                }
                else if (!string.IsNullOrEmpty(ff.Extension) && string.Compare(ff.Extension, ".crdownload", true) == 0)
                {
                    isValidName = false;
                    errorMessage = "EL ARCHIVO TIENE UNA EXTENSION INVALIDA";
                }
                else if (!String.IsNullOrEmpty(ff.Extension) && String.IsNullOrEmpty(ff.Name.Replace(ff.Extension, "")))
                {
                    isValidName = false;
                    errorMessage = "EL ARCHIVO NO TIENE UN NOMBRE APROPIADO";
                }

            }
            catch (System.ArgumentException ex)
            {
                isValidName = false;
                errorMessage = "EL ARCHIVO NO TIENE UN NOMBRE APROPIADO";
            }


            nombrearchivoarchivo = regex.Replace(nombrearchivoarchivo, "");
            if (isValidName && nombrearchivoarchivo.Contains("."))
            {
                return true;
            }
            itemExport.nombrearchivoarchivoresultante = nombrearchivoarchivo;
            itemExport.estado = "ER";
            itemExport.mensajeerror = errorMessage;
            itemExport.archivoresultante = itemExport.ArchivoData;
            DBManager.GetDBManager().UpdateValidacionarchivos(itemExport);
            itemExport.ArchivoData = null;
            itemExport.archivoresultante = null;
            return false;
        }

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

        private int AddTextMark(ProcessInfo itemExport, string fullPath)
        {
            int pDF = 0;
            if (!string.IsNullOrEmpty(itemExport.etiqueta1))
            {
                pDF = this.ConvertToPDF(fullPath, itemExport, "TextWaterMark1");
            }
            if (pDF != 0)
            {
                this.ReportError(itemExport, pDF);
                return pDF;
            }
            if (!string.IsNullOrEmpty(itemExport.etiqueta2))
            {
                pDF = this.ConvertToPDF(fullPath, itemExport, "TextWaterMark2");
            }
            if (pDF != 0)
            {
                this.ReportError(itemExport, pDF);
                return pDF;
            }
            if (!string.IsNullOrEmpty(itemExport.etiqueta3))
            {
                pDF = this.ConvertToPDF(fullPath, itemExport, "TextWaterMark3");
            }
            if (pDF != 0)
            {
                this.ReportError(itemExport, pDF);
                return pDF;
            }
            if (!string.IsNullOrEmpty(itemExport.etiqueta4))
            {
                pDF = this.ConvertToPDF(fullPath, itemExport, "TextWaterMark4");
            }
            if (pDF == 0)
            {
                return 0;
            }
            this.ReportError(itemExport, pDF);
            return pDF;
        }

        private void ReportError(ProcessInfo itemExport, int errorCode)
        {
            DBManager dBManager = DBManager.GetDBManager();
            itemExport.extensionarchivoresultant = itemExport.extensionarchivo;
            itemExport.estado = "ER";
            itemExport.mensajeerror = "NO SE PUEDE CONVERTIR A PDF";
            itemExport.archivoresultante = itemExport.ArchivoData;
            itemExport.ArchivoData = null;
            itemExport.nombrearchivoarchivoresultante = itemExport.nombrearchivoarchivo;
            dBManager.UpdateValidacionarchivos(itemExport);
        }

        private void CrearArchivoBase(ProcessInfo itemExport)
        {
            using (MemoryStream memoryStream = new MemoryStream(itemExport.ArchivoData))
            {
                FileInfo fileInfo = new FileInfo(itemExport.nombrearchivoarchivoresultante);
                using (FileStream fileStream = new FileStream(string.Concat(this.WorkingPath, "\\", fileInfo.Name), FileMode.Create, FileAccess.Write))
                {
                    byte[] numArray = new byte[memoryStream.Length];
                    memoryStream.Read(numArray, 0, (int)memoryStream.Length);
                    fileStream.Write(numArray, 0, (int)numArray.Length);
                    memoryStream.Close();
                }
            }
        }

        private void CorregirNombre(ProcessInfo itemExport)
        {
            Regex regex = new Regex("[^A-Za-z0-9á-úÁ-Ó ._-]", RegexOptions.IgnoreCase);
            itemExport.nombrearchivoarchivoresultante = itemExport.nombrearchivoarchivo.Normalize(NormalizationForm.FormD);
            itemExport.nombrearchivoarchivoresultante = regex.Replace(itemExport.nombrearchivoarchivoresultante, "");
            if (itemExport.nombrearchivoarchivoresultante.Contains("."))
            {
                string str = itemExport.nombrearchivoarchivoresultante;
                string str1 = str.Substring(0, str.LastIndexOf("."));
                string str2 = str.Substring(str.LastIndexOf("."));
                str1 = str1.Replace(".", "_");
                itemExport.nombrearchivoarchivoresultante = string.Concat(str1, str2.ToLower());
                itemExport.extensionarchivoresultant = str2.ToUpper();
                itemExport.nombrearchivoarchivo = itemExport.nombrearchivoarchivoresultante;
                itemExport.extensionarchivo = str2.ToUpper();
            }
        }

        private byte[] GetFileData(ProcessInfo itemExport)
        {
            string str = string.Concat(this.WorkingPath, "\\", itemExport.nombrearchivoarchivoresultante);
            FileInfo fileInfo = new FileInfo(str);
            byte[] numArray = null;
            using (FileStream fileStream = new FileStream(str, FileMode.Open, FileAccess.Read))
            {
                numArray = new byte[fileStream.Length];
                int length = (int)fileStream.Length;
                int num = 0;
                while (length > 0)
                {
                    int num1 = fileStream.Read(numArray, num, length);
                    if (num1 == 0)
                    {
                        break;
                    }
                    num += num1;
                    length -= num1;
                }
                length = (int)numArray.Length;
            }
            return numArray;
        }

        private void ClearFolder()
        {
            IEnumerable<FileInfo> fileInfos = (new DirectoryInfo(this.WorkingPath)).GetFiles("*.*");
            if (fileInfos.Any<FileInfo>())
            {
                foreach (FileInfo fileInfo in fileInfos)
                {
                    if (fileInfo.Extension.ToLower().Contains("bat") || fileInfo.Extension.ToLower().Contains("exe"))
                    {
                        continue;
                    }
                    fileInfo.Delete();
                }
            }
        }

        private int ConvertToPDF(string fullpath, ProcessInfo itemExport, string mode)
        {
            FileInfo fileInfo;
            int exitCode;
            int num = itemExport.fldidvalidacionarchivos;
            this.WriteLog(string.Concat("Start ConvertToPDF ", num.ToString(), " action:", itemExport.accion), null);
            string str = itemExport.accion;
            if (str.Contains("A") && mode == null)
            {
                str = "A";
                fileInfo = new FileInfo(fullpath);
                if (fileInfo.Extension.Contains(".pdf"))
                {
                    return 0;
                }
            }
            else if (str.Contains("B") && mode != null)
            {
                str = "B";
                fileInfo = new FileInfo(fullpath);
                if (!fileInfo.Extension.Contains(".pdf"))
                {
                    fullpath = string.Concat(fileInfo.DirectoryName, "\\", fileInfo.Name.Replace(fileInfo.Extension, ".pdf"));
                }
            }
            fileInfo = new FileInfo(fullpath);
            string empty = string.Empty;
            if (str == "A")
            {
                empty = string.Format(this.cmdConvertPDF, fileInfo.FullName, fileInfo.DirectoryName);
            }
            else if (str == "B")
            {
                if (mode == "waterMark")
                {
                    empty = string.Format(this.cmdWaterMark, fileInfo.FullName, fileInfo.DirectoryName, this.imgLogo);
                }
                else if (mode == "TextWaterMark1")
                {
                    empty = string.Format(this.cmdTextWaterMark1, fileInfo.FullName, fileInfo.DirectoryName, itemExport.etiqueta1);
                }
                else if (mode == "TextWaterMark2")
                {
                    empty = string.Format(this.cmdTextWaterMark2, fileInfo.FullName, fileInfo.DirectoryName, itemExport.etiqueta2);
                }
                else if (mode == "TextWaterMark3")
                {
                    empty = string.Format(this.cmdTextWaterMark3, fileInfo.FullName, fileInfo.DirectoryName, itemExport.etiqueta3);
                }
                else if (mode == "TextWaterMark4")
                {
                    empty = string.Format(this.cmdTextWaterMark4, fileInfo.FullName, fileInfo.DirectoryName, itemExport.etiqueta4);
                }
            }
            ProcessStartInfo processStartInfo = new ProcessStartInfo()
            {
                Arguments = empty,
                FileName = this.PdfExe,
                WindowStyle = ProcessWindowStyle.Normal,
                CreateNoWindow = true,
                UseShellExecute = false
            };
            this.WriteLog(string.Concat("Launching  ", processStartInfo.FileName), null);
            Stopwatch stopwatch = new Stopwatch();
            Process process = Process.Start(processStartInfo);
            Process process1 = process;
            this.proc = process;
            using (process1)
            {
                Thread.Sleep(this.secondsToPressContinue2PDF * 1000);
                this.WriteLog("WaitForExit", null);
                stopwatch.Start();
                this.proc.WaitForExit();
                stopwatch.Stop();
                TimeSpan elapsed = stopwatch.Elapsed;
                this.WriteLog(string.Format("WaitForExit Time: {0}h {1}m {2}s {3}ms", new object[] { elapsed.Hours, elapsed.Minutes, elapsed.Seconds, elapsed.Milliseconds }), null);
                exitCode = this.proc.ExitCode;
            }
            string[] strArrays = new string[] { "End ConvertToPDF ", null, null, null, null, null };
            num = itemExport.fldidvalidacionarchivos;
            strArrays[1] = num.ToString();
            strArrays[2] = " action:";
            strArrays[3] = itemExport.accion;
            strArrays[4] = " with exitCode: ";
            strArrays[5] = exitCode.ToString();
            this.WriteLog(string.Concat(strArrays), null);
            if (exitCode == 0)
            {
                if (fileInfo.Extension.ToLower().Contains(".pdf"))
                {
                    fileInfo.Delete();
                    File.Move(fileInfo.FullName.Replace(".pdf", " (1).pdf"), fileInfo.FullName);
                }
                itemExport.nombrearchivoarchivoresultante = fileInfo.Name.Replace(fileInfo.Extension, ".pdf");
                itemExport.extensionarchivoresultant = ".PDF";
                if (!File.Exists(string.Concat(fileInfo.DirectoryName, "\\", itemExport.nombrearchivoarchivoresultante)))
                {
                    this.WriteLog(string.Concat(new string[] { "Invalid 2PDF execution [", fileInfo.DirectoryName, "\\", itemExport.nombrearchivoarchivoresultante, "] no existe!" }), null);
                    this.WriteLog(empty, null);
                    return -1;
                }
            }
            num = itemExport.fldidvalidacionarchivos;
            this.WriteLog(string.Concat("End ConvertToPDF ", num.ToString(), " action:", itemExport.accion), null);
            return exitCode;
        }

        private int ConvertToPDF_BKP(string fullpath, ProcessInfo itemExport, string mode)
        {
            FileInfo fileInfo;
            int exitCode;
            int num = itemExport.fldidvalidacionarchivos;
            this.WriteLog(string.Concat("Start ConvertToPDF ", num.ToString(), " action:", itemExport.accion), null);
            string str = itemExport.accion;
            if (str.Contains("A") && mode == null)
            {
                str = "A";
                fileInfo = new FileInfo(fullpath);
                if (fileInfo.Extension.Contains(".pdf"))
                {
                    return 0;
                }
            }
            else if (str.Contains("B") && mode != null)
            {
                str = "B";
                fileInfo = new FileInfo(fullpath);
                if (!fileInfo.Extension.Contains(".pdf"))
                {
                    fullpath = string.Concat(fileInfo.DirectoryName, "\\", fileInfo.Name.Replace(fileInfo.Extension, ".pdf"));
                }
            }
            fileInfo = new FileInfo(fullpath);
            string empty = string.Empty;
            if (str == "A")
            {
                empty = string.Format(this.cmdConvertPDF, fileInfo.FullName, fileInfo.DirectoryName);
            }
            else if (str == "B")
            {
                if (mode == "waterMark")
                {
                    empty = string.Format(this.cmdWaterMark, fileInfo.FullName, fileInfo.DirectoryName, this.imgLogo);
                }
                else if (mode == "TextWaterMark1")
                {
                    empty = string.Format(this.cmdTextWaterMark1, fileInfo.FullName, fileInfo.DirectoryName, itemExport.etiqueta1);
                }
                else if (mode == "TextWaterMark2")
                {
                    empty = string.Format(this.cmdTextWaterMark2, fileInfo.FullName, fileInfo.DirectoryName, itemExport.etiqueta2);
                }
                else if (mode == "TextWaterMark3")
                {
                    empty = string.Format(this.cmdTextWaterMark3, fileInfo.FullName, fileInfo.DirectoryName, itemExport.etiqueta3);
                }
                else if (mode == "TextWaterMark4")
                {
                    empty = string.Format(this.cmdTextWaterMark4, fileInfo.FullName, fileInfo.DirectoryName, itemExport.etiqueta4);
                }
            }
            ProcessStartInfo processStartInfo = new ProcessStartInfo()
            {
                Arguments = empty,
                FileName = this.PdfExe,
                WindowStyle = ProcessWindowStyle.Normal,
                CreateNoWindow = true,
                UseShellExecute = false
            };
            this.WriteLog(string.Concat("Launching  ", processStartInfo.FileName), null);
            Process process = Process.Start(processStartInfo);
            Process process1 = process;
            this.proc = process;
            using (process1)
            {
                Thread.Sleep(this.secondsToPressContinue2PDF * 1000);
                this.WriteLog("WaitForExit", null);
                this.proc.WaitForExit();
                exitCode = this.proc.ExitCode;
            }
            string[] strArrays = new string[] { "End ConvertToPDF ", null, null, null, null, null };
            num = itemExport.fldidvalidacionarchivos;
            strArrays[1] = num.ToString();
            strArrays[2] = " action:";
            strArrays[3] = itemExport.accion;
            strArrays[4] = " with exitCode: ";
            strArrays[5] = exitCode.ToString();
            this.WriteLog(string.Concat(strArrays), null);
            if (exitCode == 0)
            {
                if (fileInfo.Extension.ToLower().Contains(".pdf"))
                {
                    fileInfo.Delete();
                    File.Move(fileInfo.FullName.Replace(".pdf", " (1).pdf"), fileInfo.FullName);
                }
                itemExport.nombrearchivoarchivoresultante = fileInfo.Name.Replace(fileInfo.Extension, ".pdf");
                itemExport.extensionarchivoresultant = ".PDF";
                if (!File.Exists(string.Concat(fileInfo.DirectoryName, "\\", itemExport.nombrearchivoarchivoresultante)))
                {
                    this.WriteLog(string.Concat(new string[] { "Invalid 2PDF execution [", fileInfo.DirectoryName, "\\", itemExport.nombrearchivoarchivoresultante, "] no existe!" }), null);
                    this.WriteLog(empty, null);
                    return -1;
                }
            }
            num = itemExport.fldidvalidacionarchivos;
            this.WriteLog(string.Concat("End ConvertToPDF ", num.ToString(), " action:", itemExport.accion), null);
            return exitCode;
        }


        private int CompressFile(string fullpath, ProcessInfo itemExport)
        {
            int exitCode;
            FileInfo fileInfo = new FileInfo(fullpath);
            if (!fileInfo.Extension.Contains(".pdf"))
            {
                fullpath = string.Concat(fileInfo.DirectoryName, "\\", fileInfo.Name.Replace(fileInfo.Extension, ".pdf"));
            }
            fileInfo = new FileInfo(fullpath);
            string str = fileInfo.Name.Replace(".pdf", " CP.pdf");
            str = string.Concat(fileInfo.DirectoryName, "\\", str);
            string str1 = string.Format(this.cmdCompresorPDF, fileInfo.FullName, str);
            ProcessStartInfo processStartInfo = new ProcessStartInfo()
            {
                Arguments = str1,
                FileName = this.PdfCompresorExe,
                WindowStyle = ProcessWindowStyle.Hidden,
                CreateNoWindow = true,
                UseShellExecute = false
            };
            Process process = Process.Start(processStartInfo);
            Process process1 = process;
            this.proc = process;
            using (process1)
            {
                this.proc.WaitForExit();
                exitCode = this.proc.ExitCode;
            }
            if (exitCode == 0 || exitCode == 1)
            {
                FileInfo fileInfo1 = new FileInfo(str);
                if (fileInfo1.Length >= fileInfo.Length)
                {
                    fileInfo1.Delete();
                }
                else
                {
                    fileInfo.Delete();
                    File.Move(str, fileInfo.FullName);
                }
                itemExport.nombrearchivoarchivoresultante = fileInfo.Name.Replace(fileInfo.Extension, ".pdf");
                itemExport.extensionarchivoresultant = ".PDF";
                exitCode = 0;
            }
            return exitCode;
        }

    }
}