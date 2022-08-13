using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SisnetValidacionArchivos
{
    public class FileConversor_
    {
        // Fields
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

        // Methods
        public FileConversor_(IntPtr ServiceHandle)
        {
            this.ServiceHandle = ServiceHandle;
            DBManager dBManager = DBManager.GetDBManager();
            try
            {
                dBManager.SetDBManager(ConfigurationManager.AppSettings["server"], ConfigurationManager.AppSettings["database"], ConfigurationManager.AppSettings["user"], ConfigurationManager.AppSettings["password"]);
            }
            catch (Exception exception)
            {
                this.WriteLog(exception.Message, null);
                throw exception;
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

        private int AddTextMark(ProcessInfo itemExport, string fullPath)
        {
            int errorCode = 0;
            if (!string.IsNullOrEmpty(itemExport.etiqueta1))
            {
                errorCode = this.ConvertToPDF(fullPath, itemExport, "TextWaterMark1");
            }
            if (errorCode != 0)
            {
                this.ReportError(itemExport, errorCode);
                return errorCode;
            }
            if (!string.IsNullOrEmpty(itemExport.etiqueta2))
            {
                errorCode = this.ConvertToPDF(fullPath, itemExport, "TextWaterMark2");
            }
            if (errorCode != 0)
            {
                this.ReportError(itemExport, errorCode);
                return errorCode;
            }
            if (!string.IsNullOrEmpty(itemExport.etiqueta3))
            {
                errorCode = this.ConvertToPDF(fullPath, itemExport, "TextWaterMark3");
            }
            if (errorCode != 0)
            {
                this.ReportError(itemExport, errorCode);
                return errorCode;
            }
            if (!string.IsNullOrEmpty(itemExport.etiqueta4))
            {
                errorCode = this.ConvertToPDF(fullPath, itemExport, "TextWaterMark4");
            }
            if (errorCode == 0)
            {
                return 0;
            }
            this.ReportError(itemExport, errorCode);
            return errorCode;
        }

        public void ChangeData()
        {
            try
            {
                this.globalData = DBManager.GetDBManager().GetPendingFiles(this.tableToValidate.ToString());
                this.ExportData();
            }
            catch (Exception exception)
            {
                this.WriteLog(exception.Message, exception);
            }
        }

        private void ClearFolder()
        {
            IEnumerable<FileInfo> source = new DirectoryInfo(this.WorkingPath).EnumerateFiles("*.*");
            if (source.Any<FileInfo>())
            {
                foreach (FileInfo info in source)
                {
                    if (!info.Extension.ToLower().Contains("bat") && !info.Extension.ToLower().Contains("exe"))
                    {
                        info.Delete();
                    }
                }
            }
        }

        private int CompressFile(string fullpath, ProcessInfo itemExport)
        {
            int exitCode;
            FileInfo info = new FileInfo(fullpath);
            if (!info.Extension.Contains(".pdf"))
            {
                fullpath = info.DirectoryName + @"\" + info.Name.Replace(info.Extension, ".pdf");
            }
            info = new FileInfo(fullpath);
            string str = info.Name.Replace(".pdf", " CP.pdf");
            str = info.DirectoryName + @"\" + str;
            ProcessStartInfo startInfo = new ProcessStartInfo
            {
                Arguments = string.Format(this.cmdCompresorPDF, info.FullName, str),
                FileName = this.PdfCompresorExe,
                WindowStyle = ProcessWindowStyle.Hidden,
                CreateNoWindow = true,
                UseShellExecute = false
            };
            using (this.proc = Process.Start(startInfo))
            {
                this.proc.WaitForExit();
                exitCode = this.proc.ExitCode;
            }
            if ((exitCode == 0) || (exitCode == 1))
            {
                FileInfo info3 = new FileInfo(str);
                if (info3.Length >= info.Length)
                {
                    info3.Delete();
                }
                else
                {
                    info.Delete();
                    File.Move(str, info.FullName);
                }
                itemExport.nombrearchivoarchivoresultante = info.Name.Replace(info.Extension, ".pdf");
                itemExport.extensionarchivoresultant = ".PDF";
                exitCode = 0;
            }
            return exitCode;
        }

        private int ConvertToPDF(string fullpath, ProcessInfo itemExport, string mode)
        {
            FileInfo info;
            int exitCode;
            this.WriteLog("Start ConvertToPDF " + itemExport.fldidvalidacionarchivos.ToString() + " action:" + itemExport.accion, null);
            string accion = itemExport.accion;
            if (accion.Contains("A") && (mode == null))
            {
                accion = "A";
                if (new FileInfo(fullpath).Extension.Contains(".pdf"))
                {
                    return 0;
                }
            }
            else if (accion.Contains("B") && (mode != null))
            {
                accion = "B";
                info = new FileInfo(fullpath);
                if (!info.Extension.Contains(".pdf"))
                {
                    fullpath = info.DirectoryName + @"\" + info.Name.Replace(info.Extension, ".pdf");
                }
            }
            info = new FileInfo(fullpath);
            string text = string.Empty;
            if (accion == "A")
            {
                text = string.Format(this.cmdConvertPDF, info.FullName, info.DirectoryName);
            }
            else if (accion == "B")
            {
                if (mode == "waterMark")
                {
                    text = string.Format(this.cmdWaterMark, info.FullName, info.DirectoryName, this.imgLogo);
                }
                else if (mode == "TextWaterMark1")
                {
                    text = string.Format(this.cmdTextWaterMark1, info.FullName, info.DirectoryName, itemExport.etiqueta1);
                }
                else if (mode == "TextWaterMark2")
                {
                    text = string.Format(this.cmdTextWaterMark2, info.FullName, info.DirectoryName, itemExport.etiqueta2);
                }
                else if (mode == "TextWaterMark3")
                {
                    text = string.Format(this.cmdTextWaterMark3, info.FullName, info.DirectoryName, itemExport.etiqueta3);
                }
                else if (mode == "TextWaterMark4")
                {
                    text = string.Format(this.cmdTextWaterMark4, info.FullName, info.DirectoryName, itemExport.etiqueta4);
                }
            }
            ProcessStartInfo startInfo = new ProcessStartInfo
            {
                Arguments = text,
                FileName = this.PdfExe,
                WindowStyle = ProcessWindowStyle.Normal,
                CreateNoWindow = true,
                UseShellExecute = false
            };
            this.WriteLog("Launching  " + startInfo.FileName, null);
            Stopwatch stopwatch = new Stopwatch();
            using (this.proc = Process.Start(startInfo))
            {
                Thread.Sleep((int)(this.secondsToPressContinue2PDF * 0x3e8));
                this.WriteLog("WaitForExit", null);
                stopwatch.Start();
                this.proc.WaitForExit();
                stopwatch.Stop();
                TimeSpan elapsed = stopwatch.Elapsed;
                this.WriteLog($"WaitForExit Time: {elapsed.Hours}h {elapsed.Minutes}m {elapsed.Seconds}s {elapsed.Milliseconds}ms", null);
                exitCode = this.proc.ExitCode;
            }
            string[] textArray1 = new string[] { "End ConvertToPDF ", itemExport.fldidvalidacionarchivos.ToString(), " action:", itemExport.accion, " with exitCode: ", exitCode.ToString() };
            this.WriteLog(string.Concat(textArray1), null);
            if (exitCode == 0)
            {
                if (info.Extension.ToLower().Contains(".pdf"))
                {
                    info.Delete();
                    File.Move(info.FullName.Replace(".pdf", " (1).pdf"), info.FullName);
                }
                itemExport.nombrearchivoarchivoresultante = info.Name.Replace(info.Extension, ".pdf");
                itemExport.extensionarchivoresultant = ".PDF";
                if (!File.Exists(info.DirectoryName + @"\" + itemExport.nombrearchivoarchivoresultante))
                {
                    string[] textArray2 = new string[] { "Invalid 2PDF execution [", info.DirectoryName, @"\", itemExport.nombrearchivoarchivoresultante, "] no existe!" };
                    this.WriteLog(string.Concat(textArray2), null);
                    this.WriteLog(text, null);
                    return -1;
                }
            }
            this.WriteLog("End ConvertToPDF " + itemExport.fldidvalidacionarchivos.ToString() + " action:" + itemExport.accion, null);
            return exitCode;
        }

        private int ConvertToPDF_BKP(string fullpath, ProcessInfo itemExport, string mode)
        {
            FileInfo info;
            int exitCode;
            this.WriteLog("Start ConvertToPDF " + itemExport.fldidvalidacionarchivos.ToString() + " action:" + itemExport.accion, null);
            string accion = itemExport.accion;
            if (accion.Contains("A") && (mode == null))
            {
                accion = "A";
                if (new FileInfo(fullpath).Extension.Contains(".pdf"))
                {
                    return 0;
                }
            }
            else if (accion.Contains("B") && (mode != null))
            {
                accion = "B";
                info = new FileInfo(fullpath);
                if (!info.Extension.Contains(".pdf"))
                {
                    fullpath = info.DirectoryName + @"\" + info.Name.Replace(info.Extension, ".pdf");
                }
            }
            info = new FileInfo(fullpath);
            string text = string.Empty;
            if (accion == "A")
            {
                text = string.Format(this.cmdConvertPDF, info.FullName, info.DirectoryName);
            }
            else if (accion == "B")
            {
                if (mode == "waterMark")
                {
                    text = string.Format(this.cmdWaterMark, info.FullName, info.DirectoryName, this.imgLogo);
                }
                else if (mode == "TextWaterMark1")
                {
                    text = string.Format(this.cmdTextWaterMark1, info.FullName, info.DirectoryName, itemExport.etiqueta1);
                }
                else if (mode == "TextWaterMark2")
                {
                    text = string.Format(this.cmdTextWaterMark2, info.FullName, info.DirectoryName, itemExport.etiqueta2);
                }
                else if (mode == "TextWaterMark3")
                {
                    text = string.Format(this.cmdTextWaterMark3, info.FullName, info.DirectoryName, itemExport.etiqueta3);
                }
                else if (mode == "TextWaterMark4")
                {
                    text = string.Format(this.cmdTextWaterMark4, info.FullName, info.DirectoryName, itemExport.etiqueta4);
                }
            }
            ProcessStartInfo startInfo = new ProcessStartInfo
            {
                Arguments = text,
                FileName = this.PdfExe,
                WindowStyle = ProcessWindowStyle.Normal,
                CreateNoWindow = true,
                UseShellExecute = false
            };
            this.WriteLog("Launching  " + startInfo.FileName, null);
            using (this.proc = Process.Start(startInfo))
            {
                Thread.Sleep((int)(this.secondsToPressContinue2PDF * 0x3e8));
                this.WriteLog("WaitForExit", null);
                this.proc.WaitForExit();
                exitCode = this.proc.ExitCode;
            }
            string[] textArray1 = new string[] { "End ConvertToPDF ", itemExport.fldidvalidacionarchivos.ToString(), " action:", itemExport.accion, " with exitCode: ", exitCode.ToString() };
            this.WriteLog(string.Concat(textArray1), null);
            if (exitCode == 0)
            {
                if (info.Extension.ToLower().Contains(".pdf"))
                {
                    info.Delete();
                    File.Move(info.FullName.Replace(".pdf", " (1).pdf"), info.FullName);
                }
                itemExport.nombrearchivoarchivoresultante = info.Name.Replace(info.Extension, ".pdf");
                itemExport.extensionarchivoresultant = ".PDF";
                if (!File.Exists(info.DirectoryName + @"\" + itemExport.nombrearchivoarchivoresultante))
                {
                    string[] textArray2 = new string[] { "Invalid 2PDF execution [", info.DirectoryName, @"\", itemExport.nombrearchivoarchivoresultante, "] no existe!" };
                    this.WriteLog(string.Concat(textArray2), null);
                    this.WriteLog(text, null);
                    return -1;
                }
            }
            this.WriteLog("End ConvertToPDF " + itemExport.fldidvalidacionarchivos.ToString() + " action:" + itemExport.accion, null);
            return exitCode;
        }

        private void CorregirNombre(ProcessInfo itemExport)
        {
            Regex regex = new Regex("[^A-Za-z0-9\x00e1-\x00fa\x00c1-\x00d3 ._-]", RegexOptions.IgnoreCase);
            itemExport.nombrearchivoarchivoresultante = itemExport.nombrearchivoarchivo.Normalize(NormalizationForm.FormD);
            itemExport.nombrearchivoarchivoresultante = regex.Replace(itemExport.nombrearchivoarchivoresultante, "");
            if (itemExport.nombrearchivoarchivoresultante.Contains("."))
            {
                string nombrearchivoarchivoresultante = itemExport.nombrearchivoarchivoresultante;
                string str3 = nombrearchivoarchivoresultante.Substring(nombrearchivoarchivoresultante.LastIndexOf("."));
                string str2 = nombrearchivoarchivoresultante.Substring(0, nombrearchivoarchivoresultante.LastIndexOf(".")).Replace(".", "_");
                itemExport.nombrearchivoarchivoresultante = str2 + str3.ToLower();
                itemExport.extensionarchivoresultant = str3.ToUpper();
                itemExport.nombrearchivoarchivo = itemExport.nombrearchivoarchivoresultante;
                itemExport.extensionarchivo = str3.ToUpper();
            }
        }

        private void CrearArchivoBase(ProcessInfo itemExport)
        {
            using (MemoryStream stream = new MemoryStream(itemExport.ArchivoData))
            {
                FileInfo info = new FileInfo(itemExport.nombrearchivoarchivoresultante);
                using (FileStream stream2 = new FileStream(this.WorkingPath + @"\" + info.Name, FileMode.Create, FileAccess.Write))
                {
                    byte[] buffer = new byte[stream.Length];
                    stream.Read(buffer, 0, (int)stream.Length);
                    stream2.Write(buffer, 0, buffer.Length);
                    stream.Close();
                }
            }
        }

        private void ExportData()
        {
            if (this.globalData.Count != 0)
            {
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();
                DBManager dBManager = DBManager.GetDBManager();
                List<string> list1 = new List<string>();
                Func<ProcessInfo, string> selector = <> c.<> 9__22_0;
                if (<> c.<> 9__22_0 == null)
                {
                    Func<ProcessInfo, string> local1 = <> c.<> 9__22_0;
                    selector = <> c.<> 9__22_0 = drv => "'" + drv.fldidvalidacionarchivos.ToString() + "'";
                }
                List<string> source = this.globalData.Select<ProcessInfo, string>(selector).ToList<string>();
                DirectoryInfo info2 = new DirectoryInfo(this.WorkingPath);
                if (!info2.Exists)
                {
                    this.WriteLog("Creating directory " + this.WorkingPath, null);
                    info2.Create();
                }
                int num = 0;
                while (true)
                {
                    List<string> list2;
                    while (true)
                    {
                        TimeSpan elapsed;
                        if (source.Any<string>())
                        {
                            this.ClearFolder();
                            list2 = source.Take<string>(50).ToList<string>();
                            string fldidvalidacionarchivos = string.Join(",", list2);
                            using (List<ProcessInfo>.Enumerator enumerator = dBManager.GetDataFileToConvert(this.tableToValidate.ToString(), fldidvalidacionarchivos).GetEnumerator())
                            {
                                while (true)
                                {
                                    if (enumerator.MoveNext())
                                    {
                                        ProcessInfo itemExportFile = enumerator.Current;
                                        if (!this.StopSignal)
                                        {
                                            FileInfo info;
                                            string str;
                                            ProcessInfo itemExport = (from toProcess in this.globalData
                                                                      where toProcess.fldidvalidacionarchivos == itemExportFile.fldidvalidacionarchivos
                                                                      select toProcess).Single<ProcessInfo>();
                                            itemExport.ArchivoData = itemExportFile.ArchivoData;
                                            this.WriteLog("Processing " + itemExport.fldidvalidacionarchivos.ToString() + " action:" + itemExport.accion, null);
                                            if (!this.IsValidExtension(itemExport))
                                            {
                                                continue;
                                            }
                                            if (string.IsNullOrEmpty(itemExport.accion))
                                            {
                                                this.CorregirNombre(itemExport);
                                                itemExport.estado = "OK";
                                                itemExport.archivoresultante = itemExport.ArchivoData;
                                                dBManager.UpdateValidacionarchivos(itemExport);
                                                itemExport.ArchivoData = null;
                                                itemExport.archivoresultante = null;
                                                continue;
                                            }
                                            if ((itemExport.accion != "A") || ((itemExport.ArchivoData == null) || (itemExport.ArchivoData.Length == 0)))
                                            {
                                                if ((itemExport.accion == "A") && ((itemExport.ArchivoData == null) || (itemExport.ArchivoData.Length == 0)))
                                                {
                                                    itemExport.estado = "ER";
                                                    itemExport.mensajeerror = "NO SE PUEDE CONVERTIR A PDF";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                }
                                            }
                                            else
                                            {
                                                this.CorregirNombre(itemExport);
                                                info = new FileInfo(itemExport.nombrearchivoarchivoresultante);
                                                this.CrearArchivoBase(itemExport);
                                                str = this.WorkingPath + @"\" + info.Name;
                                                int errorCode = 0;
                                                if (!itemExport.nombrearchivoarchivo.EndsWith(".pdf") && itemExport.ProcesarExcel)
                                                {
                                                    errorCode = this.ConvertToPDF(str, itemExport, null);
                                                }
                                                if (errorCode != 0)
                                                {
                                                    this.ReportError(itemExport, errorCode);
                                                }
                                                else
                                                {
                                                    itemExport.archivoresultante = this.GetFileData(itemExport);
                                                    itemExport.estado = "OK";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                }
                                                itemExport.ArchivoData = null;
                                                itemExport.archivoresultante = null;
                                            }
                                            if ((itemExport.accion != "B") || ((itemExport.ArchivoData == null) || (itemExport.ArchivoData.Length == 0)))
                                            {
                                                if ((itemExport.accion == "B") && ((itemExport.ArchivoData == null) || (itemExport.ArchivoData.Length == 0)))
                                                {
                                                    itemExport.estado = "ER";
                                                    itemExport.mensajeerror = "ARCHIVO DEBE SER PDF";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                }
                                            }
                                            else
                                            {
                                                this.CorregirNombre(itemExport);
                                                if (!itemExport.nombrearchivoarchivo.EndsWith(".pdf"))
                                                {
                                                    itemExport.archivoresultante = itemExport.ArchivoData;
                                                    itemExport.estado = "ER";
                                                    itemExport.mensajeerror = "ARCHIVO DEBE SER PDF";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                    itemExport.archivoresultante = null;
                                                    itemExport.ArchivoData = null;
                                                    continue;
                                                }
                                                info = new FileInfo(itemExport.nombrearchivoarchivoresultante);
                                                this.CrearArchivoBase(itemExport);
                                                str = this.WorkingPath + @"\" + info.Name;
                                                int errorCode = this.ConvertToPDF(str, itemExport, "waterMark");
                                                if (errorCode != 0)
                                                {
                                                    this.ReportError(itemExport, errorCode);
                                                    continue;
                                                }
                                                if (this.AddTextMark(itemExport, str) == 0)
                                                {
                                                    itemExport.archivoresultante = this.GetFileData(itemExport);
                                                    itemExport.estado = "OK";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                }
                                                itemExport.ArchivoData = null;
                                                itemExport.archivoresultante = null;
                                            }
                                            if ((itemExport.accion != "C") || ((itemExport.ArchivoData == null) || (itemExport.ArchivoData.Length == 0)))
                                            {
                                                if ((itemExport.accion == "C") && ((itemExport.ArchivoData == null) || (itemExport.ArchivoData.Length == 0)))
                                                {
                                                    itemExport.estado = "ER";
                                                    itemExport.mensajeerror = "ARCHIVO DEBE SER PDF";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                }
                                            }
                                            else
                                            {
                                                this.CorregirNombre(itemExport);
                                                if (!itemExport.nombrearchivoarchivo.EndsWith(".pdf"))
                                                {
                                                    itemExport.archivoresultante = itemExport.ArchivoData;
                                                    itemExport.estado = "ER";
                                                    itemExport.mensajeerror = "ARCHIVO DEBE SER PDF";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                    itemExport.archivoresultante = null;
                                                    itemExport.ArchivoData = null;
                                                    continue;
                                                }
                                                info = new FileInfo(itemExport.nombrearchivoarchivoresultante);
                                                this.CrearArchivoBase(itemExport);
                                                str = this.WorkingPath + @"\" + info.Name;
                                                if (this.CompressFile(str, itemExport) == 0)
                                                {
                                                    itemExport.archivoresultante = this.GetFileData(itemExport);
                                                    itemExport.estado = "OK";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                }
                                                itemExport.ArchivoData = null;
                                                itemExport.archivoresultante = null;
                                            }
                                            if ((itemExport.accion != "AB") || ((itemExport.ArchivoData == null) || (itemExport.ArchivoData.Length == 0)))
                                            {
                                                if ((itemExport.accion == "AB") && ((itemExport.ArchivoData == null) || (itemExport.ArchivoData.Length == 0)))
                                                {
                                                    itemExport.estado = "ER";
                                                    itemExport.mensajeerror = "NO SE PUEDE CONVERTIR A PDF";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                }
                                            }
                                            else
                                            {
                                                this.CorregirNombre(itemExport);
                                                info = new FileInfo(itemExport.nombrearchivoarchivoresultante);
                                                this.CrearArchivoBase(itemExport);
                                                stopwatch.Stop();
                                                elapsed = stopwatch.Elapsed;
                                                this.WriteLog($"Archivo base Time: {elapsed.Hours}h {elapsed.Minutes}m {elapsed.Seconds}s {elapsed.Milliseconds}ms", null);
                                                stopwatch.Restart();
                                                str = this.WorkingPath + @"\" + info.Name;
                                                int errorCode = 0;
                                                if (!itemExport.nombrearchivoarchivo.EndsWith(".pdf") && itemExport.ProcesarExcel)
                                                {
                                                    errorCode = this.ConvertToPDF(str, itemExport, null);
                                                    elapsed = stopwatch.Elapsed;
                                                    this.WriteLog($"ConvertToPDF Time: {elapsed.Hours}h {elapsed.Minutes}m {elapsed.Seconds}s {elapsed.Milliseconds}ms", null);
                                                    stopwatch.Restart();
                                                }
                                                if (errorCode != 0)
                                                {
                                                    this.ReportError(itemExport, errorCode);
                                                    continue;
                                                }
                                                errorCode = this.ConvertToPDF(str, itemExport, "waterMark");
                                                elapsed = stopwatch.Elapsed;
                                                this.WriteLog($"marca de agua Time: {elapsed.Hours}h {elapsed.Minutes}m {elapsed.Seconds}s {elapsed.Milliseconds}ms", null);
                                                stopwatch.Restart();
                                                if (errorCode != 0)
                                                {
                                                    this.ReportError(itemExport, errorCode);
                                                    continue;
                                                }
                                                errorCode = this.AddTextMark(itemExport, str);
                                                elapsed = stopwatch.Elapsed;
                                                this.WriteLog($"AddTextMark Time: {elapsed.Hours}h {elapsed.Minutes}m {elapsed.Seconds}s {elapsed.Milliseconds}ms", null);
                                                stopwatch.Restart();
                                                if (errorCode == 0)
                                                {
                                                    itemExport.archivoresultante = this.GetFileData(itemExport);
                                                    itemExport.estado = "OK";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                    elapsed = stopwatch.Elapsed;
                                                    this.WriteLog($"Save DB Time: {elapsed.Hours}h {elapsed.Minutes}m {elapsed.Seconds}s {elapsed.Milliseconds}ms", null);
                                                    stopwatch.Restart();
                                                }
                                                itemExport.ArchivoData = null;
                                                itemExport.archivoresultante = null;
                                            }
                                            if ((itemExport.accion != "AC") || ((itemExport.ArchivoData == null) || (itemExport.ArchivoData.Length == 0)))
                                            {
                                                if ((itemExport.accion == "AC") && ((itemExport.ArchivoData == null) || (itemExport.ArchivoData.Length == 0)))
                                                {
                                                    itemExport.estado = "ER";
                                                    itemExport.mensajeerror = "NO SE PUEDE CONVERTIR A PDF";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                }
                                            }
                                            else
                                            {
                                                this.CorregirNombre(itemExport);
                                                info = new FileInfo(itemExport.nombrearchivoarchivoresultante);
                                                this.CrearArchivoBase(itemExport);
                                                str = this.WorkingPath + @"\" + info.Name;
                                                int errorCode = 0;
                                                if (!itemExport.nombrearchivoarchivo.EndsWith(".pdf") && itemExport.ProcesarExcel)
                                                {
                                                    errorCode = this.ConvertToPDF(str, itemExport, null);
                                                }
                                                if (errorCode != 0)
                                                {
                                                    this.ReportError(itemExport, errorCode);
                                                    continue;
                                                }
                                                if (this.CompressFile(str, itemExport) == 0)
                                                {
                                                    itemExport.archivoresultante = this.GetFileData(itemExport);
                                                    itemExport.estado = "OK";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                }
                                                itemExport.ArchivoData = null;
                                                itemExport.archivoresultante = null;
                                            }
                                            if ((itemExport.accion != "BC") || ((itemExport.ArchivoData == null) || (itemExport.ArchivoData.Length == 0)))
                                            {
                                                if ((itemExport.accion == "BC") && ((itemExport.ArchivoData == null) || (itemExport.ArchivoData.Length == 0)))
                                                {
                                                    itemExport.estado = "ER";
                                                    itemExport.mensajeerror = "ARCHIVO DEBE SER PDF";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                }
                                            }
                                            else
                                            {
                                                this.CorregirNombre(itemExport);
                                                if (!itemExport.nombrearchivoarchivo.EndsWith(".pdf"))
                                                {
                                                    itemExport.archivoresultante = itemExport.ArchivoData;
                                                    itemExport.estado = "ER";
                                                    itemExport.mensajeerror = "ARCHIVO DEBE SER PDF";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                    itemExport.archivoresultante = null;
                                                    itemExport.ArchivoData = null;
                                                    continue;
                                                }
                                                info = new FileInfo(itemExport.nombrearchivoarchivoresultante);
                                                this.CrearArchivoBase(itemExport);
                                                str = this.WorkingPath + @"\" + info.Name;
                                                int errorCode = this.ConvertToPDF(str, itemExport, "waterMark");
                                                if (errorCode != 0)
                                                {
                                                    this.ReportError(itemExport, errorCode);
                                                    continue;
                                                }
                                                if (this.AddTextMark(itemExport, str) != 0)
                                                {
                                                    continue;
                                                }
                                                if (this.CompressFile(str, itemExport) == 0)
                                                {
                                                    itemExport.archivoresultante = this.GetFileData(itemExport);
                                                    itemExport.estado = "OK";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                }
                                                itemExport.ArchivoData = null;
                                                itemExport.archivoresultante = null;
                                            }
                                            if ((itemExport.accion != "ABC") || ((itemExport.ArchivoData == null) || (itemExport.ArchivoData.Length == 0)))
                                            {
                                                if ((itemExport.accion == "ABC") && ((itemExport.ArchivoData == null) || (itemExport.ArchivoData.Length == 0)))
                                                {
                                                    itemExport.estado = "ER";
                                                    itemExport.mensajeerror = "NO SE PUEDE CONVERTIR A PDF";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                }
                                            }
                                            else
                                            {
                                                this.CorregirNombre(itemExport);
                                                info = new FileInfo(itemExport.nombrearchivoarchivoresultante);
                                                this.CrearArchivoBase(itemExport);
                                                str = this.WorkingPath + @"\" + info.Name;
                                                int errorCode = 0;
                                                if (!itemExport.nombrearchivoarchivo.EndsWith(".pdf") && itemExport.ProcesarExcel)
                                                {
                                                    errorCode = this.ConvertToPDF(str, itemExport, null);
                                                }
                                                if (errorCode != 0)
                                                {
                                                    this.ReportError(itemExport, errorCode);
                                                    continue;
                                                }
                                                errorCode = this.ConvertToPDF(str, itemExport, "waterMark");
                                                if (errorCode != 0)
                                                {
                                                    this.ReportError(itemExport, errorCode);
                                                    continue;
                                                }
                                                if (this.AddTextMark(itemExport, str) != 0)
                                                {
                                                    continue;
                                                }
                                                if (this.CompressFile(str, itemExport) == 0)
                                                {
                                                    itemExport.archivoresultante = this.GetFileData(itemExport);
                                                    itemExport.estado = "OK";
                                                    dBManager.UpdateValidacionarchivos(itemExport);
                                                }
                                                itemExport.ArchivoData = null;
                                                itemExport.archivoresultante = null;
                                            }
                                            int num1 = (num * 100) / this.globalData.Count;
                                            num++;
                                            continue;
                                        }
                                    }
                                    else
                                    {
                                        break;
                                    }
                                    break;
                                }
                            }
                            return;
                        }
                        else
                        {
                            stopwatch.Stop();
                            elapsed = stopwatch.Elapsed;
                            this.WriteLog($"Time: {elapsed.Hours}h {elapsed.Minutes}m {elapsed.Seconds}s {elapsed.Milliseconds}ms", null);
                            this.ClearFolder();
                            return;
                        }
                        break;
                    }
                    source.RemoveRange(0, list2.Count);
                }
            }
        }

        private string GetCurrentDirectory() =>
            AppDomain.CurrentDomain.BaseDirectory;

        private byte[] GetFileData(ProcessInfo itemExport)
        {
            FileInfo info1 = new FileInfo(this.WorkingPath + @"\" + itemExport.nombrearchivoarchivoresultante);
            byte[] buffer = null;
            using (FileStream stream = new FileStream(this.WorkingPath + @"\" + itemExport.nombrearchivoarchivoresultante, FileMode.Open, FileAccess.Read))
            {
                buffer = new byte[stream.Length];
                int length = (int)stream.Length;
                int offset = 0;
                while (true)
                {
                    if (length > 0)
                    {
                        int num3 = stream.Read(buffer, offset, length);
                        if (num3 != 0)
                        {
                            offset += num3;
                            length -= num3;
                            continue;
                        }
                    }
                    length = buffer.Length;
                    break;
                }
            }
            return buffer;
        }

        private bool IsValidExtension(ProcessInfo itemExport)
        {
            string str = new Regex("[^A-Za-z0-9\x00e1-\x00fa\x00c1-\x00d3 ._-]", RegexOptions.IgnoreCase).Replace(itemExport.nombrearchivoarchivo.Normalize(NormalizationForm.FormD), "");
            if (str.Contains("."))
            {
                return true;
            }
            itemExport.nombrearchivoarchivoresultante = str;
            itemExport.estado = "ER";
            itemExport.mensajeerror = "EL ARCHIVO NO TIENE EXTENSION";
            itemExport.archivoresultante = itemExport.ArchivoData;
            DBManager.GetDBManager().UpdateValidacionarchivos(itemExport);
            itemExport.ArchivoData = null;
            itemExport.archivoresultante = null;
            return false;
        }

        private void ReportError(ProcessInfo itemExport, int errorCode)
        {
            itemExport.extensionarchivoresultant = itemExport.extensionarchivo;
            itemExport.estado = "ER";
            itemExport.mensajeerror = "NO SE PUEDE CONVERTIR A PDF";
            itemExport.archivoresultante = itemExport.ArchivoData;
            itemExport.ArchivoData = null;
            itemExport.nombrearchivoarchivoresultante = itemExport.nombrearchivoarchivo;
            DBManager.GetDBManager().UpdateValidacionarchivos(itemExport);
        }

        [DllImport("user32.dll")]
        public static extern bool SetForegroundWindow(IntPtr hWnd);
        [DllImport("user32.dll", SetLastError = true)]
        public static extern IntPtr SetParent(IntPtr hWndChild, IntPtr hWndNewParent);
        private void WriteLog(string text, Exception ex = null)
        {
            if (this.log && text.EndsWith("ms"))
            {
                string path = this.GetCurrentDirectory() + "log.txt";
                File.AppendAllText(path, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " " + text + "\r\n");
                if (ex != null)
                {
                    File.AppendAllText(path, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " " + ex.StackTrace + "\r\n");
                }
            }
        }

        // Nested Types
        [Serializable, CompilerGenerated]
        private sealed class <>c
    {
        // Fields
        public static readonly FileConversor_.<>c<>9 = new FileConversor_.<>FileConversor_();
        public static Func<ProcessInfo, string> <>9__22_0;

        // Methods
        internal string <ExportData>FileConversor_(ProcessInfo drv) =>
            "'" + drv.fldidvalidacionarchivos.ToString() + "'";
    }
}

 

}
