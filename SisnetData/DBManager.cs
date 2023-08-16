using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;

namespace SisnetData
{
    public class DBManager
    {
        // Fields
        public static DBManager _self;
        private NpgsqlConnection connection;

        // Methods
        internal List<ExportInfo> GetData(string tableName, DataTable dataToExport, string consecutivoField, string arhivoNameField, string archivoField)
        {
            List<ExportInfo> list = new List<ExportInfo>();
            string message = string.Empty;
            try
            {
                List<string> list2 = new List<string>();
                foreach (DataRow row in dataToExport.Rows)
                {
                    list2.Add("'" + row["Consecutivo"].ToString() + "'");
                }
                string str2 = string.Join(",", list2.ToArray());
                this.connection.Open();
                string[] textArray1 = new string[15];
                textArray1[0] = "select cast(";
                textArray1[1] = consecutivoField;
                textArray1[2] = " as text) AS ";
                textArray1[3] = consecutivoField;
                textArray1[4] = ", ";
                textArray1[5] = arhivoNameField;
                textArray1[6] = ", cast((length(";
                textArray1[7] = archivoField;
                textArray1[8] = ") / 1048576.0) as text)|| ' MB' as filesize from ";
                textArray1[9] = tableName;
                textArray1[10] = " where cast(";
                textArray1[11] = consecutivoField;
                textArray1[12] = " as text) in(";
                textArray1[13] = str2;
                textArray1[14] = ") ;";
                using (NpgsqlDataReader reader = new NpgsqlCommand(string.Concat(textArray1), this.connection).ExecuteReader())
                {
                    while (reader.Read())
                    {
                        ExportInfo info1 = new ExportInfo();
                        info1.Consecutivo = reader.GetString(0);
                        info1.ArchivoName = reader.GetString(1);
                        info1.ArchivoLength = reader.GetString(2);
                        ExportInfo item = info1;
                        list.Add(item);
                    }
                }
            }
            catch (Exception exception1)
            {
                throw new Exception(message, exception1);
            }
            finally
            {
                if (this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }
            return list;
        }

        internal List<ExportInfo> GetDataFile(string tableName, DataTable dataToExport, string consecutivoField, string arhivoNameField, string archivoField, string consecutivos)
        {
            List<ExportInfo> list = new List<ExportInfo>();
            string message = string.Empty;
            try
            {
                this.connection.Open();
                string[] textArray1 = new string[0x11];
                textArray1[0] = "select cast(";
                textArray1[1] = consecutivoField;
                textArray1[2] = " as text) AS ";
                textArray1[3] = consecutivoField;
                textArray1[4] = ", ";
                textArray1[5] = arhivoNameField;
                textArray1[6] = ", cast((length(";
                textArray1[7] = archivoField;
                textArray1[8] = ") / 1048576.0) as text)|| ' MB' as filesize,";
                textArray1[9] = archivoField;
                textArray1[10] = " from ";
                textArray1[11] = tableName;
                textArray1[12] = " where cast(";
                textArray1[13] = consecutivoField;
                textArray1[14] = " as text) in(";
                textArray1[15] = consecutivos;
                textArray1[0x10] = ") ;";
                using (NpgsqlDataReader reader = new NpgsqlCommand(string.Concat(textArray1), this.connection).ExecuteReader())
                {
                    while (reader.Read())
                    {
                        ExportInfo info1 = new ExportInfo();
                        info1.Consecutivo = reader.GetString(0);
                        info1.ArchivoName = reader.GetString(1);
                        info1.ArchivoLength = reader.GetString(2);
                        info1.ArchivoData = (byte[])reader[3];
                        ExportInfo item = info1;
                        list.Add(item);
                    }
                }
            }
            catch (Exception exception1)
            {
                throw new Exception(message, exception1);
            }
            finally
            {
                if (this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }
            return list;
        }

        public List<ProcessInfo> GetDataFileToConvert(string tableName, string fldidvalidacionarchivos)
        {
            List<ProcessInfo> list = new List<ProcessInfo>();
#if DEBUG
            ProcessInfo itemTest = new ProcessInfo();
            var path = "E:\\32-27-813.zip";

            byte[]  byt = System.IO.File.ReadAllBytes(path);

            itemTest.fldidvalidacionarchivos = 1;
            itemTest.ArchivoData = byt;
            list.Add(itemTest);
            return list;
#endif
            string message = string.Empty;
            try
            {
                this.connection.Open();
                string[] textArray1 = new string[] { "select fldidvalidacionarchivos, archivo from ", tableName, " where  cast(fldidvalidacionarchivos as text) in(", fldidvalidacionarchivos, ") ;" };
                using (NpgsqlDataReader reader = new NpgsqlCommand(string.Concat(textArray1), this.connection).ExecuteReader())
                {
                    while (reader.Read())
                    {
                        ProcessInfo item = new ProcessInfo();
                        item.fldidvalidacionarchivos = reader.GetInt32(0);
                        item.ArchivoData = reader.IsDBNull(1) ? null : ((byte[])reader[1]);
                        list.Add(item);
                    }
                }
            }
            catch (Exception exception1)
            {
                throw new Exception(message, exception1);
            }
            finally
            {
                if (this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }
            return list;
        }

        public static DBManager GetDBManager()
        {
            if (DBManager._self == null)
                DBManager._self = new DBManager();
            return DBManager._self;
        }

        public Dictionary<string, string> GetFields(string tableName)
        {
            Dictionary<string, string> dictionary = new Dictionary<string, string>();
            try
            {
                this.connection.Open();
                using (NpgsqlDataReader reader = new NpgsqlCommand("select column_name, data_type from information_schema.columns where table_name = '" + tableName + "' order by column_name;", this.connection).ExecuteReader())
                {
                    while (reader.Read())
                    {
                        dictionary.Add(reader.GetString(0), reader.GetString(1));
                    }
                }
            }
            catch (Exception exception1)
            {
                throw exception1;
            }
            finally
            {
                if (this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }
            return dictionary;
        }
        private bool log;

        private string GetCurrentDirectory() => AppDomain.CurrentDomain.BaseDirectory;

        private void WriteLog(string text, Exception ex = null)
        {
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

        public List<ProcessInfo> GetPendingFiles(string tableName)
        {
            List<ProcessInfo> list = new List<ProcessInfo>();
            string str = string.Empty;
#if DEBUG
            ProcessInfo itemTest = new ProcessInfo
            {
                fldidvalidacionarchivos = 1,
                fecha = DateTime.Now,
                tipo = "",
                numeroidentificacion = null,
                nombrearchivoarchivo = "._AURAMILENAVELANDIA",
                etiqueta = null,
                accion = null,
                calidadpdf = 0M,
                estado = "PE",
                etiqueta1 = null,
                etiqueta2 = null,
                etiqueta3 = null,
                etiqueta4 = null,
                procesarexcel = "si"
            };
            list.Add(itemTest);
            return list;

#endif
            try
            {
                this.connection.Open();
                using (NpgsqlDataReader reader = new NpgsqlCommand("SELECT fldidvalidacionarchivos, fecha, tipo, numeroidentificacion, nombrearchivoarchivo, etiqueta, accion, calidadpdf, estado, etiqueta1, etiqueta2, etiqueta3, etiqueta4, procesarexcel FROM " + tableName + " WHERE estado='PE' ;", this.connection).ExecuteReader())
                {
                    while (reader.Read())
                    {
                        ProcessInfo item = new ProcessInfo
                        {
                            fldidvalidacionarchivos = reader.GetInt32(0),
                            fecha = !reader.IsDBNull(1) ? reader.GetDateTime(1) : DateTime.MinValue,
                            tipo = !reader.IsDBNull(2) ? reader.GetString(2) : null,
                            numeroidentificacion = !reader.IsDBNull(3) ? reader.GetString(3) : null,
                            nombrearchivoarchivo = !reader.IsDBNull(4) ? reader.GetString(4) : null,
                            etiqueta = !reader.IsDBNull(5) ? reader.GetString(5) : null,
                            accion = !reader.IsDBNull(6) ? reader.GetString(6).ToUpper() : null,
                            calidadpdf = !reader.IsDBNull(7) ? reader.GetDecimal(7) : 0M,
                            estado = reader.GetString(8),
                            etiqueta1 = !reader.IsDBNull(9) ? reader.GetString(9) : null,
                            etiqueta2 = !reader.IsDBNull(10) ? reader.GetString(10) : null,
                            etiqueta3 = !reader.IsDBNull(11) ? reader.GetString(11) : null,
                            etiqueta4 = !reader.IsDBNull(12) ? reader.GetString(12) : null,
                            procesarexcel = !reader.IsDBNull(13) ? reader.GetString(13).ToLower() : "si"
                        };
                        list.Add(item);
                    }
                }

                if (list.Count == 0)
                {
                    this.WriteLog("No hay archivos con estado PE");
                }
                else
                {
                    this.WriteLog("Se procesan " + list.Count + " archivos con estado PE");
                }
            }
            catch (Exception exception)
            {
                throw new Exception(exception.Message + "\r\n" + str, exception);
            }
            finally
            {
                if (this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }
            return list;
        }

        public List<string> GetTables()
        {
            List<string> list = new List<string>();
            try
            {
                this.connection.Open();
                using (NpgsqlDataReader reader = new NpgsqlCommand("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' order by table_name", this.connection).ExecuteReader())
                {
                    while (reader.Read())
                    {
                        list.Add(reader.GetString(0));
                    }
                }
            }
            catch (Exception exception1)
            {
                throw exception1;
            }
            finally
            {
                if (this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }
            return list;
        }

        public void Insert(string fileName, byte[] ImgByteA)
        {
            NpgsqlCommand command1 = new NpgsqlCommand();
            command1.CommandText = "insert into tablaarchivo (serial, imgname, img) VALUES ('123', '" + fileName + "', @Image)";
            command1.Connection = this.connection;
            command1.Parameters.Add(new NpgsqlParameter("Image", ImgByteA));
            this.connection.Open();
            command1.ExecuteNonQuery();
            this.connection.Close();
        }

        public void InsertValidacionarchivos(string fileName, byte[] ImgByteA)
        {
            NpgsqlCommand command1 = new NpgsqlCommand();
            command1.CommandText = "insert into Validacionarchivos (fldidvalidacionarchivos, ESTADO, nombrearchivoarchivo, archivo) VALUES (6, 'PE', '" + fileName + "', @Image)";
            command1.Connection =this.connection;
            command1.Parameters.Add(new NpgsqlParameter("Image", ImgByteA));
            this.connection.Open();
            command1.ExecuteNonQuery();
            this.connection.Close();
        }

        public void SetDBManager(string ip, string db, string user, string pwd)
        {
            try
            {
                string[] textArray1 = new string[] { "Server=", ip, ";Port=5432;User ID=", user, ";Password=", pwd, ";Database=", db };
                string str = string.Concat(textArray1);
                this.connection = new NpgsqlConnection(str);
                this.connection.Open();
                this.connection.Close();
            }
            catch (Exception exception1)
            {
                throw exception1;
            }
        }

        public void UpdateValidacionarchivos(ProcessInfo processInfo)
        {
            NpgsqlCommand command = new NpgsqlCommand();
            string[] textArray1 = new string[12];
            textArray1[0] = "update validacionarchivos set estado = '";
            textArray1[1] = processInfo.estado;
            textArray1[2] = "',nombrearchivoarchivoresultante = '";
            textArray1[3] = processInfo.nombrearchivoarchivoresultante;
            textArray1[4] = "',extensionarchivoresultant = '";
            textArray1[5] = processInfo.extensionarchivoresultant;
            textArray1[6] = "',extensionarchivo = '";
            textArray1[7] = processInfo.extensionarchivo;
            textArray1[8] = "',mensajeerror = '";
            textArray1[9] = processInfo.mensajeerror;
            textArray1[10] = "',archivoresultante = @Archivoresultante ";
            textArray1[11] = $"WHERE fldidvalidacionarchivos = '{processInfo.fldidvalidacionarchivos}';";
            command.CommandText = string.Concat(textArray1);
            command.Connection =this.connection;
            processInfo.archivoresultante = new byte[0];
            command.Parameters.Add(new NpgsqlParameter("Archivoresultante", processInfo.archivoresultante));
            this.connection.Open();
            command.ExecuteNonQuery();
            this.connection.Close();
        }
    }

}
