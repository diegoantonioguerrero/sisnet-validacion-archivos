using Npgsql;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.Remoting.Messaging;

namespace SisnetData
{
    public class DBManager
    {
        public static DBManager _self;

        private NpgsqlConnection connection;

        public DBManager()
        {
        }

        internal List<ExportInfo> GetData(string tableName, DataTable dataToExport, string consecutivoField, string arhivoNameField, string archivoField)
        {
            List<ExportInfo> exportInfos = new List<ExportInfo>();
            string empty = string.Empty;
            try
            {
                try
                {
                    List<string> strs = new List<string>();
                    foreach (DataRow row in dataToExport.Rows)
                    {
                        strs.Add(string.Concat("'", row["Consecutivo"].ToString(), "'"));
                    }
                    string str = string.Join(",", strs.ToArray());
                    this.connection.Open();
                    empty = string.Concat(new string[] { "select cast(", consecutivoField, " as text) AS ", consecutivoField, ", ", arhivoNameField, ", cast((length(", archivoField, ") / 1048576.0) as text)|| ' MB' as filesize from ", tableName, " where cast(", consecutivoField, " as text) in(", str, ") ;" });
                    using (NpgsqlDataReader npgsqlDataReader = (new NpgsqlCommand(empty, this.connection)).ExecuteReader())
                    {
                        while (npgsqlDataReader.Read())
                        {
                            exportInfos.Add(new ExportInfo()
                            {
                                Consecutivo = npgsqlDataReader.GetString(0),
                                ArchivoName = npgsqlDataReader.GetString(1),
                                ArchivoLength = npgsqlDataReader.GetString(2)
                            });
                        }
                    }
                }
                catch (Exception exception)
                {
                    throw new Exception(empty, exception);
                }
            }
            finally
            {
                if (this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }
            return exportInfos;
        }

        internal List<ExportInfo> GetDataFile(string tableName, DataTable dataToExport, string consecutivoField, string arhivoNameField, string archivoField, string consecutivos)
        {
            List<ExportInfo> exportInfos = new List<ExportInfo>();
            string empty = string.Empty;
            try
            {
                try
                {
                    this.connection.Open();
                    empty = string.Concat(new string[] { "select cast(", consecutivoField, " as text) AS ", consecutivoField, ", ", arhivoNameField, ", cast((length(", archivoField, ") / 1048576.0) as text)|| ' MB' as filesize,", archivoField, " from ", tableName, " where cast(", consecutivoField, " as text) in(", consecutivos, ") ;" });
                    using (NpgsqlDataReader npgsqlDataReader = (new NpgsqlCommand(empty, this.connection)).ExecuteReader())
                    {
                        while (npgsqlDataReader.Read())
                        {
                            exportInfos.Add(new ExportInfo()
                            {
                                Consecutivo = npgsqlDataReader.GetString(0),
                                ArchivoName = npgsqlDataReader.GetString(1),
                                ArchivoLength = npgsqlDataReader.GetString(2),
                                ArchivoData = (byte[])npgsqlDataReader[3]
                            });
                        }
                    }
                }
                catch (Exception exception)
                {
                    throw new Exception(empty, exception);
                }
            }
            finally
            {
                if (this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }
            return exportInfos;
        }

        public List<ProcessInfo> GetDataFileToConvert(string tableName, string fldidvalidacionarchivos)
        {
            byte[] item;
            List<ProcessInfo> processInfos = new List<ProcessInfo>();
#if DEBUG
            ProcessInfo itemTest = new ProcessInfo();
            var path = "E:\\32-27-813.zip";

            byte[]  byt = System.IO.File.ReadAllBytes(path);

            itemTest.fldidvalidacionarchivos = 1;
            itemTest.ArchivoData = byt;
            processInfos.Add(itemTest);
            return processInfos;
#endif
            string empty = string.Empty;
            try
            {
                try
                {
                    this.connection.Open();
                    empty = string.Concat(new string[] { "select fldidvalidacionarchivos, archivo from ", tableName, " where  cast(fldidvalidacionarchivos as text) in(", fldidvalidacionarchivos, ") ;" });
                    using (NpgsqlDataReader npgsqlDataReader = (new NpgsqlCommand(empty, this.connection)).ExecuteReader())
                    {
                        while (npgsqlDataReader.Read())
                        {
                            ProcessInfo processInfo = new ProcessInfo()
                            {
                                fldidvalidacionarchivos = npgsqlDataReader.GetInt32(0)
                            };
                            if (npgsqlDataReader.IsDBNull(1))
                            {
                                item = null;
                            }
                            else
                            {
                                item = (byte[])npgsqlDataReader[1];
                            }
                            processInfo.ArchivoData = item;
                            processInfos.Add(processInfo);
                        }
                    }
                }
                catch (Exception exception)
                {
                    throw new Exception(empty, exception);
                }
            }
            finally
            {
                if (this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }
            return processInfos;
        }

        public static DBManager GetDBManager()
        {
            if (DBManager._self == null)
            {
                DBManager._self = new DBManager();
            }
            return DBManager._self;
        }

        public Dictionary<string, string> GetFields(string tableName)
        {
            Dictionary<string, string> strs = new Dictionary<string, string>();
            try
            {
                try
                {
                    this.connection.Open();
                    using (NpgsqlDataReader npgsqlDataReader = (new NpgsqlCommand(string.Concat("select column_name, data_type from information_schema.columns where table_name = '", tableName, "' order by column_name;"), this.connection)).ExecuteReader())
                    {
                        while (npgsqlDataReader.Read())
                        {
                            strs.Add(npgsqlDataReader.GetString(0), npgsqlDataReader.GetString(1));
                        }
                    }
                }
                catch (Exception exception)
                {
                    throw exception;
                }
            }
            finally
            {
                if (this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }
            return strs;
        }

        public List<ProcessInfo> GetPendingFiles(string tableName)
        {
            string str;
            string str1;
            string str2;
            string str3;
            string upper;
            string str4;
            string str5;
            string str6;
            string str7;
            List<ProcessInfo> processInfos = new List<ProcessInfo>();
            string empty = string.Empty;
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
            processInfos.Add(itemTest);
            return processInfos;

#endif
            try
            {
                try
                {
                    this.connection.Open();
                    empty = string.Concat("SELECT fldidvalidacionarchivos, fecha, tipo, numeroidentificacion, nombrearchivoarchivo, etiqueta, accion, calidadpdf, estado, etiqueta1, etiqueta2, etiqueta3, etiqueta4, procesarexcel FROM ", tableName, " WHERE estado='PE' ;");
                    using (NpgsqlDataReader npgsqlDataReader = (new NpgsqlCommand(empty, this.connection)).ExecuteReader())
                    {
                        while (npgsqlDataReader.Read())
                        {
                            ProcessInfo processInfo = new ProcessInfo()
                            {
                                fldidvalidacionarchivos = npgsqlDataReader.GetInt32(0),
                                fecha = (!npgsqlDataReader.IsDBNull(1) ? npgsqlDataReader.GetDateTime(1) : DateTime.MinValue),
                                tipo = !npgsqlDataReader.IsDBNull(2) ? npgsqlDataReader.GetString(2) : null,
                                numeroidentificacion = !npgsqlDataReader.IsDBNull(3) ? npgsqlDataReader.GetString(3) : null,
                                nombrearchivoarchivo = !npgsqlDataReader.IsDBNull(4) ? npgsqlDataReader.GetString(4) : "",
                                etiqueta = !npgsqlDataReader.IsDBNull(5) ? npgsqlDataReader.GetString(5) : null,
                                accion = !npgsqlDataReader.IsDBNull(6) ? npgsqlDataReader.GetString(6).ToUpper() : null,
                                calidadpdf = !npgsqlDataReader.IsDBNull(7) ? npgsqlDataReader.GetDecimal(7) : 0M,
                                estado = npgsqlDataReader.GetString(8),
                                etiqueta1 = !npgsqlDataReader.IsDBNull(9) ? npgsqlDataReader.GetString(9) : null,
                                etiqueta2 = !npgsqlDataReader.IsDBNull(10) ? npgsqlDataReader.GetString(10) : null,
                                etiqueta3 = !npgsqlDataReader.IsDBNull(11) ? npgsqlDataReader.GetString(11) : null,
                                etiqueta4 = !npgsqlDataReader.IsDBNull(12) ? npgsqlDataReader.GetString(12) : null,
                                procesarexcel = !npgsqlDataReader.IsDBNull(13) ? npgsqlDataReader.GetString(13).ToLower() : "si"
                            };
                           
                            processInfos.Add(processInfo);
                        }
                    }
                }
                catch (Exception exception1)
                {
                    Exception exception = exception1;
                    throw new Exception(string.Concat(exception.Message, "\r\n", empty), exception);
                }
            }
            finally
            {
                if (this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }
            return processInfos;
        }

        public List<string> GetTables()
        {
            List<string> strs = new List<string>();
            try
            {
                try
                {
                    this.connection.Open();
                    using (NpgsqlDataReader npgsqlDataReader = (new NpgsqlCommand("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' order by table_name", this.connection)).ExecuteReader())
                    {
                        while (npgsqlDataReader.Read())
                        {
                            strs.Add(npgsqlDataReader.GetString(0));
                        }
                    }
                }
                catch (Exception exception)
                {
                    throw exception;
                }
            }
            finally
            {
                if (this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }
            return strs;
        }

        public void Insert(string fileName, byte[] ImgByteA)
        {
            NpgsqlCommand npgsqlCommand = new NpgsqlCommand()
            {
                CommandText = string.Concat("insert into tablaarchivo (serial, imgname, img) VALUES ('123', '", fileName, "', @Image)"),
                Connection = this.connection
            };
            npgsqlCommand.Parameters.Add(new NpgsqlParameter("Image", ImgByteA));
            this.connection.Open();
            npgsqlCommand.ExecuteNonQuery();
            this.connection.Close();
        }

        public void InsertValidacionarchivos(string fileName, byte[] ImgByteA)
        {
            NpgsqlCommand npgsqlCommand = new NpgsqlCommand()
            {
                CommandText = string.Concat("insert into Validacionarchivos (fldidvalidacionarchivos, ESTADO, nombrearchivoarchivo, archivo) VALUES (6, 'PE', '", fileName, "', @Image)"),
                Connection = this.connection
            };
            npgsqlCommand.Parameters.Add(new NpgsqlParameter("Image", ImgByteA));
            this.connection.Open();
            npgsqlCommand.ExecuteNonQuery();
            this.connection.Close();
        }

        public void SetDBManager(string ip, string db, string user, string pwd)
        {
            try
            {
                this.connection = new NpgsqlConnection(string.Concat(new string[] { "Server=", ip, ";Port=5432;User ID=", user, ";Password=", pwd, ";Database=", db }));
                this.connection.Open();
                this.connection.Close();
            }
            catch (Exception exception)
            {
                throw exception;
            }
        }

        public void UpdateValidacionarchivos(ProcessInfo processInfo)
        {
            NpgsqlCommand npgsqlCommand = new NpgsqlCommand()
            {
                CommandText = string.Concat(new string[] { "update validacionarchivos set estado = '", processInfo.estado, "',nombrearchivoarchivoresultante = '", processInfo.nombrearchivoarchivoresultante, "',extensionarchivoresultant = '", processInfo.extensionarchivoresultant, "',extensionarchivo = '", processInfo.extensionarchivo, "',mensajeerror = '", processInfo.mensajeerror, "',archivoresultante = @Archivoresultante ", string.Format("WHERE fldidvalidacionarchivos = '{0}';", processInfo.fldidvalidacionarchivos) }),
                Connection = this.connection
            };
            if (processInfo.archivoresultante == null)
            {
                processInfo.archivoresultante = new byte[0];
            }
            npgsqlCommand.Parameters.Add(new NpgsqlParameter("Archivoresultante", processInfo.archivoresultante));
            this.connection.Open();
            npgsqlCommand.ExecuteNonQuery();
            this.connection.Close();
        }
    }
}