using Npgsql;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using System.ComponentModel;
using System.Threading;
//using Mono.Security.Cryptography;
using System.Data.Odbc;
using System.Text.RegularExpressions;
using System.Diagnostics;
using System.Timers;
using System.Globalization;

namespace SisnetData
{
    public class DBManager
    {
        public static DBManager _self;

        private NpgsqlConnection connection;

        public float ServerVersion { get; private set; }

        List<NpgsqlConnection> connectionPoolLocal;
        List<NpgsqlTransaction> transactionPoolLocal;
        private static readonly int TWENTY_MINS;

        public DBManager()
        {
        }

        public List<ExportInfo> GetData(string tableName, DataTable dataToExport, string consecutivoField, string arhivoNameField, string archivoField, int bufferItemsArchivo, int reintentosConexion)
        {
            List<ExportInfo> exportInfos = new List<ExportInfo>();
            string empty = string.Empty;
            try
            {
                try
                {

                    List<ExportInfo> data = new List<ExportInfo>();

                    int tamañoDeLote = bufferItemsArchivo;

                    for (int i = 0; i < dataToExport.Rows.Count; i += tamañoDeLote)
                    {
                        List<string> strs = new List<string>();
                        // Utiliza LINQ para tomar el siguiente lote de registros.
                        var loteDeRegistros = dataToExport.AsEnumerable().Skip(i).Take(tamañoDeLote);
                        strs.AddRange(from row in loteDeRegistros
                                      select string.Concat("'", row["Consecutivo"].ToString(), "'"));

                        string str = string.Join(",", strs.ToArray());
                        if (this.connection.State != ConnectionState.Open)
                        {
                            this.connection.Open();
                        }

                        empty = string.Concat(new string[] { "select cast(", consecutivoField, " as text) AS ", consecutivoField, ", ", arhivoNameField, ", cast((length(", archivoField, ") / 1048576.0) as text)|| ' MB' as filesize from ", tableName, " where cast(", consecutivoField, " as text) in(", str, ") ;" });
                        NpgsqlCommand command = new NpgsqlCommand(empty, this.connection);
                        command.CommandTimeout = 60;
                        using (NpgsqlDataReader npgsqlDataReader = (command).ExecuteReader())
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
                        command.Dispose();
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

        public List<ExportInfo> GetDataFile(string tableName, DataTable dataToExport, string consecutivoField, string arhivoNameField, string archivoField, string consecutivos, int reintentosConexion)
        {
            List<ExportInfo> exportInfos = new List<ExportInfo>();
            string empty = string.Empty;
            try
            {

                try
                {
                    this.connection.Open();
                    empty = string.Concat(new string[] { "select cast(", consecutivoField, " as text) AS ", consecutivoField, ", ", arhivoNameField, ", cast((length(", archivoField, ") / 1048576.0) as text)|| ' MB' as filesize,", archivoField, " from ", tableName, " where cast(", consecutivoField, " as text) in(", consecutivos, ") ;" });
                    NpgsqlCommand command = new NpgsqlCommand(empty, this.connection);
                    command.CommandTimeout = 60;

                    int intento = reintentosConexion;
                    while (intento > 0)
                    {
                        try
                        {
                            using (NpgsqlDataReader npgsqlDataReader = command.ExecuteReader())
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
                            command.Dispose();
                            intento = -1;

                        }
                        catch (Exception ex)
                        {
                            intento--;
                            if (intento == 0)
                            {
                                throw ex;
                            }
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
            List<ProcessInfo> processInfos = new List<ProcessInfo>();
#if DEBUG
            ProcessInfo itemTest = new ProcessInfo();
            var path = "E:\\32-27-813.zip";

            byte[] byt = System.IO.File.ReadAllBytes(path);

            itemTest.fldidvalidacionarchivos = 1;
            itemTest.ArchivoData = byt;
            processInfos.Add(itemTest);
            return processInfos;
#else
            byte[] item;

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
#endif
        }

        public static DBManager GetDBManager()
        {
            if (DBManager._self == null)
            {
                DBManager._self = new DBManager();
            }
            return DBManager._self;
        }

        public static DBManager GetInstance()
        {
            DBManager _instance = new DBManager();
            return _instance;
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
#else

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
#endif
        }

        public List<TableInfo> GetTables()
        {
            List<TableInfo> tablesDetected = new List<TableInfo>();
            try
            {
                try
                {
                    this.OpenConnection(this.connection);
                    string sqlTables = @"SELECT table_name, pg_size_pretty(pg_total_relation_size('""' || table_name || '""')) AS size_text, 
                            pg_total_relation_size('""' || table_name || '""') AS size
                        FROM information_schema.tables 
                        WHERE table_schema = 'public' 
-- and table_name not in ('correspondencia')
                                -- and table_name in ('abarchivoss', 'carpetas')
--and table_name in ('revisionarchivo','zonadeldespachojudicial', 'aacoco')
--and table_name in ('aplicativoconteo','zonadetrabajofuncionario')
                        order by table_name
 --limit 70
;";

                    using (NpgsqlDataReader npgsqlDataReader = (new NpgsqlCommand(sqlTables, this.connection)).ExecuteReader())
                    {
                        while (npgsqlDataReader.Read())
                        {
                            tablesDetected.Add(new TableInfo()
                            {
                                Name = npgsqlDataReader.GetString(0),
                                SizeInfo = npgsqlDataReader.GetString(1),
                                Size = npgsqlDataReader.GetInt64(2)
                            });
                        }
                    }

                    if (!tablesDetected.Any())
                    {
                        return tablesDetected;
                    }

                    string sql = @"SELECT
                        kc.table_name AS table_name,
                        kc.column_name AS column_name,
                        pc.conname
                    FROM
                        information_schema.key_column_usage kc
                    JOIN
                        pg_constraint pc ON
                        kc.constraint_name = pc.conname
                    WHERE
                        pc.contype = 'p' AND
                        kc.table_name in (";
                    sql = sql + string.Join(", ", tablesDetected.Select(tableInfo => "'" + tableInfo.Name + "'").ToArray()) + ");";

                    using (NpgsqlCommand command = new NpgsqlCommand(sql, this.connection))
                    using (NpgsqlDataReader npgsqlDataReader = command.ExecuteReader())
                    {
                        while (npgsqlDataReader.Read())
                        {
                            TableInfo tableInfoKey = tablesDetected.Where(tableInfo => tableInfo.Name == npgsqlDataReader.GetString(0)).Single();
                            tableInfoKey.Keys.Add(npgsqlDataReader.GetString(1));
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
            return tablesDetected;
        }

        public void GetRecordCount(List<TableInfo> tableNames)
        {
            try
            {

                string sql =
                    string.Join($"{Environment.NewLine}UNION ", tableNames.Select(tableInfo =>
                    $"SELECT '{tableInfo.Name}' AS TableName, COUNT(*) FROM {tableInfo.Name}"
                    ).ToArray()) + ";";


                this.connection.Open();
                using (NpgsqlCommand command = new NpgsqlCommand(sql, this.connection))
                using (NpgsqlDataReader npgsqlDataReader = command.ExecuteReader())
                {
                    while (npgsqlDataReader.Read())
                    {
                        TableInfo tableInfoKey = tableNames.Where(tableInfo => tableInfo.Name == npgsqlDataReader.GetString(0)).Single();
                        tableInfoKey.Count = npgsqlDataReader.GetInt64(1);
                    }
                }

            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }
        }

        public float GetServerVersion()
        {
            try
            {
                this.connection.Open();

                using (NpgsqlCommand cmd = new NpgsqlCommand("SELECT version();", this.connection))
                {
                    object result = cmd.ExecuteScalar();

                    if (result != null)
                    {
                        Console.WriteLine("Versión de PostgreSQL: " + result.ToString());
                        // Patrón de expresión regular
                        string pattern = @"PostgreSQL (\d+(\.\d+)?)";

                        // Coincidencias
                        Match match = Regex.Match(result.ToString(), pattern);

                        if (match.Success)
                        {
                            // El número después de "PostgreSQL"
                            string versionNumber = match.Groups[1].Value;
                            Console.WriteLine("Versión de PostgreSQL: " + versionNumber);
                            CultureInfo usCulture = new CultureInfo("en-US");
                            NumberFormatInfo dbNumberFormat = usCulture.NumberFormat;
                            dbNumberFormat.NumberDecimalSeparator = ".";
                            return float.Parse(versionNumber.Replace(",", "."), dbNumberFormat);

                        }
                        else
                        {
                            throw new ApplicationException("No se encontró una versión de PostgreSQL en la cadena.");
                        }
                    }

                    string error = "No se pudo obtener la versión de PostgreSQL.";
                    Console.WriteLine(error);
                    throw new ApplicationException(error);

                }

            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }
        }


        public void Insert(string fileName, byte[] ImgByteA)
        {
            NpgsqlCommand npgsqlCommand = new NpgsqlCommand()
            {
                CommandText = string.Concat("insert into tablaarchivo (serial, imgname, img) VALUES ('128', '", fileName, "', @Image)"),
                Connection = this.connection
            };
            npgsqlCommand.Parameters.Add(new NpgsqlParameter("Image", ImgByteA));
            this.connection.Open();
            npgsqlCommand.ExecuteNonQuery();
            this.connection.Close();
        }

        public void InsertArchivo(string id, byte[] ImgByteA)
        {

            NpgsqlCommand npgsqlCommand = new NpgsqlCommand()
            {
                CommandText = string.Concat("insert into abarchivos (idarchivo, archivo) VALUES ('" + id + "', @Image)"),
                Connection = this.connection
            };
            NpgsqlParameter param = new NpgsqlParameter("Image", ImgByteA);
            param.DbType = DbType.Binary;
            npgsqlCommand.Parameters.Add(param);
            this.connection.Open();
            npgsqlCommand.ExecuteNonQuery();
            this.connection.Close();


        }

        public void InsertArchivoOdbc(string id, byte[] ImgByteA)
        {
            // Setup a connection string
            string connectionString = "DSN=pgAnapo;" +
                               "UID=postgres;" +
                               "PWD=postgres";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    // Consulta SQL para la inserción
                    string sqlQuery = "INSERT INTO abarchivos (idarchivo, archivo) VALUES (?, ?)";

                    using (OdbcCommand command = new OdbcCommand(sqlQuery, connection))
                    {
                        // Parámetros para la consulta
                        command.Parameters.Add("id", OdbcType.Int).Value = id;
                        command.Parameters.Add("img", OdbcType.Binary).Value = ImgByteA;

                        // Ejecutar la consulta de inserción
                        int rowsAffected = command.ExecuteNonQuery();

                        Console.WriteLine(rowsAffected + " fila(s) insertada(s) correctamente.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error al insertar datos: " + ex.Message);
                }
            }


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



        public void SetDBManager(string ip, string port, string db, string user, string pwd, bool testConnection = true)
        {
            try
            {
                this.connection = new NpgsqlConnection(string.Concat(new string[] { "Server=", ip, ";Port=" + port + ";User ID=", user, ";Password=", pwd, ";Database=", db, ";ApplicationName=SisnetComparer;" }));
                if (!testConnection)
                {
                    return;
                }
                this.OpenConnection(this.connection);
                this.connection.Close();
                this.ServerVersion = GetServerVersion();
            }
            catch (Exception exception)
            {
                throw exception;
            }
        }

        public void UpdateValidacionarchivos(ProcessInfo processInfo)
        {
            string extensionarchivoresultant = string.IsNullOrEmpty(processInfo.extensionarchivoresultant)
                ? "NULL" : "'" + processInfo.extensionarchivoresultant + "'";

            string extensionarchivo = string.IsNullOrEmpty(processInfo.extensionarchivo)
                ? "NULL": "'" + processInfo.extensionarchivo + "'";

            string mensajeerror = string.IsNullOrEmpty(processInfo.mensajeerror)
                ? "NULL" : "'" + processInfo.mensajeerror + "'";

            NpgsqlCommand npgsqlCommand = new NpgsqlCommand()
            {
                CommandText = string.Concat(new string[] { "update validacionarchivos set estado = '", processInfo.estado, "',nombrearchivoarchivoresultante = '", processInfo.nombrearchivoarchivoresultante, "',extensionarchivoresultant = " , extensionarchivoresultant, ",extensionarchivo = ", extensionarchivo, ",mensajeerror = ", mensajeerror, ",archivoresultante = @Archivoresultante ", string.Format("WHERE fldidvalidacionarchivos = '{0}';", processInfo.fldidvalidacionarchivos) }),
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

        public void RecreateTable(string table)
        {
            List<string> indices = GetIndices(table);
            List<string> restr = GetRestricciones(table);

            String command = @"-- Paso 1: Copiar la tabla junto con índices y restricciones a una nueva tabla
CREATE TABLE {0}_nueva AS
  SELECT * FROM {0};";

            command = String.Format(command, table);
            NpgsqlCommand npgsqlCommand = new NpgsqlCommand()
            {
                CommandText = command,
                Connection = this.connection
            };


            //npgsqlCommand.Parameters.Add(new NpgsqlParameter("Archivoresultante", processInfo.archivoresultante));
            this.connection.Open();
            npgsqlCommand.ExecuteNonQuery();
            this.connection.Close();

            this.CrearIndices(table, indices);



            command = @"

-- Paso 2: Eliminar la tabla original
DROP TABLE {0};

-- Paso 3: Renombrar la nueva tabla con el nombre de la original
ALTER TABLE {0}_nueva RENAME TO {0};";

            npgsqlCommand = new NpgsqlCommand()
            {
                CommandText = command,
                Connection = this.connection
            };

            //npgsqlCommand.Parameters.Add(new NpgsqlParameter("Archivoresultante", processInfo.archivoresultante));
            //this.connection.Open();
            //npgsqlCommand.ExecuteNonQuery();
            //this.connection.Close();


        }

        private void CrearIndices(string table, List<string> indices)
        {

            foreach (String indice in indices)
            {
                String command = String.Format(@"EXECUTE 'CREATE INDEX {1} ' ON {0}_nueva USING btree (' || (SELECT string_agg(column_name, ', ') FROM information_schema.index_columns WHERE index_name = {1}) || ');';", table, indice);
                NpgsqlCommand npgsqlCommand = new NpgsqlCommand()
                {
                    CommandText = command,
                    Connection = this.connection
                };


                //npgsqlCommand.Parameters.Add(new NpgsqlParameter("Archivoresultante", processInfo.archivoresultante));
                this.connection.Open();
                npgsqlCommand.ExecuteNonQuery();
                this.connection.Close();
            }

        }

        public List<string> GetIndices(String table)
        {
            List<string> strs = new List<string>();
            try
            {
                try
                {
                    this.connection.Open();
                    using (NpgsqlDataReader npgsqlDataReader = (new NpgsqlCommand(
                        String.Format("SELECT indexname FROM pg_indexes WHERE tablename = '{0}'", table), this.connection)).ExecuteReader())
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

        public List<string> GetRestricciones(String table)
        {
            List<string> strs = new List<string>();
            try
            {
                try
                {
                    this.connection.Open();
                    using (NpgsqlDataReader npgsqlDataReader = (new NpgsqlCommand(
                        String.Format("SELECT conname FROM pg_constraint WHERE confrelid = '{0}'::regclass", table), this.connection)).ExecuteReader())
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

        public DataTable GetBds()
        {
            DataTable table = null;
            string sql = string.Empty;
            try
            {
                int reintentos = 2;
                while (reintentos >= 0)
                {
                    try
                    {
                        table = new DataTable();
                        if (this.connection.State != ConnectionState.Open)
                        {
                            this.OpenConnection(this.connection);
                        }

                        sql = @"SELECT datname, '0 mb' AS size FROM pg_database WHERE datname NOT IN ('postgres', 'template0', 'template1') ORDER BY datname;";

                        using (NpgsqlCommand command = new NpgsqlCommand(sql, this.connection))
                        {
                            // Ajusta el tiempo de espera aquí (en segundos)
                            // 5 minutos
                            command.CommandTimeout = 60 * 5;
                            using (NpgsqlDataReader npgsqlDataReader = command.ExecuteReader())
                            {
                                DataTable schema = npgsqlDataReader.GetSchemaTable();
                                foreach (DataRow row in schema.Rows)
                                {
                                    table.Columns.Add(row["ColumnName"].ToString(), (Type)row["DataType"]);
                                }

                                table.Load(npgsqlDataReader);
                            }
                        }

                        sql = @"SELECT pg_size_pretty (cast(sum(pg_total_relation_size('""' || table_schema || '"".""' || table_name || '""')) as bigint)) AS total_size
                                FROM
                                    information_schema.tables
                                WHERE
                                    table_schema NOT IN ('pg_catalog', 'information_schema')";

                        foreach (DataRow dbRow in table.Rows)
                        {
                            this.connection.ChangeDatabase(dbRow["datname"].ToString());
                            using (NpgsqlCommand command = new NpgsqlCommand(sql, this.connection))
                            {
                                // Ajusta el tiempo de espera aquí (en segundos)
                                // 5 minutos
                                command.CommandTimeout = 60 * 5;
                                dbRow["size"] = command.ExecuteScalar();

                            }
                        }
                        reintentos = -1;

                    }
                    catch (Exception ex)
                    {
                        if (reintentos == 0)
                        {
                            throw ex;
                        }

                    }
                    finally
                    {
                        reintentos--;
                    }
                }

            }
            catch (Exception ex)
            {
                if (ex is NpgsqlException)
                {
                    NpgsqlException e =
                        (NpgsqlException)ex;
                    Console.WriteLine("Error GetBds -> " + e.Message);
                    Console.WriteLine("Error GetBds -> " + e.Code);
                    Console.WriteLine("Error GetBds -> " + (e.InnerException != null ? e.InnerException.Message : "N/A"));
                }
                Console.WriteLine("Error GetBds -> " + ex.Message);
                Console.WriteLine("Error GetBds -> " + sql);
                Console.WriteLine("Error GetBds -> " + ex.StackTrace);

                throw new ApplicationException("Error GetBds -> " + ex.Message + "\r\n", ex);
            }
            finally
            {
                if (this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }

            return table;
        }

        public void OpenConnection(NpgsqlConnection connectionToOpen)
        {
            int reintentos = 4;
            while (reintentos >= 0)
            {
                try
                {
                    if (connectionToOpen != null && connectionToOpen.State != ConnectionState.Open)
                    {
                        connectionToOpen.Open();
                        reintentos = -1;
                    }
                }
                catch (Exception ex)
                {

                    Console.WriteLine("Error OpenConnection");
                    if (reintentos == 0)
                    {
                        throw ex;
                    }
                    // duerme 1 segundo para reintentar conexion
                    Thread.Sleep(1000);

                }
                finally
                {
                    reintentos--;
                }
            }
        }

        public void OpenConnectionODBC(OdbcConnection connection)
        {
            int reintentos = 4;
            while (reintentos >= 0)
            {
                try
                {
                    if (connection != null && connection.State != ConnectionState.Open)
                    {
                        connection.Open();
                        reintentos = -1;
                    }
                }
                catch (Exception ex)
                {

                    Console.WriteLine("Error OpenConnection");
                    if (reintentos == 0)
                    {
                        throw ex;
                    }
                    // duerme 1 segundo para reintentar conexion
                    Thread.Sleep(1000);

                }
                finally
                {
                    reintentos--;
                }
            }
        }

        public void CloseConnection()
        {
            this.CloseConnection(this.connection);
        }

        public void CloseConnection(NpgsqlConnection connection)
        {
            if (connection != null && connection.State == ConnectionState.Open)
            {
                connection.Close();
            }
        }
        public DataTable GetTableData(string tableName, List<DataColumn> columnsKeys = null, object[][] filterData = null, bool onlySchema = false)
        {
            DataTable table = new DataTable();
            bool openLocalConexion = false;
            try
            {
                if (this.connection.State != ConnectionState.Open)
                {
                    this.OpenConnection(this.connection);
                    openLocalConexion = true;
                }


                int reintentos = 2;
                while (reintentos >= 0)
                {
                    string[] fieldsToSelect = null;
                    try
                    {

                        string fields = "*";
                        if (columnsKeys != null && columnsKeys.Any())
                        {
                            fieldsToSelect = (from DataColumn column in columnsKeys
                                              select column.ColumnName).ToArray();
                        }

                        if (columnsKeys != null && columnsKeys.Any() && filterData == null)
                        {
                            fields = string.Join(",", fieldsToSelect);
                        }


                        string sql = $"SELECT {fields} FROM {tableName}";

                        if (filterData != null)
                        {
                            if (columnsKeys != null && columnsKeys.Count == 1)
                            {
                                List<string> filter = filterData
                                    .OrderBy(prymKeyValue => long.Parse(prymKeyValue[0].ToString()))
                                    .Select(prymKeyValues => prymKeyValues[0].ToString()).ToList();

                                sql += " WHERE " + columnsKeys[0] + " BETWEEN " + filter.First() + " AND " + filter.Last();
                            }
                            else
                            {
                                string[] dataToSelect = (from object[] data in filterData
                                                         select GenerateFilter(columnsKeys, data)).ToArray();
                                sql += " WHERE " + string.Join(" OR ", dataToSelect);
                            }

                        }

                        if (onlySchema)
                        {
                            sql += " WHERE 1=2 ";
                        }
                        /*if (!sql.Contains("WHERE"))
                        {
                            sql += " where fldidimagenesanexas between 37055 and 38060 ";
                        }*/
                        sql += " ORDER BY 1;";
                        using (NpgsqlCommand command = new NpgsqlCommand(sql, this.connection))
                        {
                            // Ajusta el tiempo de espera aquí (en segundos)
                            // 20 minutos
                            command.CommandTimeout = TWENTY_MINS;
                            using (NpgsqlDataReader npgsqlDataReader = command.ExecuteReader())
                            {
                                DataTable schema = npgsqlDataReader.GetSchemaTable();
                                foreach (DataRow row in schema.Rows)
                                {
                                    DataColumn columnAdded = table.Columns.Add(row["ColumnName"].ToString(), (Type)row["DataType"]);
                                    if (row["ProviderType"].ToString() == "time")
                                    {
                                        columnAdded.ExtendedProperties.Add("ProviderType", "time");
                                    }
                                    else if (row["ProviderType"].ToString() == "date")
                                    {
                                        columnAdded.ExtendedProperties.Add("ProviderType", "date");
                                    }

                                    if (columnAdded.DataType == typeof(DateTime) && columnAdded.ExtendedProperties.Count == 0)
                                    {
                                        throw new ApplicationException("Unsuported time");
                                    }

                                }
                                table.Load(npgsqlDataReader);
                            }
                        }
                        reintentos = -1;

                    }
                    catch (Exception ex)
                    {
                        if (ex is NpgsqlException)
                        {
                            NpgsqlException npgsqlException = (NpgsqlException)ex;
                            if (npgsqlException.Code == "53200" || npgsqlException.Code == "XX000")
                            {
                                return this.GetTableDataODBC(tableName, columnsKeys, filterData);
                            }
                        }
                        Console.WriteLine("Error GetTableData -> " + ex.Message);
                        if (reintentos == 0)
                        {
                            throw ex;
                        }

                    }
                    finally
                    {
                        reintentos--;
                    }
                }

            }
            catch (Exception ex)
            {
                string context = "Error GetTableData " + tableName + " -> " + ex.Message;
                //+ "\r\n\r\n" + ctx;
                Console.WriteLine(context);
                throw new ApplicationException(context, ex);
            }
            finally
            {
                if (openLocalConexion && this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }

            if (filterData != null && table.Rows.Count != filterData.Length)
            {
                throw new ApplicationException("Error obteniendo datos " + tableName + " conteo de registros diferente");
            }
            return table;

        }

        public DataTable GetTableDataODBC(string tableName, List<DataColumn> columnsKeys, object[][] filterData = null)
        {
            Console.WriteLine("GetTableDataODBC -> " + tableName);
            DataTable table = new DataTable();
            bool openLocalConexion = false;
            OdbcConnection connection = GetODBCConnectionOrigen();

            try
            {
                if (connection.State != ConnectionState.Open)
                {
                    OpenConnectionODBC(connection);
                    openLocalConexion = true;
                }


                int reintentos = 2;
                while (reintentos >= 0)
                {
                    try
                    {
                        //string[] fieldsToSelect = null;
                        string fields = "*";
                        /*if (columnsKeys != null)
                        {
                            fieldsToSelect = (from DataColumn column in columnsKeys
                                              select column.ColumnName).ToArray();
                        }
                        
                        if (columnsKeys != null && filterData == null)
                        {
                            fields = string.Join(",", fieldsToSelect);
                        }
                        */

                        string sql = $"SELECT {fields} FROM {tableName}";

                        if (filterData != null)
                        {
                            string[] dataToSelect = (from object[] data in filterData
                                                     select GenerateFilter(columnsKeys, data)).ToArray();
                            sql += " WHERE " + string.Join(" OR ", dataToSelect);
                        }
                        /*
                        if (!sql.Contains("WHERE"))
                        {
                            sql += " where fldidimagenesanexas between 37055 and 38060 ";
                        }
                        */
                        sql += " ORDER BY 1;";
                        using (OdbcCommand command = new OdbcCommand(sql, connection))
                        {
                            command.CommandTimeout = 60 * 20;
                            using (OdbcDataReader npgsqlDataReader = command.ExecuteReader())
                            {
                                DataTable schema = npgsqlDataReader.GetSchemaTable();
                                foreach (DataRow row in schema.Rows)
                                {
                                    DataColumn columnAdded = table.Columns.Add(row["ColumnName"].ToString(), (Type)row["DataType"]);
                                    if (row["ProviderType"].ToString() == "time")
                                    {
                                        columnAdded.ExtendedProperties.Add("ProviderType", "time");
                                    }
                                    else if (row["ProviderType"].ToString() == "date")
                                    {
                                        columnAdded.ExtendedProperties.Add("ProviderType", "date");
                                    }
                                }

                                table.Load(npgsqlDataReader);
                            }
                        }
                        reintentos = -1;

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Error GetTableData ODBC -> " + ex.Message);
                        if (reintentos == 0)
                        {
                            throw ex;
                        }

                    }
                    finally
                    {
                        reintentos--;
                    }
                }

            }
            catch (Exception ex)
            {
                string context = "Error GetTableData ODBC " + tableName + " -> " + ex.Message;
                Console.WriteLine(context);
                throw new ApplicationException(context, ex);
            }
            finally
            {
                if (openLocalConexion && connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
            }

            return table;

        }
        public static object ExtractTime(DataColumn column, object theTime)
        {
            try
            {
                if (theTime == DBNull.Value)
                    return theTime;

                DateTime time = (DateTime)theTime;
                if (column.ExtendedProperties["ProviderType"].ToString() == "time")
                {
                    TimeSpan onlyHour = time.TimeOfDay;
                    return onlyHour;
                }
                else if (column.ExtendedProperties["ProviderType"].ToString() == "date")
                {
                    string onlyDate = time.Date.ToString("yyyy-MM-dd");
                    return onlyDate;
                }

                throw new ApplicationException("DateNot supported");

            }
            catch (Exception)
            {
                Console.WriteLine("Error en registro ");
                throw;
            }

        }

        private string GenerateFilter(List<DataColumn> fieldsToSelect, object[] data)
        {
            if (fieldsToSelect == null || !fieldsToSelect.Any())
                return string.Empty;

            string[] filterKeys =
                fieldsToSelect.Select((column, indice) =>
                                column.ExtendedProperties.Count == 0 ?
                                ExtractValue(column, data[indice])
                                :
                                $"({column.ColumnName} = '" + ExtractTime(column, data[indice]) + "')"
                                ).ToArray();

            return "( " + string.Join(" AND ", filterKeys) + " )";
        }

        private string ExtractValue(DataColumn column, object data)
        {
            if (data == DBNull.Value)
            {
                return $"({column.ColumnName} IS NULL)";
            }
            return $"({column.ColumnName} = '" + data + "')";

        }

        public string GetCreateSchemaTable(string tableName, DataRow fieldReplace = null)
        {
            DataTable dataTableFields = this.GetFieldsAdvance(tableName);
            string[] columns = (from DataRow row in dataTableFields.Rows
                                select BuildFieldDefinition(row, fieldReplace)
                                     ).ToArray();

            DataTable dataTableFieldsPK = this.GetPrimaryFields(tableName);

            string constraint_name =
                (from DataRow row in dataTableFieldsPK.Rows
                 select row["constraint_name"].ToString()).FirstOrDefault();

            string[] columnsPrimaryKey = (from DataRow row in dataTableFieldsPK.Rows
                                          select row["column_name"].ToString()
                                     ).ToArray();

            constraint_name = string.IsNullOrEmpty(constraint_name) ? string.Empty :
                 $"CONSTRAINT {constraint_name} PRIMARY KEY ( " + string.Join(",", columnsPrimaryKey) + ")";

            string createSentence = "CREATE TABLE " + tableName + " (" + string.Join(",", columns) + $" {Environment.NewLine}";
            //si tiene llave primaria se suma 
            createSentence += (string.IsNullOrEmpty(constraint_name) ? string.Empty : $",{constraint_name}{Environment.NewLine}");
            createSentence += "); ";


            DataTable dataTableAdditionaIndices = this.GetAdditionlIndices(tableName);
            string[] indexdefs = (from DataRow row in dataTableAdditionaIndices.Rows
                                  select row["indexdef"].ToString()
                                     ).ToArray();

            createSentence += indexdefs.Length > 0 ?
                Environment.NewLine + string.Join($";{Environment.NewLine}", indexdefs) + ";" : string.Empty;


            return createSentence;
        }

        private string BuildFieldDefinition(DataRow rowDefinition, DataRow fieldReplace)
        {
            DataRow row = fieldReplace != null && fieldReplace["column_name"].ToString() ==
                rowDefinition["column_name"].ToString() ? fieldReplace : rowDefinition;

            return Environment.NewLine + row["column_name"].ToString() + ' ' +
                                (
                                row["data_type"].ToString() == "numeric" ?
                                    row["data_type"].ToString() + "(" + row["numeric_precision"].ToString() + "," + row["numeric_scale"].ToString() + ")"
                                    : row["data_type"].ToString()
                                )
                                + " " +
                                  (row["character_maximum_length"] != null && !string.IsNullOrEmpty(row["character_maximum_length"].ToString()) ?
                                 "(" + row["character_maximum_length"].ToString() + ")" :
                                 string.Empty)

                                 +
                                 (
                                 row["default_value"] != null && !string.IsNullOrEmpty(row["default_value"].ToString()) ?
                                 " DEFAULT " + row["default_value"].ToString() :
                                 string.Empty

                                 );
        }

        public DataTable GetFieldsAdvance(string tableName)
        {
            DataTable table = new DataTable();
            bool openLocalConexion = false;

            try
            {
                if (this.connection.State != ConnectionState.Open)
                {
                    this.OpenConnection(this.connection);
                    openLocalConexion = true;
                }


                int reintentos = 2;
                while (reintentos >= 0)
                {
                    try
                    {
                        string sqlDefaultValue = @"SELECT
	                                a.attname AS column_name,
	                                d.adsrc AS default_value
	                            FROM
	                                pg_attribute a
	                            JOIN
	                                pg_attrdef d ON
	                                a.attrelid = d.adrelid AND a.attnum = d.adnum
	                            JOIN
	                                pg_class c ON
	                                a.attrelid = c.oid
	                            WHERE
	                                c.relname = '" + tableName + "'";
                        if (this.ServerVersion > 12)
                        {
                            sqlDefaultValue = @"SELECT
                                    a.attname AS column_name,
                                    pg_get_expr(d.adbin, d.adrelid) AS default_value
                                FROM
                                    pg_catalog.pg_attrdef d
                                JOIN
                                    pg_catalog.pg_attribute a ON a.attnum = d.adnum AND a.attrelid = d.adrelid
                                JOIN
                                    pg_catalog.pg_class c ON c.oid = a.attrelid
                                JOIN
                                    pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                                WHERE
                                    n.nspname NOT LIKE 'pg_%' AND n.nspname != 'information_schema'
	                                AND c.relname = '" + tableName + "'";
                        }


                        string sql = @"SELECT ic.table_name, ic.column_name, ic.data_type, ic.character_maximum_length, ic.numeric_precision, ic.numeric_scale, df.default_value
                            FROM information_schema.columns ic
                            LEFT JOIN (
	                             " + sqlDefaultValue +
                            @") AS df ON df.column_name = ic.column_name
                        
                            WHERE ic.table_name = '" + tableName + "'" +
                            "ORDER BY ic.ordinal_position;";

                        using (NpgsqlCommand command = new NpgsqlCommand(sql, this.connection))
                        {
                            // Ajusta el tiempo de espera aquí (en segundos)
                            // 5 minutos
                            command.CommandTimeout = 60 * 5;
                            using (NpgsqlDataReader npgsqlDataReader = command.ExecuteReader())
                            {
                                DataTable schema = npgsqlDataReader.GetSchemaTable();
                                foreach (DataRow row in schema.Rows)
                                {
                                    table.Columns.Add(row["ColumnName"].ToString(), (Type)row["DataType"]);
                                }

                                table.Load(npgsqlDataReader);
                            }
                        }
                        reintentos = -1;

                    }
                    catch (Exception ex)
                    {

                        Console.WriteLine("Error GetFieldsAdvance -> " + ex.Message);
                        if (reintentos == 0)
                        {
                            throw ex;
                        }

                    }
                    finally
                    {
                        reintentos--;
                    }
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error GetFieldsAdvance " + tableName + " -> " + ex.Message);
                throw ex;
            }
            finally
            {
                if (openLocalConexion && this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }

            return table;
        }

        public DataTable GetPrimaryFields(string tableName)
        {
            DataTable table = new DataTable();
            bool openLocalConexion = false;

            try
            {
                if (this.connection.State != ConnectionState.Open)
                {
                    this.OpenConnection(this.connection);
                    openLocalConexion = true;
                }


                int reintentos = 2;
                while (reintentos >= 0)
                {
                    try
                    {
                        string sql = @"SELECT
                                kc.column_name AS column_name,
                                pc.conname AS constraint_name
                            FROM
                                information_schema.key_column_usage kc
                            JOIN
                                pg_constraint pc ON
                                kc.constraint_name = pc.conname
                            WHERE
                                pc.contype = 'p' AND
                                kc.table_name = ";

                        sql += $"'{tableName}';";

                        using (NpgsqlCommand command = new NpgsqlCommand(sql, this.connection))
                        {
                            using (NpgsqlDataReader npgsqlDataReader = command.ExecuteReader())
                            {
                                DataTable schema = npgsqlDataReader.GetSchemaTable();
                                foreach (DataRow row in schema.Rows)
                                {
                                    table.Columns.Add(row["ColumnName"].ToString(), (Type)row["DataType"]);
                                }

                                table.Load(npgsqlDataReader);
                            }
                        }
                        reintentos = -1;

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Error GetPrimaryFields ->" + ex.Message);
                        if (reintentos == 0)
                        {
                            throw ex;
                        }

                    }
                    finally
                    {
                        reintentos--;
                    }
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error GetPrimaryFields " + tableName + " -> " + ex.Message);
                throw ex;
            }
            finally
            {
                if (openLocalConexion && this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }

            return table;
        }


        public void SincronizeTable(DataTable data, string tableName)
        {
            try
            {
                // Ordenar aleatoriamente los datos
                var random = new Random();
                var dataTable = data.AsEnumerable()
                    .OrderBy(row => random.Next())
                    .Take(5).CopyToDataTable();


                // Construir la sentencia SQL
                // Inicializar la cadena SQL
                string sql = $"INSERT INTO {tableName} (";

                // Agregar los nombres de las columnas al inicio de la sentencia SQL
                sql += string.Join(", ", dataTable.Columns.Cast<DataColumn>().Select(column => column.ColumnName).ToArray());

                // Continuar la sentencia SQL con los valores
                sql += $") {Environment.NewLine} VALUES (";

                // Recorrer las filas y agregar los valores
                for (int i = 0; i < dataTable.Rows.Count; i++)
                {
                    DataRow row = dataTable.Rows[i];
                    for (int j = 0; j < row.ItemArray.Length; j++)
                    {
                        if (j > 0)
                        {
                            sql += ", ";
                        }

                        sql += $"@param_{i}_{j}";
                    }

                    if (i < dataTable.Rows.Count - 1)
                    {
                        sql += $"),{Environment.NewLine} (";
                    }
                }

                sql += ");";

                // Crear y agregar parámetros para evitar inyección de SQL
                using (NpgsqlCommand cmd = new NpgsqlCommand())
                {
                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        DataRow row = dataTable.Rows[i];
                        for (int j = 0; j < row.ItemArray.Length; j++)
                        {
                            cmd.Parameters.AddWithValue($"@param_{i}_{j}", row.ItemArray[j]);
                        }
                    }

                    cmd.CommandText = sql;
                    cmd.Connection = this.connection;
                    this.connection.Open();
                    cmd.ExecuteNonQuery();

                }

            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }

        }


        string ConstruirSentenciaSql(DataTable dataTable, string nombreTabla)
        {
            // Inicializar la cadena SQL
            string sql = $"INSERT INTO {nombreTabla} (";

            // Agregar los nombres de las columnas al inicio de la sentencia SQL
            sql += string.Join(", ", dataTable.Columns.Cast<DataColumn>().Select(column => column.ColumnName).ToArray());

            // Continuar la sentencia SQL con los valores
            sql += ") VALUES" + Environment.NewLine;

            // Recorrer las filas y agregar los valores
            foreach (DataRow row in dataTable.Rows)
            {
                sql += $"    ({string.Join(", ", row.ItemArray.Select(value => FormatearValor(value)).ToArray())}),{Environment.NewLine}";
            }

            // Quitar la coma adicional al final y agregar un punto y coma al final
            sql = sql.TrimEnd(',', '\r', '\n') + ";";

            return sql;
        }

        string FormatearValor(object valor)
        {
            // Asegurarse de que los valores de cadena estén entre comillas
            if (valor is string)
            {
                return $"'{valor}'";
            }

            return valor.ToString();
        }

        public void ExecuteSentence(string sqlSentence)
        {
            DataTable table = new DataTable();
            bool openLocalConexion = false;

            try
            {
                if (this.connection.State != ConnectionState.Open)
                {
                    this.OpenConnection(this.connection);
                    openLocalConexion = true;
                }


                int reintentos = 2;
                while (reintentos >= 0)
                {
                    try
                    {
                        using (NpgsqlCommand command = new NpgsqlCommand(sqlSentence, this.connection))
                        {
                            command.CommandTimeout = 60 * 30;
                            command.ExecuteNonQuery();
                        }
                        reintentos = -1;

                    }
                    catch (Exception ex)
                    {

                        if (ex is NpgsqlException && ((NpgsqlException)ex).Code == "42804")
                        {
                            NpgsqlException ngpEx = (NpgsqlException)ex;
                            throw new ApplicationException(ngpEx.Code + " " + ngpEx.Detail, ex);
                        }

                        if (reintentos == 0)
                        {
                            throw ex;
                        }

                    }
                    finally
                    {
                        reintentos--;
                    }
                }

            }
            catch (Exception ex)
            {
                //Console.WriteLine("Error ExecutingSentence -> " + sqlSentence + " -> " + ex.Message);
                throw new ApplicationException(ex.Message + "\r\n" + sqlSentence, ex);
            }
            finally
            {
                if (openLocalConexion && this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }
        }

        public void StartConnectionPool(string user, string pwd)
        {
            //Stopwatch timeMeasure = new Stopwatch();
            //timeMeasure.Start();

            int connectionPoolSize = 6;
            if (this.connectionPoolLocal == null)
            {
                this.connectionPoolLocal = new List<NpgsqlConnection>();
                for (int i = 0; i < connectionPoolSize; i++)
                {
                    NpgsqlConnection connectionLocal =
                                         new NpgsqlConnection(string.Concat(new string[] { "Server=", this.connection.Host,
                          ";Port=" + this.connection.Port + ";User ID=", user,
                          ";Password=", pwd, ";Database=", this.connection.Database, ";ApplicationName=SisnetComparerPool;" }));
                    this.OpenConnection(connectionLocal);
                    Console.WriteLine($"Pool {i} ServerVersion: {connectionLocal.ServerVersion}  {connectionLocal.PostgreSqlVersion}");
                    this.connectionPoolLocal.Add(connectionLocal);
                }
            }
            //timeMeasure.Stop();
            //Console.WriteLine($"Pool Tiempo: {timeMeasure.Elapsed.TotalMilliseconds} ms");
            Console.WriteLine("Finish pool");
        }

        public void FinishConnectionPool()
        {
            if (this.connectionPoolLocal != null)
            {
                foreach (var item in this.connectionPoolLocal)
                {
                    this.CloseConnection(item);
                }
            }

            this.connectionPoolLocal = null;
        }
        private NpgsqlConnection GetPoolingConnection()
        {
            Random rnd = new Random();
            int nd = rnd.Next(this.connectionPoolLocal.Count);
            //Console.Write($"Connection {nd} ");
            return this.connectionPoolLocal[nd];
        }

        public void BeginPoolTransaction()
        {
            if (this.connectionPoolLocal != null)
            {
                transactionPoolLocal = new List<NpgsqlTransaction>();
                foreach (var item in this.connectionPoolLocal)
                {
                    transactionPoolLocal.Add(item.BeginTransaction());
                }
            }
        }

        public void CommithPoolTransaction()
        {
            if (this.transactionPoolLocal != null)
            {
                foreach (var item in this.transactionPoolLocal)
                {
                    item.Commit();
                    item.Dispose();
                }
            }
            this.transactionPoolLocal = null;
        }

        public void RollbackPoolTransaction()
        {
            if (this.transactionPoolLocal != null)
            {
                foreach (var item in this.transactionPoolLocal)
                {
                    item.Rollback();
                    item.Dispose();
                }
            }
            this.transactionPoolLocal = null;
        }

        public void ExecuteRecord(string preparedStatement, object[] data, bool withLocalConnection)
        {
            bool openLocalConexion = false;

            NpgsqlConnection connectionLocal = null;
            //Stopwatch timeMeasure = new Stopwatch();
            //Stopwatch timeOC = new Stopwatch();

            try
            {
                //timeMeasure.Start();

                if (!withLocalConnection && this.connection.State != ConnectionState.Open)
                {
                    this.OpenConnection(this.connection);
                    openLocalConexion = true;
                }

                if (withLocalConnection)
                {
                    //timeOC.Start();

                    connectionLocal = GetPoolingConnection();
                    /*  new NpgsqlConnection(string.Concat(new string[] { "Server=", this.connection.Host,
                          ";Port=" + this.connection.Port + ";User ID=", user,
                          ";Password=", pwd, ";Database=", this.connection.Database }));
                    this.OpenConnection(connectionLocal);
                    */
                    //timeOC.Stop();

                }

                int reintentos = 2;
                while (reintentos >= 0)
                {
                    try
                    {
                        var parameters =
                                    data.Select((valor, indice) =>
                                    new NpgsqlParameter($"p{indice}", valor)).ToArray();

                        if (withLocalConnection)
                        {
                            lock (connectionLocal)
                            {
                                using (NpgsqlCommand command = new NpgsqlCommand(preparedStatement, connectionLocal))
                                {
                                    command.Parameters.AddRange(parameters);
                                    command.ExecuteNonQuery();
                                }
                            }
                        }
                        else
                        {
                            lock (this.connection)
                            {
                                using (NpgsqlCommand command = new NpgsqlCommand(preparedStatement, this.connection))
                                {
                                    command.Parameters.AddRange(parameters);
                                    command.ExecuteNonQuery();
                                }
                            }
                        }

                        reintentos = -1;

                    }
                    catch (Exception ex)
                    {
                        if (ex is NpgsqlException npgsqlException)
                        {
                            if (npgsqlException.Code == "53200" || npgsqlException.Code == "XX000")
                            {
                                this.ExecuteRecordODBC(preparedStatement, data);
                                return;
                            }
                        }
                        Console.WriteLine("Error ExecuteRecord -> " + preparedStatement + ex.Message);
                        if (reintentos == 0)
                        {
                            throw ex;
                        }

                    }
                    finally
                    {
                        reintentos--;
                    }
                }

            }
            catch (Exception ex)
            {
                string context = "Error ExecuteRecord -> " + preparedStatement + " -> " + ex.Message;
                Console.WriteLine(context);
                throw new ApplicationException(context, ex);
            }
            finally
            {
                if (!withLocalConnection && openLocalConexion && this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }

                //if (withLocalConnection && connectionLocal != null && connectionLocal.State == ConnectionState.Open)
                //{
                //connectionLocal.Close();
                //}
            }
            //timeMeasure.Stop();
            //Console.WriteLine($"{Thread.CurrentThread.ManagedThreadId} TC: {timeOC.Elapsed.TotalMilliseconds} ms | Tiempo: {timeMeasure.Elapsed.TotalMilliseconds} ms");


        }


        public void ExecuteRecordODBC(string preparedStatement, object[] data)
        {
            Console.WriteLine("ExecuteRecordODBC -> " + preparedStatement);
            bool openLocalConexion = false;
            OdbcConnection connection = GetODBCConnectionDestino();

            try
            {
                if (connection.State != ConnectionState.Open)
                {
                    OpenConnectionODBC(connection);
                    openLocalConexion = true;
                }


                int reintentos = 2;
                while (reintentos >= 0)
                {
                    try
                    {
                        lock (connection)
                        {
                            for (int i = 0; ; i++)
                            {
                                string search = "@p" + i;
                                if (preparedStatement.IndexOf(search) > 0)
                                {
                                    preparedStatement = preparedStatement.Replace(search, "?");
                                }
                                else
                                {
                                    break;
                                }
                            }

                            using (OdbcCommand command = new OdbcCommand(preparedStatement, connection))
                            {
                                command.CommandTimeout = 60 * 20;
                                var parameters =
                                    data.Select((valor, indice) =>
                                    new OdbcParameter($"?p{indice}", valor)).ToArray();

                                command.Parameters.AddRange(parameters);
                                command.ExecuteNonQuery();
                            }
                        }
                        reintentos = -1;

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Error ExecuteRecord -> " + preparedStatement + ex.Message);
                        if (reintentos == 0)
                        {
                            throw ex;
                        }

                    }
                    finally
                    {
                        reintentos--;
                    }
                }

            }
            catch (Exception ex)
            {
                string context = "Error ExecuteRecord ODBC -> " + preparedStatement + " -> " + ex.Message;
                Console.WriteLine(context);
                throw new ApplicationException(context, ex);
            }
            finally
            {
                if (openLocalConexion && connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
            }
        }


        public DataTable GetAdditionlIndices(string tableName)
        {
            DataTable table = new DataTable();
            bool openLocalConexion = false;

            try
            {
                if (this.connection.State != ConnectionState.Open)
                {
                    this.OpenConnection(this.connection);
                    openLocalConexion = true;
                }


                int reintentos = 2;
                while (reintentos >= 0)
                {
                    try
                    {
                        string sql = @"SELECT
                            indexname,
                            indexdef
                        FROM
                            pg_indexes
                        WHERE
                            tablename = '" + tableName + "' " +
                            @"AND indexname NOT IN(
	                        SELECT                               
	                        pc.conname 
	                        FROM
	                        information_schema.key_column_usage kc
	                        JOIN
	                        pg_constraint pc ON
	                        kc.constraint_name = pc.conname
	                        WHERE
	                        pc.contype = 'p' AND
	                        kc.table_name = '" + tableName + "'" +
                        ");";

                        using (NpgsqlCommand command = new NpgsqlCommand(sql, this.connection))
                        {
                            using (NpgsqlDataReader npgsqlDataReader = command.ExecuteReader())
                            {
                                DataTable schema = npgsqlDataReader.GetSchemaTable();
                                foreach (DataRow row in schema.Rows)
                                {
                                    table.Columns.Add(row["ColumnName"].ToString(), (Type)row["DataType"]);
                                }

                                table.Load(npgsqlDataReader);
                            }
                        }
                        reintentos = -1;

                    }
                    catch (Exception ex)
                    {

                        Console.WriteLine("Error GetIndices -> " + ex.Message);
                        if (reintentos == 0)
                        {
                            throw ex;
                        }

                    }
                    finally
                    {
                        reintentos--;
                    }
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error GetIndices " + tableName + " -> " + ex.Message);
                throw ex;
            }
            finally
            {
                if (openLocalConexion && this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }

            return table;
        }

        public DataTable GetForeginKeys(string tableName)
        {
            DataTable table = new DataTable();
            bool openLocalConexion = false;

            try
            {
                if (this.connection.State != ConnectionState.Open)
                {
                    this.OpenConnection(this.connection);
                    openLocalConexion = true;
                }


                int reintentos = 2;
                while (reintentos >= 0)
                {
                    try
                    {
                        string sql = @"SELECT
                            conname AS foreign_key_name,
                            conrelid::regclass AS table_origin,
                            a.attname AS column_origin,
                            confrelid::regclass AS table_destination,
                            af.attname AS column_destination,
                            confupdtype AS action_update,
                            confdeltype AS action_delete
                        FROM
                            pg_constraint c
                        JOIN
                            pg_attribute a ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid
                        JOIN
                            pg_attribute af ON af.attnum = ANY(c.confkey) AND af.attrelid = c.confrelid";

                        sql += (string.IsNullOrEmpty(tableName) ? ";" :
                            "WHERE conrelid = '" + tableName + "'::regclass;");

                        using (NpgsqlCommand command = new NpgsqlCommand(sql, this.connection))
                        {
                            command.CommandTimeout = 60;

                            using (NpgsqlDataReader npgsqlDataReader = command.ExecuteReader())
                            {
                                DataTable schema = npgsqlDataReader.GetSchemaTable();
                                foreach (DataRow row in schema.Rows)
                                {
                                    table.Columns.Add(row["ColumnName"].ToString(), (Type)row["DataType"]);
                                }

                                table.Load(npgsqlDataReader);
                            }
                        }
                        reintentos = -1;

                    }
                    catch (Exception ex)
                    {

                        Console.WriteLine("Error GetIndices -> " + ex.Message);
                        if (reintentos == 0)
                        {
                            throw ex;
                        }

                    }
                    finally
                    {
                        reintentos--;
                    }
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error GetIndices " + tableName + " -> " + ex.Message);
                throw ex;
            }
            finally
            {
                if (openLocalConexion && this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }

            return table;
        }

        private OdbcConnection GetODBCConnectionOrigen()
        {
            // Setup a connection string
            string connectionString = "DSN=sisnet64_origen;" +
                               "UID=postgres;" +
                               "PWD=postgres;" +
                               "Database=" + this.connection.Database + ";" +
                               "ApplicationName=SisnetComparerODBC";

            OdbcConnection connection = new OdbcConnection(connectionString);
            return connection;

        }

        private OdbcConnection GetODBCConnectionDestino()
        {
            // Setup a connection string
            string connectionString = "DSN=sisnet64_destino;" +
                               "UID=postgres;" +
                               "PWD=postgres;" +
                               "Database=" + this.connection.Database + ";" +
                               "ApplicationName=SisnetComparerODBC";

            OdbcConnection connection = new OdbcConnection(connectionString);
            return connection;

        }

        public void CopyTableExport(string tableName, string sharedfolder)
        {

            Stopwatch timeMeasure = new Stopwatch();
            //Stopwatch timeOC = new Stopwatch();

            try
            {
                timeMeasure.Start();



                int reintentos = 2;
                while (reintentos >= 0)
                {
                    try
                    {
                        string sql = $"COPY {tableName} TO E'"
                            + sharedfolder.Replace(@"\", @"\\")
                            + @"\\bkpshared.sql'";
                        using (NpgsqlCommand command = new NpgsqlCommand(sql, this.connection))
                        {
                            command.CommandTimeout = TWENTY_MINS;
                            command.ExecuteNonQuery();
                        }


                        reintentos = -1;

                    }
                    catch (Exception ex)
                    {
                        if (ex is NpgsqlException npgsqlException)
                        {

                        }
                        if (reintentos == 0)
                        {
                            throw ex;
                        }

                    }
                    finally
                    {
                        reintentos--;
                    }
                }

            }
            catch (Exception ex)
            {
                string context = "Error CopyTable export -> " + tableName + " " + ex.Message;
                Console.WriteLine(context);
                throw new ApplicationException(context, ex);
            }
           
            timeMeasure.Stop();
           Console.WriteLine($"{Thread.CurrentThread.ManagedThreadId} {tableName} Tiempo export: {timeMeasure.Elapsed.TotalMilliseconds} ms");


        }


        public void CopyTableImport(string tableName, string sharedfolder)
        {

            Stopwatch timeMeasure = new Stopwatch();
            //Stopwatch timeOC = new Stopwatch();

            try
            {
                timeMeasure.Start();



                int reintentos = 2;
                while (reintentos >= 0)
                {
                    try
                    {
                        string sql = $"COPY {tableName} FROM E'"
                            + sharedfolder.Replace(@"\", @"\\")
                            + @"\\bkpshared.sql'";
                        using (NpgsqlCommand command = new NpgsqlCommand(sql, this.connection))
                        {
                            command.CommandTimeout = TWENTY_MINS;
                            command.ExecuteNonQuery();
                        }


                        reintentos = -1;

                    }
                    catch (Exception ex)
                    {
                        if (ex is NpgsqlException npgsqlException)
                        {

                        }
                        if (reintentos == 0)
                        {
                            throw ex;
                        }

                    }
                    finally
                    {
                        reintentos--;
                    }
                }

            }
            catch (Exception ex)
            {
                string context = "Error CopyTable import -> " + tableName + " " + ex.Message;
                Console.WriteLine(context);
                throw new ApplicationException(context, ex);
            }

            timeMeasure.Stop();
            Console.WriteLine($"{Thread.CurrentThread.ManagedThreadId} {tableName} Tiempo import: {timeMeasure.Elapsed.TotalMilliseconds} ms");


        }


        public void OpenRetainConnection()
        {
            this.OpenConnection(this.connection);
        }


    }
}