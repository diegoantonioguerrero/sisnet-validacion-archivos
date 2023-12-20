using Npgsql;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using System.ComponentModel;
using System.Threading;

namespace SisnetData
{
    public class DBManager
    {
        public static DBManager _self;

        private NpgsqlConnection connection;

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
                    this.OpenConnection();
                    string sqlTables = @"SELECT table_name, pg_size_pretty(pg_total_relation_size(table_name)) AS size_text, pg_total_relation_size(table_name) AS size
                        FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                  --              and table_name = 'actuacionespendientes'
                        order by table_name
 --limit 15
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

        public void SetDBManager(string ip, string db, string user, string pwd, bool testConnection = true)
        {
            try
            {
                this.connection = new NpgsqlConnection(string.Concat(new string[] { "Server=", ip, ";Port=5432;User ID=", user, ";Password=", pwd, ";Database=", db }));
                if (!testConnection)
                {
                    return;
                }
                this.OpenConnection();
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

        public List<string> GetBds()
        {
            List<string> databaseNames = new List<string>();
            try
            {
                try
                {
                    this.OpenConnection();
                    using (NpgsqlDataReader dataReader = (new NpgsqlCommand("SELECT datname FROM pg_database ORDER BY datname;", this.connection)).ExecuteReader())
                    {
                        while (dataReader.Read())
                        {
                            databaseNames.Add(dataReader.GetString(0));
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            }
            finally
            {
                this.CloseConnection();
            }
            return databaseNames;
        }

        public void OpenConnection()
        {
            int reintentos = 4;
            while (reintentos >= 0)
            {
                try
                {
                    if (this.connection != null && this.connection.State != ConnectionState.Open)
                    {
                        this.connection.Open();
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
            if (this.connection != null && this.connection.State == ConnectionState.Open)
            {
                this.connection.Close();
            }
        }
        public DataTable GetTableData(string tableName, bool loadOnlySchema = false)
        {
            DataTable table = new DataTable();
            bool openLocalConexion = false;

            try
            {
                if (this.connection.State != ConnectionState.Open)
                {
                    this.OpenConnection();
                    openLocalConexion = true;
                }


                int reintentos = 2;
                while (reintentos >= 0)
                {
                    try
                    {

                        string sql = $"SELECT * FROM {tableName}" + (loadOnlySchema ? "WHERE 1=2" : string.Empty  ) + " ORDER BY 1;";
                        using (NpgsqlCommand command = new NpgsqlCommand(sql, this.connection))
                        {
                            // Ajusta el tiempo de espera aquí (en segundos)
                            // 20 minutos
                            command.CommandTimeout = 60 * 20;
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

                                }

                                table.Load(npgsqlDataReader);
                            }
                        }
                        reintentos = -1;

                    }
                    catch (Exception ex)
                    {
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
                Console.WriteLine("Error GetTableData " + tableName + " -> " + ex.Message);
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

        public string GetCreateSchemaTable(string tableName)
        {
            DataTable dataTableFields = this.GetFieldsAdvance(tableName);
            string[] columns = (from DataRow row in dataTableFields.Rows
                                select Environment.NewLine + row["column_name"].ToString() + ' ' +
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

                                 )
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
        public DataTable GetFieldsAdvance(string tableName)
        {
            DataTable table = new DataTable();
            bool openLocalConexion = false;

            try
            {
                if (this.connection.State != ConnectionState.Open)
                {
                    this.OpenConnection();
                    openLocalConexion = true;
                }


                int reintentos = 2;
                while (reintentos >= 0)
                {
                    try
                    {
                        string sql = @"SELECT ic.table_name, ic.column_name, ic.data_type, ic.character_maximum_length, ic.numeric_precision, ic.numeric_scale, df.default_value
                            FROM information_schema.columns ic
                            LEFT JOIN (
	                            SELECT
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
	                                c.relname = '" + tableName + "'" +
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
                    this.OpenConnection();
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
                    this.OpenConnection();
                    openLocalConexion = true;
                }


                int reintentos = 2;
                while (reintentos >= 0)
                {
                    try
                    {
                        using (NpgsqlCommand command = new NpgsqlCommand(sqlSentence, this.connection))
                        {
                            command.ExecuteNonQuery();
                        }
                        reintentos = -1;

                    }
                    catch (Exception ex)
                    {

                        Console.WriteLine("Error ExecuteSentence -> " + sqlSentence + ex.Message);
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
                Console.WriteLine("Error ExecutingSentence -> " + sqlSentence + " -> " + ex.Message);
                throw ex;
            }
            finally
            {
                if (openLocalConexion && this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
                }
            }
        }


        public void ExecuteRecord(string preparedStatement, object[] data)
        {
            bool openLocalConexion = false;

            try
            {
                if (this.connection.State != ConnectionState.Open)
                {
                    this.OpenConnection();
                    openLocalConexion = true;
                }


                int reintentos = 2;
                while (reintentos >= 0)
                {
                    try
                    {
                        using (NpgsqlCommand command = new NpgsqlCommand(preparedStatement, this.connection))
                        {

                            var parameters =
                                data.Select((valor, indice) =>
                                new NpgsqlParameter($"p{indice}", valor)).ToArray();

                            command.Parameters.AddRange(parameters);

                            command.ExecuteNonQuery();
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
                Console.WriteLine("Error ExecuteRecord -> " + preparedStatement + " -> " + ex.Message);
                throw ex;
            }
            finally
            {
                if (openLocalConexion && this.connection.State == ConnectionState.Open)
                {
                    this.connection.Close();
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
                    this.OpenConnection();
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
                    this.OpenConnection();
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


    }
}