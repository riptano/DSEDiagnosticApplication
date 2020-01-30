using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;
using Common;

namespace DSEDiagnosticToDataSource
{

    public static class Data
    {        
        public struct RunInfo
        {
            public string DiagnosticId;
            public string ClusterName;
            public Guid? ClusterId;
            public Guid? InsightClusterId;
            public string ApplicationVersion;
            public DateTimeOffset RunStartTime;
            public DateTimeOffset? AnalysisPeriodStart;
            public DateTimeOffset? AnalysisPeriodEnd;
            public TimeSpan AggregationDuration;
            public string ApplicationArgs;
            public bool Aborted;
            public int Errors;
            public int Warnings;
            public int Exceptions;
            public int NbrDCs;
            public int NbrNodes;
            public int NbrKeyspaces;
            public int NbrTables;
            public string DataQuality;            
        }

        public static int ExportRunInfo(SqlConnection connection, RunInfo runInfo)
        {
            using (var cmd = new SqlCommand("INSERT INTO [RunMetaData](" +
                                                                        "[DiagnosticId]," +
                                                                        "[Cluster Name]," +
                                                                        "[RunTimestamp]," +
                                                                        "[ClusterId]," +
                                                                        "[InsightClusterId]," +
                                                                        "[AnalysisPeriodStart]," +
                                                                        "[AnalysisPeriodEnd]," +
                                                                        "[AggregationDuration]," +
                                                                        "[NbrDCs]," +
                                                                        "[NbrNodes]," +
                                                                        "[NbrKeyspaces]," +
                                                                        "[NbrTables]," +
                                                                        "[Diagnostic Data Quality]," +
                                                                        "[Aborted]," +
                                                                        "[RunExceptions]," +
                                                                        "[RunErrors]," +
                                                                        "[RunWarnings]," +
                                                                        "[Version]," +
                                                                        "[CommandLine]) " +
                                                                        "VALUES(" +
                                                                            "@DiagnosticId," +
                                                                            "@ClusterName," +
                                                                            "@RunTimestamp," +
                                                                            "@ClusterId," +
                                                                            "@InsightClusterId," +
                                                                            "@AnalysisPeriodStart," +
                                                                            "@AnalysisPeriodEnd," +
                                                                            "@AggregationDuration," +
                                                                            "@NbrDCs," +
                                                                            "@NbrNodes," +
                                                                            "@NbrKeyspaces," +
                                                                            "@NbrTables," +
                                                                            "@DiagnosticDataQuality," +
                                                                            "@Aborted," +
                                                                            "@RunExceptions," +
                                                                            "@RunErrors," +
                                                                            "@RunWarnings," +
                                                                            "@Version," +
                                                                            "@CommandLine )",
                                            connection))

            {
                cmd.Parameters.AddWithValue("@DiagnosticId", runInfo.DiagnosticId);
                cmd.Parameters.AddWithValue("@ClusterName", runInfo.ClusterName);
                cmd.Parameters.AddWithValue("@RunTimestamp", runInfo.RunStartTime);
                cmd.Parameters.AddWithValue("@ClusterId", runInfo.ClusterId.HasValue ? (object) runInfo.ClusterId.Value : DBNull.Value).IsNullable = true;
                cmd.Parameters.AddWithValue("@InsightClusterId", runInfo.InsightClusterId.HasValue ? (object) runInfo.InsightClusterId.Value : DBNull.Value).IsNullable = true;
                cmd.Parameters.AddWithValue("@AnalysisPeriodStart", runInfo.AnalysisPeriodStart).IsNullable = true;
                cmd.Parameters.AddWithValue("@AnalysisPeriodEnd", runInfo.AnalysisPeriodEnd).IsNullable = true;
                cmd.Parameters.AddWithValue("@AggregationDuration", runInfo.AggregationDuration.Ticks);
                cmd.Parameters.AddWithValue("@NbrDCs", runInfo.NbrDCs);
                cmd.Parameters.AddWithValue("@NbrNodes", runInfo.NbrNodes);
                cmd.Parameters.AddWithValue("@NbrKeyspaces", runInfo.NbrKeyspaces);
                cmd.Parameters.AddWithValue("@NbrTables", runInfo.NbrTables);
                cmd.Parameters.AddWithValue("@DiagnosticDataQuality", runInfo.DataQuality).IsNullable = true;
                cmd.Parameters.AddWithValue("@Aborted", runInfo.Aborted).IsNullable = true;
                cmd.Parameters.AddWithValue("@RunExceptions", runInfo.Exceptions);
                cmd.Parameters.AddWithValue("@RunErrors", runInfo.Errors);
                cmd.Parameters.AddWithValue("@RunWarnings", runInfo.Warnings);
                cmd.Parameters.AddWithValue("@Version", runInfo.ApplicationVersion).IsNullable = true;
                cmd.Parameters.AddWithValue("@CommandLine", runInfo.ApplicationArgs).IsNullable = true;
                
                return cmd.ExecuteNonQuery();               
            }
        }

        public static void BulkCopy(string connectionString,
                                        IEnumerable<DataTable> dataTables,
                                        RunInfo runInfo,
                                        System.Threading.CancellationTokenSource cancellationSource,
                                        Action<DataTable> uponStart = null,
                                        Action<DataTable> uponFinish = null)
        {
            var cancellationToken = cancellationSource.Token;

            cancellationToken.ThrowIfCancellationRequested();

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                
                var loopOptions = new ParallelOptions()
                {
                    CancellationToken = cancellationToken,
                    MaxDegreeOfParallelism = Math.Min(dataTables.Count() / 4, 3),
                    TaskScheduler = TaskScheduler.Current
                };

                cancellationToken.ThrowIfCancellationRequested();

                ExportRunInfo(connection, runInfo);

                //Parallel.ForEach(dataTables, loopOptions, dataTable =>
                foreach (var dataTable in dataTables)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (uponStart != null) uponStart(dataTable);

                    BulkCopy(connection, dataTable, runInfo.DiagnosticId);

                    if (uponFinish != null) uponFinish(dataTable);
                }//);
            }
        }

        /*
        public static void BulkCopy(string connectionString,
                                        DataTable dataTable,
                                        RunInfo runInfo)
        {

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();               
                BulkCopy(connection, dataTable, runInfo.DiagnosticId);               
            }
        }*/

        public static void BulkCopy(SqlConnection connection, DataTable dataTable, string diagnosticId)
        {                    
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
            {                
                bulkCopy.DestinationTableName = dataTable.TableName;
                try
                {
                    using (var reader = new DataReader(dataTable, diagnosticId))
                    {
                        foreach (DataColumn c in reader.GetSchemaTable().Columns)
                        {
                            bulkCopy.ColumnMappings.Add(c.ColumnName, c.ColumnName);                            
                        }

                        bulkCopy.WriteToServer(reader);
                    }                    
                }
                catch
                {
                    throw;
                }
            }
        }
    }

    public class SqlTableCreator
    {
        #region Instance Variables
        private SqlConnection _connection;
        public SqlConnection Connection
        {
            get { return _connection; }
            set { _connection = value; }
        }

        private SqlTransaction _transaction;
        public SqlTransaction Transaction
        {
            get { return _transaction; }
            set { _transaction = value; }
        }

        private string _tableName;
        public string DestinationTableName
        {
            get { return _tableName; }
            set { _tableName = value; }
        }
        #endregion

        #region Constructor
        public SqlTableCreator() { }
        public SqlTableCreator(SqlConnection connection) : this(connection, null) { }
        public SqlTableCreator(SqlConnection connection, SqlTransaction transaction)
        {
            _connection = connection;
            _transaction = transaction;
        }
        #endregion

        #region Instance Methods
        public object Create(DataTable schema)
        {
            return Create(schema, null);
        }
        public object Create(DataTable schema, int numKeys)
        {
            int[] primaryKeys = new int[numKeys];
            for (int i = 0; i < numKeys; i++)
            {
                primaryKeys[i] = i;
            }
            return Create(schema, primaryKeys);
        }
        public object Create(DataTable schema, int[] primaryKeys)
        {
            string sql = GetCreateSQL(_tableName, schema, primaryKeys);

            SqlCommand cmd;
            if (_transaction != null && _transaction.Connection != null)
                cmd = new SqlCommand(sql, _connection, _transaction);
            else
                cmd = new SqlCommand(sql, _connection);

            return cmd.ExecuteNonQuery();
        }

        public object CreateFromDataTable(DataTable table)
        {
            string sql = GetCreateFromDataTableSQL(_tableName, table);

            SqlCommand cmd;
            if (_transaction != null && _transaction.Connection != null)
                cmd = new SqlCommand(sql, _connection, _transaction);
            else
                cmd = new SqlCommand(sql, _connection);

            return cmd.ExecuteNonQuery();
        }
        #endregion

        #region Static Methods

        public static string GetCreateSQL(string tableName, DataTable schema, int[] primaryKeys)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                tableName = schema.TableName;
            }

            string sql = "CREATE TABLE [" + tableName + "] (\n";

            // columns
            foreach (DataRow column in schema.Rows)
            {
                if (!(schema.Columns.Contains("IsHidden") && (bool)column["IsHidden"]))
                {
                    sql += "\t[" + column["ColumnName"].ToString() + "] " + SQLGetType(column);

                    if (schema.Columns.Contains("AllowDBNull") && (bool)column["AllowDBNull"] == false)
                        sql += " NOT NULL";

                    sql += ",\n";
                }
            }
            sql = sql.TrimEnd(new char[] { ',', '\n' }) + "\n";

            // primary keys
            string pk = ", CONSTRAINT PK_" + tableName + " PRIMARY KEY CLUSTERED (";
            bool hasKeys = (primaryKeys != null && primaryKeys.Length > 0);
            if (hasKeys)
            {
                // user defined keys
                foreach (int key in primaryKeys)
                {
                    pk += schema.Rows[key]["ColumnName"].ToString() + ", ";
                }
            }
            else
            {
                // check schema for keys
                string keys = string.Join(", ", GetPrimaryKeys(schema));
                pk += keys;
                hasKeys = keys.Length > 0;
            }
            pk = pk.TrimEnd(new char[] { ',', ' ', '\n' }) + ")\n";
            if (hasKeys) sql += pk;

            sql += ")";

            return sql;
        }

        public static string GetCreateFromDataTableSQL(string tableName, DataTable table)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                tableName = table.TableName;
            }

            string sql = "CREATE TABLE [" + tableName + "] (\n";
            // columns
            foreach (DataColumn column in table.Columns)
            {
                sql += "[" + column.ColumnName + "] " + SQLGetType(column) + ",\n";
            }
            sql = sql.TrimEnd(new char[] { ',', '\n' }) + "\n";
            // primary keys
            if (table.PrimaryKey.Length > 0)
            {
                sql += "CONSTRAINT [PK_" + tableName + "] PRIMARY KEY CLUSTERED (";
                foreach (DataColumn column in table.PrimaryKey)
                {
                    sql += "[" + column.ColumnName + "],";
                }
                sql = sql.TrimEnd(new char[] { ',' }) + "))\n";
            }

            //if not ends with ")"
            if ((table.PrimaryKey.Length == 0) && (!sql.EndsWith(")")))
            {
                sql += ")";
            }

            return sql;
        }

        public static string[] GetPrimaryKeys(DataTable schema)
        {
            List<string> keys = new List<string>();

            foreach (DataRow column in schema.Rows)
            {
                if (schema.Columns.Contains("IsKey") && (bool)column["IsKey"])
                    keys.Add(column["ColumnName"].ToString());
            }

            return keys.ToArray();
        }

        // Return T-SQL data type definition, based on schema definition for a column
        public static string SQLGetType(object type, int columnSize, int numericPrecision, int numericScale)
        {
            switch (type.ToString())
            {
                case "System.String":
                    return "VARCHAR(" + ((columnSize == -1) ? "255" : (columnSize > 8000) ? "MAX" : columnSize.ToString()) + ")";

                case "System.Object":
                    return "VARCHAR(" + ((columnSize == -1) ? "MAX" : (columnSize > 8000) ? "MAX" : columnSize.ToString()) + ")";

                case "System.Decimal":
                    return "DECIMAL";

                case "System.Double":
                case "System.Single":
                    return "REAL";

                case "System.UInt64":
                case "System.Int64":
                    return "BIGINT";

                case "System.Int16":
                case "System.Int32":
                    return "INT";

                case "System.DateTime":
                    return "DATETIME";

                case "System.Boolean":
                    return "BIT";

                case "System.Byte":
                    return "TINYINT";

                case "System.Guid":
                    return "UNIQUEIDENTIFIER";

                case "System.TimeSpan":
                    return "BIGINT";

                case "System.DateTimeOffset":
                    return "datetimeoffset";

                default:
                    throw new Exception(type.ToString() + " not implemented.");
            }
        }

        // Overload based on row from schema table
        public static string SQLGetType(DataRow schemaRow)
        {
            return SQLGetType(schemaRow["DataType"],
                                int.Parse(schemaRow["ColumnSize"].ToString()),
                                int.Parse(schemaRow["NumericPrecision"].ToString()),
                                int.Parse(schemaRow["NumericScale"].ToString()));
        }
        // Overload based on DataColumn from DataTable type
        public static string SQLGetType(DataColumn column)
        {
            return SQLGetType(column.DataType,
                                column.DataType == typeof(string)
                                    && (column.ColumnName == "DDL" || column.ColumnName == "Value")
                                    && column.MaxLength == -1 ? 8001 : column.MaxLength,
                                10, 2);
        }
        #endregion
    }

}
