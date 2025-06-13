using System.Collections.Generic;
using System.Linq;

namespace OdessaCDCwithSSIS
{
    public interface ISchemaSynchronizer
    {
        void SyncSchema(Dictionary<string, Dictionary<string, ColumnInfo>> sourceSchema, Dictionary<string, Dictionary<string, ColumnInfo>> destSchema, string targetConnStr);
    }

    public class SchemaSynchronizer : ISchemaSynchronizer
    {
        public void SyncSchema(
            Dictionary<string, Dictionary<string, ColumnInfo>> sourceSchema,
            Dictionary<string, Dictionary<string, ColumnInfo>> destSchema,
            string targetConnStr)
        {
            using (var conn = new System.Data.SqlClient.SqlConnection(targetConnStr))
            {
                conn.Open();

                // 1. Create new tables in destination
                foreach (var tableEntry in sourceSchema)
                {
                    string tableName = tableEntry.Key;
                    var sourceCols = tableEntry.Value;

                    if (!destSchema.ContainsKey(tableName))
                    {
                        // Table does not exist in destination, create it
                        var columnsSql = string.Join(", ", sourceCols.Values.Select(c => c.GetDefinition()));
                        var createTableSql = $"CREATE TABLE {sourceCols.Values.First().FullTableName} ({columnsSql});";
                        ExecuteDDL(conn, createTableSql);
                        continue;
                    }

                    // Existing logic for column add/alter
                    var destCols = destSchema[tableName];
                    foreach (var colEntry in sourceCols)
                    {
                        var col = colEntry.Value;
                        if (!destCols.ContainsKey(col.ColumnName))
                        {
                            var sql = $"ALTER TABLE {col.FullTableName} ADD {col.GetDefinition()};";
                            ExecuteDDL(conn, sql);
                        }
                        else
                        {
                            var destCol = destCols[col.ColumnName];
                            if (!col.IsCompatibleWith(destCol))
                            {
                                var sql = $"ALTER TABLE {col.FullTableName} ALTER COLUMN {col.GetDefinition()};";
                                ExecuteDDL(conn, sql);
                            }
                        }
                    }
                }

                // 2. Drop tables from destination that are not in source
                foreach (var destTable in destSchema.Keys)
                {
                    if (!sourceSchema.ContainsKey(destTable))
                    {
                        var dropTableSql = $"DROP TABLE [{destTable.Replace(".", "].[")}]";
                        ExecuteDDL(conn, dropTableSql);
                    }
                }
            }
        }

        private void ExecuteDDL(System.Data.SqlClient.SqlConnection conn, string sql)
        {
            try
            {
                System.Console.WriteLine("Executing: " + sql);
                using (var cmd = new System.Data.SqlClient.SqlCommand(sql, conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
            catch (System.Exception ex)
            {
                System.Console.ForegroundColor = System.ConsoleColor.Red;
                System.Console.WriteLine($"Error: {ex.Message}");
                System.Console.ResetColor();
            }
        }
    }
}
