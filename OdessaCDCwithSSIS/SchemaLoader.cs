using System.Collections.Generic;

namespace OdessaCDCwithSSIS
{
    public interface ISchemaLoader
    {
        Dictionary<string, Dictionary<string, ColumnInfo>> LoadSchema(string connStr);
        List<string> GetTableNames(string connStr);
    }

    public class SchemaLoader : ISchemaLoader
    {
        public Dictionary<string, Dictionary<string, ColumnInfo>> LoadSchema(string connStr)
        {
            var result = new Dictionary<string, Dictionary<string, ColumnInfo>>(System.StringComparer.OrdinalIgnoreCase);
            using (var conn = new System.Data.SqlClient.SqlConnection(connStr))
            {
                conn.Open();
                string sql = @"SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS";
                using (var cmd = new System.Data.SqlClient.SqlCommand(sql, conn))
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string table = $"{reader["TABLE_SCHEMA"]}.{reader["TABLE_NAME"]}";
                        string column = reader["COLUMN_NAME"].ToString();
                        string dataType = reader["DATA_TYPE"].ToString();
                        string isNullable = reader["IS_NULLABLE"].ToString();
                        int? length = reader["CHARACTER_MAXIMUM_LENGTH"] as int?;
                        if (!result.ContainsKey(table))
                            result[table] = new Dictionary<string, ColumnInfo>(System.StringComparer.OrdinalIgnoreCase);
                        result[table][column] = new ColumnInfo
                        {
                            ColumnName = column,
                            DataType = dataType,
                            MaxLength = length,
                            IsNullable = isNullable == "YES",
                            FullTableName = $"[{reader["TABLE_SCHEMA"]}].[{reader["TABLE_NAME"]}]"
                        };
                    }
                }
            }
            return result;
        }

        public List<string> GetTableNames(string connStr)
        {
            var tableNames = new List<string>();
            using (var conn = new System.Data.SqlClient.SqlConnection(connStr))
            {
                conn.Open();
                using (var cmd = new System.Data.SqlClient.SqlCommand("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES", conn))
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        tableNames.Add(reader.GetString(0));
                    }
                }
            }
            return tableNames;
        }
    }
}
