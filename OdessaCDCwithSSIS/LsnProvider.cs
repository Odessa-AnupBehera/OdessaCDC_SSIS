using System.Collections.Generic;

namespace OdessaCDCwithSSIS
{
    public interface ILsnProvider
    {
        List<KeyValuePair<string, byte[]>> GetLsnCollection(string connStr);
        IEnumerable<KeyValuePair<string, byte[]>> FilterSourceLsns(List<KeyValuePair<string, byte[]>> lsnSourceCollection, List<KeyValuePair<string, byte[]>> lsnTargetCollection);
    }

    public class LsnProvider : ILsnProvider
    {
        public List<KeyValuePair<string, byte[]>> GetLsnCollection(string connStr)
        {
            var lsnCollection = new List<KeyValuePair<string, byte[]>>();
            using (var conn = new System.Data.SqlClient.SqlConnection(connStr))
            {
                conn.Open();
                string sql = @"
            SELECT TableName, MAX(start_lsn) AS start_lsn
            FROM dbo.LastProcessedLSN
            GROUP BY TableName";
                using (var cmd = new System.Data.SqlClient.SqlCommand(sql, conn))
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string tableName = reader["TableName"] as string;
                        byte[] startLsn = reader["start_lsn"] as byte[];
                        if (tableName != null && startLsn != null)
                        {
                            lsnCollection.Add(new KeyValuePair<string, byte[]>(tableName, startLsn));
                        }
                    }
                }
            }
            return lsnCollection;
        }

        public IEnumerable<KeyValuePair<string, byte[]>> FilterSourceLsns(List<KeyValuePair<string, byte[]>> lsnSourceCollection, List<KeyValuePair<string, byte[]>> lsnTargetCollection)
        {
            var targetSet = new HashSet<KeyValuePair<string, byte[]>>(lsnTargetCollection);
            return lsnSourceCollection.FindAll(kv => !targetSet.Contains(kv));
        }
    }
}
