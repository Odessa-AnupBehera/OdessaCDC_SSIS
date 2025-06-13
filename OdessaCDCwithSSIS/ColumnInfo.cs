namespace OdessaCDCwithSSIS
{
    public class ColumnInfo
    {
        public string ColumnName { get; set; }
        public string DataType { get; set; }
        public int? MaxLength { get; set; }
        public bool IsNullable { get; set; }
        public string FullTableName { get; set; }

        public string GetDefinition()
        {
            string type = DataType;
            if (MaxLength.HasValue)
            {
                if (MaxLength == -1)
                    type += "(MAX)";
                else
                    type += $"({MaxLength})";
            }
            return $"[{ColumnName}] {type} {(IsNullable ? "NULL" : "NOT NULL")}";
        }

        public bool IsCompatibleWith(ColumnInfo other)
        {
            return string.Equals(DataType, other.DataType, System.StringComparison.OrdinalIgnoreCase)
                   && MaxLength == other.MaxLength
                   && IsNullable == other.IsNullable;
        }
    }
}
