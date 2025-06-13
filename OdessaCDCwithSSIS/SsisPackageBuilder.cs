using System.Collections.Generic;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.SSIS.EzAPI;
using System;
using System.Linq;
using Microsoft.SqlServer.Dts.Runtime;

namespace OdessaCDCwithSSIS
{
    public interface ISsisPackageBuilder
    {
        void BuildAndExecuteSsisPackage(string sourceConnectionString, string targetConnectionString, List<string> tableNames, IEnumerable<KeyValuePair<string, byte[]>> targetLSNCollection);
    }

    public class SsisPackageBuilder : ISsisPackageBuilder
    {
        public void BuildAndExecuteSsisPackage(string sourceConnectionString, string targetConnectionString, List<string> tableNames, IEnumerable<KeyValuePair<string, byte[]>> targetLSNCollection)
        {

            var basePackage = new Package
            {
                TransactionOption = DTSTransactionOption.Required 
            };

            //var package = new EzPackage { MaxConcurrentExecutables = 3 };

            var package = new EzPackage(basePackage)
            {
                MaxConcurrentExecutables = 3
            };

            var sourceConn = new EzOleDbConnectionManager(package)
            {
                Name = "SourceConnection",
                ConnectionString = sourceConnectionString,
                RetainSameConnection = true
            };

            var targetConn = new EzOleDbConnectionManager(package)
            {
                Name = "TargetConnection",
                ConnectionString = targetConnectionString,
                RetainSameConnection = true
            };

            foreach (string tableName in tableNames)
            {
                var dataFlow = new EzDataFlow(package) { Name = $"DataFlow_{tableName}" };

                dataFlow.TransactionOption = DTSTransactionOption.Supported; 

                var source = new EzOleDbSource(dataFlow)
                {
                    Name = $"Source_{tableName}",
                    Connection = sourceConn,
                    AccessMode = AccessMode.AM_OPENROWSET
                };
                string targetLSNForTable = targetLSNCollection.FirstOrDefault(x => x.Key == tableName).Value != null
                    ? "0x" + BitConverter.ToString(targetLSNCollection.FirstOrDefault(x => x.Key == tableName).Value).Replace("-", "")
                    : "NULL";

                source.SqlCommand = tableName == "LastProcessedLSN"
                    ? $"SELECT * FROM [{tableName}]"
                    : $"SELECT * FROM [{tableName}] WHERE Audit_TransactionLSN > ({targetLSNForTable})";
                source.LinkAllInputsToOutputs();
                var destination = new EzOleDbDestination(dataFlow)
                {
                    Name = $"Destination_{tableName}",
                    Connection = targetConn,
                    Table = $"[{tableName}]",
                    UsesDispositions = true,
                    AccessMode = AccessMode.AM_OPENROWSET_FASTLOAD,
                    FastLoadKeepIdentity = true
                };
                destination.LinkAllInputsToOutputs();
                destination.AttachTo(source);
            }
            package.SaveToFile(@"D:\DynamicPackage01.dtsx");
            //package.Execute();
        }
    }
}
