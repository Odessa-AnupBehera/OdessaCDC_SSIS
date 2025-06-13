using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace OdessaCDCwithSSIS
{
    class Program
    {
        static void Main(string[] args)
        {
            // Setup dependencies
            ISchemaLoader schemaLoader = new SchemaLoader();
            ILsnProvider lsnProvider = new LsnProvider();
            ISchemaSynchronizer schemaSynchronizer = new SchemaSynchronizer();
            ISsisPackageBuilder ssisPackageBuilder = new SsisPackageBuilder();

            // Connection strings
            string sourceConnectionString = "data source=use2lwintprddb01.850f952d4987.database.windows.net,1433;Initial Catalog=TBD_Defect_Integration_Audit_SI;User ID=usmdbuser;password=password-12;MultipleActiveResultSets=True;Provider=MSOLEDBSQL.1;";
            string targetConnectionString = "data source=LWO-LAP1834;Initial Catalog=TBD_Defect_Integration_Audit_SI;User ID=development;password=jk;MultipleActiveResultSets=True;Provider=MSOLEDBSQL.1;";
            string sourceConnectionStringWOProvider = "data source=use2lwintprddb01.850f952d4987.database.windows.net,1433;Initial Catalog=TBD_Defect_Integration_Audit_SI;User ID=usmdbuser;password=password-12;MultipleActiveResultSets=True;";
            string targetConnectionStringWOProvider = "data source=LWO-LAP1834;Initial Catalog=TBD_Defect_Integration_Audit_SI;User ID=development;password=jk;MultipleActiveResultSets=True;";

            // Load schemas
            var sourceSchema = schemaLoader.LoadSchema(sourceConnectionStringWOProvider);
            var destSchema = schemaLoader.LoadSchema(targetConnectionStringWOProvider);

            // Get table names and LSN collections
            var tableNames = schemaLoader.GetTableNames(targetConnectionStringWOProvider);
            //var lsnSourceCollection = lsnProvider.GetLsnCollection(sourceConnectionStringWOProvider);
            var lsnTargetCollection = lsnProvider.GetLsnCollection(targetConnectionStringWOProvider);

            // Sync schema from source to target
            schemaSynchronizer.SyncSchema(sourceSchema, destSchema, targetConnectionStringWOProvider);

            // Filter LSNs to process
            //var sourceFiltered = lsnProvider.FilterSourceLsns(lsnSourceCollection, lsnTargetCollection);

            // Build and execute SSIS package
            ssisPackageBuilder.BuildAndExecuteSsisPackage(
                sourceConnectionString,
                targetConnectionString,
                tableNames,
                lsnTargetCollection
            );
        }
    }
}