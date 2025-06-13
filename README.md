# OdessaCDC_SSIS
Create a SSIS package to perform data migration from Audit DB in one server to another server
Purpose:
Automates the synchronization of SQL Server database schemas and data between a source and a target using C# (.NET Framework 4.8) and dynamically generated SSIS packages.
Key Features:
•	Schema Synchronization:
•	Adds new tables/columns to the target if they exist in the source but not in the target.
•	Alters columns in the target to match the source.
•	Drops tables from the target if they no longer exist in the source.
•	Incremental Data Movement:
•	Uses LSN (Log Sequence Number) tracking to move only new or changed data.
•	Retrieves and filters LSNs for each table to ensure efficient data transfer.
•	SSIS Package Automation:
•	Dynamically builds SSIS packages using EzAPI.
•	Configures data flows for each table and applies LSN-based filtering.
•	Supports package-level transactions for all-or-nothing execution (if enabled).
Architecture:
•	Modular design with clear separation of concerns:
•	SchemaLoader: Loads schema metadata.
•	SchemaSynchronizer: Handles schema changes.
•	LsnProvider: Manages LSN retrieval and filtering.
•	SsisPackageBuilder: Builds and (optionally) executes SSIS packages.
•	ColumnInfo: Represents column metadata.
Usage:
•	Update connection strings in Program.cs.
•	Run the application to synchronize schema and data.
•	Generated SSIS package is saved to disk and can be executed as needed.
