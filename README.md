Win: DSEDiagnosticConsoleApplication.exe -?
OSX: ./DSEDiagnosticConsoleApplication.Core -?

Arguments including Default Values:

	-D, --DiagnosticPath [Default Value "<DiagFolder>"] (Required) The directory location of the diagnostic files. The structure of these folders and files is depending on the value of DiagnosticNoSubFolders. If Relative Path, this path is merged with current default directory path. The defaults are defined in the DiagnosticPath app-config file.
	-P, --ExcelFilePath [Default Value "<none>"] Excel target file. If Relative Path, this path is merged with the current default file path. If the default file path is null, the DiagnosticPath is used for the merger. The defaults are defined in the ExcelFilePath app-config file.
	-E, --ExcelFileTemplatePath [Default Value ".\dseHealthAssessment4Template.xlsm"] Excel Template file that is used to create Excel target file (ExcelFilePath), if it doesn't already exists.
	-L, --AlternativeLogFilePath [Default Value "<none>", Multiple Allowed] A file paths that will be included in log parsing, if enabled. This can include wild card patterns. Multiple arguments allowed.
		Example: -L "c:\additionallogs\system*.log" -L "c:\additional logs\debug*.log"
	-C, --AlternativeDDLFilePath [Default Value "<none>", Multiple Allowed] A file paths that will be included in DLL (CQL) parsing, if enabled. This can include wild card patterns. Multiple arguments allowed.
		Example: -C "c:\additionalDDL\describe_schema" -C "c:\additionalDDL\describe.cql"
	-Z, --AlternativeCompressionFilePath [Default Value "<none>", Multiple Allowed] A file paths that will be included in the decompress process, if enabled. This can include wild card patterns. Multiple arguments allowed.
		Example: -Z "c:\additionalZip\system.log.1.zip" -Z "c:\additionalZip\system.2.zip"
	-A, --AlternativeFilePath [Default Value "<none>", Multiple Allowed] Key-Value pair where the key is a File Parsing Class (e.g., file_cassandra_log4net_ReadTimeRange, cql_ddl, file_unzip)  or Category Type (e.g., LogFile, CQLFile, ZipFile) and the value is a file path. The file path can contain wild cards. The Key and Value are separated by a comma.
		Example: -A "ZipFile, c:\additionalfiles\*.tar.gz"
	--OnlyNodes [Default Value "<none>", Multiple Allowed] Only process these nodes. This can be an IP Address separated by a comma or multiple argument commands
		Example: --OnlyNodes "10.0.0.1, 10.0.0.2"
	-O, --DiagFolderStruct [Default Value "OpsCtrDiagStruct"] Structure of the folders and file names used to determine the context of each file. Values are: OpsCtrDiagStruct (default), NodeAgentDiagStruct, IndivFiles, and NodeSubFldStruct
	-T, --DiagCaptureTime [Default Value "<none>"] When the OpsCenter Diagnostic TarBall was created or when the "nodetool" statistical (e.g., cfstats) capture occurred. Null will use the Date embedded in the OpsCenter tar ball directory. Syntax should be that of a date time offset (+|-HH[:MM] and no IANA name accepted). If time zone offset not given, current machine's offset is used.
	-X, --LogRangeBasedOnPrevHrs [Default Value "168"] Only import log entries based on the previous <X> hours from DiagCaptureTime. only valid if DiagCaptureTime is defined or is a OpsCtrDiagStruct. Value of "-1" disables this option
	-R, --LogTimeRange [Default Value "<none>"] Only import log entries from/to this date/time range. Empty string will parse all entries. Syntax: "<FromDateTimeOnly> [+|-HH[:MM]]|[IANA TimeZone Name]", ", <ToDateTimeOnly> [+|-HH[:MM]]|[IANA TimeZone Name]", or "<FromDateTime> [+|-HH[:MM]]|[IANA TimeZone Name],<ToDateTime> [+|-HH[:MM]]|[IANA TimeZone Name]". If [IANA TimeZone Name] or [+|-HH[:MM]] (timezone offset) is not given the local machine's TZ is used. Ignored if LogRangeBasedOnPrevHrs is defined.
		Example: -R "2018-02-01 00:00:00 +00:00, 2018-02-21 00:00:00 UDT" only logs between Feb01 to Feb21 2018 UDT -or- -R ",2018-02-21 00:00:00 UDT" All logs up to Feb21 2018 UDT -or- -R "2018-02-01 00:00:00 UDT" only logs from Feb01 2018 UDT to last log entries
	--IgnoreKeySpaces [Default Value "dse_system, system_auth, system_distributed, system_schema, system, dse_security, solr_admin, dse_auth, dse_leases, system_traces, dse_perf, HiveMetaStore, cfs_archive, cfs"] A list of keyspaces that will be ignored separated by a comma. If a keyspace begins with a "+" or "-" it will be either added or removed from the current default ignored list. The defaults are defined in the IgnoreKeySpaces app-config file.
	--WarnWhenKSTblIsDetected [Default Value "system.paxos, system_traces, dse_perf, system_auth, dse_security, system.batches, system.batchlog, system.hints"] A list of keyspaces or tables/views that will be marked as "Warn" and will be excluded from the Ignore keyspace list. Each item is separated by a comma. If the item begins with a "+" or "-" it will be either added or removed from the current default list. The defaults are defined in the WarnWhenKSTblIsDetected app-config file.
	--ProcessFileMappingPath [Default Value "D:\\Users\RichardAndersen\Documents\Projects\DataStax\GitRepro\DSEDiagnosticApplication\DSEDiagnosticConsoleApplication\bin\Debug\Json\ProcessFileMappingsValidate.json"] File Mapper Json file used to define which and how files are processed.
	--DisableSysDSEKSLoading If defined the system/DSE keyspaces are not auto-loaded. The default is to load these keyspaces. Note that the app-config file element DSESystemDDL of DSEDiagnosticFileParser lib contains the DDL of the keyspaces/tables loaded.
	--LogFileGapAnalysis [Default Value "15"] The number of minutes that will determine if a Gap occurred between Log File events. A log file event is information around the actual log file which included the starting timestamp and ending timestamp within a file.
	--LogFileContinousAnalysis [Default Value "4"] The minimal number of days to determine a series of file log events as a continuous single event. A log file event is information around the actual log file which included the starting timestamp and ending timestamp within a file.
	--AppendToExcelWorkSheet If present, all existing worksheets in the Excel workbook will NOT be cleared but instead will be appended to existing data.
	--LogLinePatternLayout [Default Value "%-5level [%thread] %date{ISO8601} %F:%L - %msg%n"] The definition that defines the format of a Log Line. This definition must follow the Apache Log PatternLayout configuration. See https://logging.apache.org/log4j/2.x/manual/configuration.html#Loggers
	--DSEVersion [Default Value "<none>"] The DSE Version to use for processing
	--DefaultClusterTimeZone [Default Value "UTC"] An IANA TimeZone name used as the default time zone for all data centers/nodes where the time zone could not be determined.
		Example: --DefaultClusterTimeZone "America/Chicago"
	--DefaultDCTimeZone [Default Value "<none>", Multiple Allowed] Key-Value pair where the key is the Data Center name and the value is the IANA time zone name. The Key and Value are separated by a comma.
		Example: --DefaultDCTimeZone "DC1, America/Chicago"
	--DefaultNodeTimeZone [Default Value "<none>", Multiple Allowed] Key-Value pair where the key is the Node's IP Address and the value is the IANA time zone name. The Key and Value are separated by a comma.
		Example: --DefaultNodeTimeZone "10.0.0.1, America/Chicago"
	--ClusterName [Default Value "<none>"] The name of the cluster
	--ClusterHashCode [Default Value "0"] The cluster's hash code.
	--LogEventMemoryMapping [Default Value "False"] If true the Log Events are mapped into virtual memory, if false Log Events are kept in physical memory (better performance but can OOM).
	--Profile [Default Value "Validate"] The Profile used to parse, transform, and analysis the data. Profile Names:
		AllFilesLogs -- Process all diagnostic files. This is the typical profile used for most health checks. 
		NoLogs -- Process all diagnostic file except any log files. 
		Validate -- Process only a subset of diagnostic files just to obtain enough information to validate the diagnostic files provided by the customer.
		Decompression -- only decompress any compressed files.
		CreateOpsCenterStruct -- Special profile that can be used to take a collection of diagnostic files not generated by OpsCenter and restructures them into a OpsCenter folder structure. 
		AllNoFlushComp -- Process all diagnostic files and logs except compaction/flush events.
		ValidateWLogs -- Similar to Validate except more validation on logs to detect any gaps in the log periods.
	--LogAggrPeriod [Default Value "00:30:00"] Log Aggregation Period in HH:MM:SS format
	--ExcelPackageCache [Default Value "True"] Enables/Disables the Excel Package caching. If disabled each time a worksheet is generated the workbook is saved and reloaded (Excel Package is deleted and recreated).
	--ExcelWorkSheetSave [Default Value "False"] Enables/Disables the saving of a workbook each time a worksheet is added/modified. If ExcelPackageCache is disabled this is always enabled.
	--IgnoreLogTagEvent [Default Value "<none>", Multiple Allowed] A Log Event Tag Id that will cause that associated log event to be ignored during parsing. If an integral value (i.e., 10, no decimal) and a session event, all items in that session are ignored. Multiple arguments can be defined.
		Example: 10 -- all session items associated to this id tag (e.g., 10.0, 10.1, 10.2, etc.); 10.1 -- only the item associated to the id of 10.1.	
	--LogIgnoreParsingErrors If defined, all parsing errors related to logs will be ignored. Warning: If defined unexpected results may occur including abnormal termination of the application, exceptions, and/or invalid/missing log event generation.
	-B, --Batch Enables Batch Mode, which basically disables prompts and enables Exception Tracing
	--TraceException [Default is disabled] Enables or Disables exception tracing depending on the application config setting
	--Debug Enables Debug Mode
	--DisableParallelProcessing Disable Parallel Processing
	--ShowDefaults Show Arguments plus Default Values
	-?, --help Show Arguments plus Default Values
	--ShowVersion Shows the Application's Version Information

Steps to process an OpsCenter diagnostic tarball:
	1) Decompress the tarball into a target folder (e.g., "c:\MyCustomerName")
		Once decompressed the OpsCenter's diagnostic folder should be placed in the targeted folder (e.g., "c:\MyCustomerName\CustClusterName-diagnostics-2019_01_21_16_30_46_UTC")
	2) Open the "Heath Check Console" command prompt or a DOS command prompt
	3) Run the Health Check Engine.
		Typical Example: DSEDiagnosticConsoleApplication.exe -D "c:\MyCustomerName\CustClusterName-diagnostics-2019_01_21_16_30_46_UTC" --Profile AllFilesLogs
	4) Once the application starts execution, observe the console screen for progress, warnings, errors, and exceptions
		If there are any warnings or errors, please review the "parsing error" and "not handled" worksheets in the generated Excel workbook for the details.
		If an exception occurs, please notify Richard Andersen
	5) Once completed, the generated Excel workbook is placed under the OptCenter's diagnostic folder (e.g., "c:\MyCustomerName\CustClusterName-diagnostics-2019_01_21_16_30_46_UTC\CustClusterName-2018-09-25-10-11-34-AllNoFlushComp-190123To0116.xlsm")
	6) Open the generated workbook (MS Excel version 16 and higher is required) and click on the "Refresh" button located in the "Refresh" worksheet to update the pivot tables and caches in the workbook
	7) Save the workbook so that the updated pivot tables have the current data
	
Notes: If the customer provided rolled/archived logs (either system or debug), they can be placed in an folder named "AdditionalLogs" under the OptCenter's diagnostic folder (e.g., "c:\MyCustomerName\CustClusterName-diagnostics-2019_01_21_16_30_46_UTC\AdditionalLogs")
	These additional logs need to be defined in a certain folder structure. Each set of logs need to be placed in a separate folder where that folder's name is the IP address of the node associated with the logs (e.g., "c:\MyCustomerName\CustClusterName-diagnostics-2019_01_21_16_30_46_UTC\AdditionalLogs\10.0.0.1\systemlog.3.228.zip")
	If there are any compressed files under the OptCenter's diagnostic fold, the engine will automatically decompress these files.
	The engine can also consume non-OpsCenter diagnostic files, please contact Richard Andersen on details

This can be built using MS Visual Studio 2017 or Published.
The solutions are:
     DSEDiagnosticApplication.sln – Which builds .Net Framework 4.x for Winx64 Platform
     DSEDiagnosticApplication.Core.sln – Which builds the .Net Core 2.x cross-platform version (MacOS, Linux, Win)
                                         You will also new to point NuGet to a local repro for third party packages. This folder is under the solution's folder and is named "NuGetRepo"
                                         To publish the application for OSX use the below command line:
                                            dotnet publish-o <your publish folder> -framework netcoreapp2.2 --runtime osx-x64 --self-contained   -c Release-NoRepro

Note that both solutions share the same source code so there is no difference in features/usage. There is a difference in performance, the Winx64 (.Net Framework 4.x) is much faster. I haven’t had time to optimize the .Net Core version… Also, there is a set of scripts that you can use that will allow the application to run with multiple uses. This script is located in the repo (HelathCheckBatFiles.zip). You will need the log4net config file located in the zip file for multiple user regardless of platform.
