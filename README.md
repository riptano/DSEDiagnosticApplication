# Arguments including Default Values

    **-D, --DiagnosticPath** [Default Value "<none>"] The directory location of the diagnostic files. The structure of these folders and files is depending on the value of DiagnosticNoSubFolders. If Relative Path, this path is merged with current default directory path. The defaults are defined in the DiagnosticPath app-config file.
    **-P, --ExcelFilePath** [Default Value "<none>"] Excel target file. If Relative Path, this path is merged with the current default file path. If the default file path is null, the DiagnosticPath is used for the merger. The defaults are defined in the ExcelFilePath app-config file.
    **-E, --ExcelFileTemplatePath** [Default Value ".\dseHealthAssessment3Template.xlsm"] Excel Template file that is used to create Excel target file (ExcelFilePath), if it doesn't already exists.
    **-L, --AlternativeLogFilePath [Default Value "<none>", Multiple Allowed] A file paths that will be included in log parsing, if enabled. This can include wild card patterns. Multiple arguments allowed.
        Exmaple: -L c:\additionallogs\system*.log -L c:\additional logs\debug*.log
    **-C, --AlternativeDDLFilePath** [Default Value "<none>", Multiple Allowed] A file paths that will be included in DLL (CQL) parsing, if enabled. This can include wild card patterns. Multiple arguments allowed.
        Exmaple: -C c:\additionalDDL\describe_schema -C c:\additionalDDL\describe.cql
    **-Z, --AlternativeCompressionFilePath** [Default Value "<none>", Multiple Allowed] A file paths that will be included in the decompress process, if enabled. This can include wild card patterns. Multiple arguments allowed.
        Exmaple: -Z c:\additionalZip\system.log.1.zip -Z c:\additionalZip\system.2.zip
    **-A, --AlternativeFilePath** [Default Value "<none>", Multiple Allowed] Key-Value pair where the key is a File Parsing Class (e.g., file_cassandra_log4net_ReadTimeRange, cql_ddl, file_unzip)  or Category Type (e.g., LogFile, CQLFile, ZipFile) and the value is a file path. The file path can contain wild cards. The Key and Value are separated by a comma.
        Exmaple: ZipFile, c:\additionalfiles\*.tar.gz
    **--OnlyNodes** [Default Value "<none>", Multiple Allowed] Only process these nodes. This can be an IP Address separated by a comma or multiple argument commands
        Exmaple: 10.0.0.1, 10.0.0.2
    **-O, --DiagFolderStruct** [Default Value "OpsCtrDiagStruct"] Structure of the folders and file names used to determine the context of each file. Values are: OpsCtrDiagStruct (default), NodeAgentDiagStruct, IndivFiles, and NodeSubFldStruct
    **-T, --DiagCaptureTime** [Default Value "<none>"] When the OpsCenter Diagnostic TarBall was created or when the "nodetool" statical (e.g., cfstats) capture occurred. Null will use the Date embedded in the OpsCenter tar ball directory. Syntax should be that of a date time offset (+|-HH[:MM] and no IANA name accepted). If time zone offset not given, current machine's offset is used.
    **-X, --LogRangeBasedOnPrevHrs** [Default Value "168"] Only import log entries based on the previous <X> hours from DiagCaptureTime. only valid if DiagCaptureTime is defined or is a OpsCtrDiagStruct. Value of "-1" disables this option
    **-R, --LogTimeRange** [Default Value "<none>"] Only import log entries from/to this date/time range. Empty string will parse all entries. Syntax: "<FromDateTimeOnly> [+|-HH[:MM]]|[IANA TimeZone Name]", ", <ToDateTimeOnly> [+|-HH[:MM]]|[IANA TimeZone Name]", or "<FromDateTime> [+|-HH[:MM]]|[IANA TimeZone Name],<ToDateTime> [+|-HH[:MM]]|[IANA TimeZone Name]". If [IANA TimeZone Name] or [+|-HH[:MM]] (timezone offset) is not given the local machine's TZ is used. Ignored if LogRangeBasedOnPrevHrs is defined.
        Exmaple: "2018-02-01 00:00:00 +00:00, 2018-02-21 00:00:00 UDT" only logs between Feb01 to Feb21 2018 UDT -or- ",2018-02-21 00:00:00 UDT" All logs up to Feb21 2018 UDT -or- "2018-02-01 00:00:00 UDT" only logs from Feb01 2018 UDT to last log entries
    **--IgnoreKeySpaces** [Default Value "dse_system, system_auth, system_distributed, system_schema, system, dse_security, solr_admin, dse_auth, dse_leases, system_traces, dse_perf, HiveMetaStore, cfs_archive, cfs"] A list of keyspaces that will be ignored separated by a comma. If a keyspace begins with a "+" or "-" it will be either added or removed from the current default ignored list. The defaults are defined in the IgnoreKeySpaces app-config file.
    **--WarnWhenKSTblIsDetected** [Default Value "system.paxos, system_traces, dse_perf, system_auth, dse_security, system.batches, system.hints"] A list of keyspaces or tables/views that will be marked as "Warn" and will be excluded from the Ignore keyspace list. Each item is separated by a comma. If the item begins with a "+" or "-" it will be either added or removed from the current default list. The defaults are defined in the WarnWhenKSTblIsDetected app-config file.
    **--ProcessFileMappingPath** [Default Value ".\Json\ProcessFileMappingsValidate.json"] File Mapper Json file used to define which and how files are processed.
    **--DisableSysDSEKSLoading** [Default Value "False"] If defined the system/DSE keyspaces are not auto-loaded. The default is to load these keyspaces. Note that the app-config file element DSESystemDDL of DSEDiagnosticFileParser lib contains the DDL of the keyspaces/tables loaded.
    **--LogFileGapAnalysis** [Default Value "15"] The number of minutes that will determine if a Gap occurred between Log File events. A log file event is information around the actual log file which included the starting timestamp and ending timestamp within a file.
    **--LogFileContinousAnalysis** [Default Value "4"] The minimal number of days to determine a series of file log events as a continuous single event. A log file event is information around the actual log file which included the starting timestamp and ending timestamp within a file.
    **--AppendToExcelWorkSheet** [Default Value "False"] If present, all existing worksheets in the Excel workbook will NOT be cleared but instead will be appended to existing data.
    **--LogLinePatternLayout** [Default Value "%-5level [%thread] %date{ISO8601} %F:%L - %msg%n"] The definition that defines the format of a Log Line. This definition must follow the Apache Log PatternLayout configuration.
    **--DSEVersion** [Default Value "<none>"] The DSE Version to use for processing
    **--DefaultClusterTimeZone** [Default Value "UTC"] An IANA TimeZone name used as the default time zone for all data centers/nodes where the time zone could not be determined.
        Exmaple: America/Chicago
    **--DefaultDCTimeZone** [Default Value "<none>", Multiple Allowed] Key-Value pair where the key is the Data Center name and the value is the IANA time zone name. The Key and Value are separated by a comma.
        Exmaple: DC1, America/Chicago
    **--DefaultNodeTimeZone** [Default Value "<none>", Multiple Allowed] Key-Value pair where the key is the Node's IP Address and the value is the IANA time zone name. The Key and Value are separated by a comma.
        Exmaple: 10.0.0.1, America/Chicago
    **--ClusterName** [Default Value "<none>"] The name of the cluster
    **--ClusterHashCode** [Default Value "0"] The cluster's hash code.
    **--LogEventMemoryMapping** [Default Value "False"] If true the Log Events are mapped into virtual memory, if false Log Events are kept in physical memory (better performance but can OOM).
    **--Profile** [Default Value "Validate"] The Profile used to parse, transform, and analysis the data. Profile Names: "AllFilesLogs, NoLogs, Validate, Decompression, CreateOpsCenterStruct, AllNoFlushComp, ValidateWLogs"
    **--LogAggrPeriod** [Default Value "00:30:00"] Log Aggregation Period in DD:HH:MM format
    **--ExcelPackageCache** [Default Value "True"] Enables/Disables the Excel Package caching. If disabled each time a worksheet is generated the workbook is saved and reloaded (Excel Package is deleted and recreated).
    **--ExcelWorkSheetSave** [Default Value "False"] Enables/Disables the saving of a workbook each time a worksheet is added/modified. If ExcelPackageCache is disabled this is always enabled.
    **--IgnoreLogTagEvent** [Default Value "<none>", Multiple Allowed] A Log Event Tag Id that will cause that associated log event to be ignored during parsing. If an integral value (i.e., 10, no decimal) and a session event, all items in that session are ignored. Multiple arguments can be defined.
        Exmaple: 10 -- all session items associated to this id tag (e.g., 10.0, 10.1, 10.2, etc.); 10.1 -- only the item assocated to the id of 10.1.
    **--OldExcelWS** [Default Value "False"] Old Excel Worksheet Format Used
    **-B, --Batch** [Default Value "False"] Enables Batch Mode, which basically disables prompts and enables Exception Tracing
    **--TraceException** [Default Value "True"] Disables exception tracing
    **--LogIgnoreParsingErrors** [Default Value "False"] If defined, all parsing errors related to logs will be ignored. Warning: If defined unexpected results may occur including abnormal termination of the application, exceptions, and/or invalid/missing log event generation.
    **--Debug** [Default Value "False"] Debug Mode
    **--DisableParallelProcessing** [Default Value "False"] Disable Parallel Processing
    **-?, --ShowDefaults**, Show Arguments plus Default Values

# Examples:

DSEDiagnosticConsoleApplication.exe -D "C:\Users\richard.andersen\Desktop\OpsCenter-Diag-Tar-Ball\MyCluster-diagnostics-2018_08_31_21_48_10_UTC" --Profile AllFilesLogs
