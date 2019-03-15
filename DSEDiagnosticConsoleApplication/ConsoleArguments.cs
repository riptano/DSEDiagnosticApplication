using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Common.Path;
using Common.Patterns.TimeZoneInfo;
using CommandLineParser.Arguments;

namespace DSEDiagnosticConsoleApplication
{
    public class DirectoryArgument : CommandLineParser.Arguments.DirectoryArgument
    {
        public DirectoryArgument(char shortName) : base(shortName) { }
        public DirectoryArgument(string longName) : base(longName) { }
        public DirectoryArgument(char shortName, string longName) : base(shortName, longName) { }
        public DirectoryArgument(char shortName, string longName, string description) : base(shortName, longName, description) { }

        public override System.IO.DirectoryInfo Convert(string stringValue)
        {
            return Common.Path.PathUtils.BuildDirectoryPath(stringValue)?.DirectoryInfo();
        }

    }

    public class FileArgument : CommandLineParser.Arguments.FileArgument
    {
        public FileArgument(char shortName) : base(shortName) { }
        public FileArgument(string longName) : base(longName) { }
        public FileArgument(char shortName, string longName) : base(shortName, longName) { }
        public FileArgument(char shortName, string longName, string description) : base(shortName, longName, description) { }

        public override System.IO.FileInfo Convert(string stringValue)
        {
            return Common.Path.PathUtils.BuildFilePath(stringValue)?.FileInfo();
        }
        
    }

    internal sealed class ConsoleArguments
    {
        private CommandLineParser.CommandLineParser _cmdLineParser = new CommandLineParser.CommandLineParser();

        public ConsoleArguments()
        {
            this._cmdLineParser.ShowUsageOnEmptyCommandline = true;
           
            Profiles.SetProfile();

            this._cmdLineParser.Arguments.Add(new DirectoryArgument('D', "DiagnosticPath")
            {
                Optional = false, //Required
                DirectoryMustExist = true,
                DefaultValue = ParserSettings.DiagnosticPath?.DirectoryInfo(),
                Description = "The directory location of the diagnostic files. The structure of these folders and files is depending on the value of DiagnosticNoSubFolders. If Relative Path, this path is merged with current default directory path. The defaults are defined in the DiagnosticPath app-config file."
            });

            this._cmdLineParser.Arguments.Add(new FileArgument('P', "ExcelFilePath")
            {
                Optional = true,
                FileMustExist = false,
                DefaultValue = ParserSettings.ExcelFilePath?.FileInfo(),
                Description = "Excel target file. If Relative Path, this path is merged with the current default file path. If the default file path is null, the DiagnosticPath is used for the merger. The defaults are defined in the ExcelFilePath app-config file."
            });

            this._cmdLineParser.Arguments.Add(new FileArgument('E', "ExcelFileTemplatePath")
            {
                Optional = true,
                FileMustExist = true,
                DefaultValue = ParserSettings.ExcelFileTemplatePath?.FileInfo(),
                Description = "Excel Template file that is used to create Excel target file (ExcelFilePath), if it doesn't already exists."
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>('L', "AlternativeLogFilePath")
            {
                Optional = true,
                AllowMultiple = true,
                DefaultValue = string.Join(", ", ParserSettings.AdditionalFilesForParsingClass.Where(a => a.Key == "LogFile" || a.Key == "file_cassandra_log4net_ReadTimeRange" || a.Key == "file_cassandra_log4net").Select(s => s.Value)),
                Description = "A file paths that will be included in log parsing, if enabled. This can include wild card patterns. Multiple arguments allowed.",
                Example = "-L \"c:\\additionallogs\\system*.log\" -L \"c:\\additional logs\\debug*.log\""
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>('C', "AlternativeDDLFilePath")
            {
                Optional = true,
                AllowMultiple = true,
                DefaultValue = string.Join(", ", ParserSettings.AdditionalFilesForParsingClass.Where(a => a.Key == "CQLFile" || a.Key == "cql_ddl").Select(s => s.Value)),
                Description = "A file paths that will be included in DLL (CQL) parsing, if enabled. This can include wild card patterns. Multiple arguments allowed.",
                Example = "-C \"c:\\additionalDDL\\describe_schema\" -C \"c:\\additionalDDL\\describe.cql\""
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>('Z', "AlternativeCompressionFilePath")
            {
                Optional = true,
                AllowMultiple = true,
                DefaultValue = string.Join(", ", ParserSettings.AdditionalFilesForParsingClass.Where(a => a.Key == "ZipFile" || a.Key == "file_unzip").Select(s => s.Value)),
                Description = "A file paths that will be included in the decompress process, if enabled. This can include wild card patterns. Multiple arguments allowed.",
                Example = "-Z \"c:\\additionalZip\\system.log.1.zip\" -Z \"c:\\additionalZip\\system.2.zip\""
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>('A', "AlternativeFilePath")
            {
                Optional = true,
                AllowMultiple = true,
                DefaultValue = string.Join(", ", ParserSettings.AdditionalFilesForParsingClass.Where(a => !(a.Key == "LogFile" || a.Key == "file_cassandra_log4net_ReadTimeRange" || a.Key == "file_cassandra_log4net" || a.Key == "CQLFile" || a.Key == "cql_ddl" || a.Key == "ZipFile" || a.Key == "file_unzip")).Select(s => s.Key + ", " + s.Value)),
                Description = "Key-Value pair where the key is a File Parsing Class (e.g., file_cassandra_log4net_ReadTimeRange, cql_ddl, file_unzip)  or Category Type (e.g., LogFile, CQLFile, ZipFile) and the value is a file path. The file path can contain wild cards. The Key and Value are separated by a comma.",
                Example = "-A \"ZipFile, c:\\additionalfiles\\*.tar.gz\""
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>("OnlyNodes")
            {
                Optional = true,
                AllowMultiple = true,
                DefaultValue = string.Join(", ", ParserSettings.OnlyNodes),
                Description = "Only process these nodes. This can be an IP Address separated by a comma or multiple argument commands",
                Example = "--OnlyNodes \"10.0.0.1, 10.0.0.2\""
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<ParserSettings.DiagFolderStructOptions>('O', "DiagFolderStruct")
            {
                DefaultValue = ParserSettings.DiagFolderStructOptions.OpsCtrDiagStruct,
                Optional = true,
                Description = "Structure of the folders and file names used to determine the context of each file. Values are: OpsCtrDiagStruct (default), NodeAgentDiagStruct, IndivFiles, and NodeSubFldStruct"
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<DateTimeOffset?>('T', "DiagCaptureTime")
            {
                DefaultValue = ParserSettings.NodeToolCaptureTimestamp,
                Optional = true,
                Description = "When the OpsCenter Diagnostic TarBall was created or when the \"nodetool\" statistical (e.g., cfstats) capture occurred. Null will use the Date embedded in the OpsCenter tar ball directory. Syntax should be that of a date time offset (+|-HH[:MM] and no IANA name accepted). If time zone offset not given, current machine's offset is used."
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<int>('X', "LogRangeBasedOnPrevHrs")
            {
                Optional = true,
                DefaultValue = ParserSettings.OnlyIncludeXHrsofLogsFromDiagCaptureTime,
                Description = "Only import log entries based on the previous <X> hours from DiagCaptureTime. only valid if DiagCaptureTime is defined or is a OpsCtrDiagStruct. Value of \"-1\" disables this option"
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>('R', "LogTimeRange")
            {
                Optional = true,
                DefaultValue = ParserSettings.LogRestrictedTimeRange == null ? null : string.Format("{0}, {1}", ParserSettings.LogRestrictedTimeRange.Min, ParserSettings.LogRestrictedTimeRange.Max),
                Description = "Only import log entries from/to this date/time range. Empty string will parse all entries. Syntax: \"<FromDateTimeOnly> [+|-HH[:MM]]|[IANA TimeZone Name]\", \", <ToDateTimeOnly> [+|-HH[:MM]]|[IANA TimeZone Name]\", or \"<FromDateTime> [+|-HH[:MM]]|[IANA TimeZone Name],<ToDateTime> [+|-HH[:MM]]|[IANA TimeZone Name]\". If [IANA TimeZone Name] or [+|-HH[:MM]] (timezone offset) is not given the local machine's TZ is used.",
                Example = "-R \"2018-02-01 00:00:00 +00:00, 2018-02-21 00:00:00 UDT\" only logs between Feb01 to Feb21 2018 UDT -or- -R \",2018-02-21 00:00:00 UDT\" All logs up to Feb21 2018 UDT -or- -R \"2018-02-01 00:00:00 UDT\" only logs from Feb01 2018 UDT to last log entries"
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>("IgnoreKeySpaces")
            {
                Optional = true,
                DefaultValue = string.Join(", ", ParserSettings.IgnoreKeySpaces),
                Description = "A list of keyspaces that will be ignored separated by a comma. If a keyspace begins with a \"+\" or \"-\" it will be either added or removed from the current default ignored list. The defaults are defined in the IgnoreKeySpaces app-config file."
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>("WarnWhenKSTblIsDetected")
            {
                Optional = true,
                DefaultValue = string.Join(", ", ParserSettings.WarnWhenKSTblIsDetected),
                Description = "A list of keyspaces or tables/views that will be marked as \"Warn\" and will be excluded from the Ignore keyspace list. Each item is separated by a comma. If the item begins with a \"+\" or \"-\" it will be either added or removed from the current default list. The defaults are defined in the WarnWhenKSTblIsDetected app-config file."
            });

            this._cmdLineParser.Arguments.Add(new FileArgument("ProcessFileMappingPath")
            {
                Optional = true,
                FileMustExist = true,
                DefaultValue = Common.Path.PathUtils.BuildFilePath(DSEDiagnosticFileParser.LibrarySettings.ProcessFileMappingValue)?.FileInfo(),
                Description = "File Mapper Json file used to define which and how files are processed."
            });

            this._cmdLineParser.Arguments.Add(new SwitchArgument("DisableSysDSEKSLoading", false)
            {
                Optional = true,
                Description = "If defined the system/DSE keyspaces are not auto-loaded. The default is to load these keyspaces. Note that the app-config file element DSESystemDDL of DSEDiagnosticFileParser lib contains the DDL of the keyspaces/tables loaded."
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<int>("LogFileGapAnalysis")
            {
                DefaultValue = ParserSettings.LogFileInfoAnalysisGapTriggerInMins,
                Optional = true,
                Description = "The number of minutes that will determine if a Gap occurred between Log File events. A log file event is information around the actual log file which included the starting timestamp and ending timestamp within a file."
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<int>("LogFileContinousAnalysis")
            {
                DefaultValue = ParserSettings.LogFileInfoAnalysisContinousEventInDays,
                Optional = true,
                Description = "The minimal number of days to determine a series of file log events as a continuous single event. A log file event is information around the actual log file which included the starting timestamp and ending timestamp within a file."
            });

            this._cmdLineParser.Arguments.Add(new SwitchArgument("AppendToExcelWorkSheet", ParserSettings.AppendToWorkSheet)
            {
                Optional = true,
                Description = "If present, all existing worksheets in the Excel workbook will NOT be cleared but instead will be appended to existing data."
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>("LogLinePatternLayout")
            {
                DefaultValue = ParserSettings.Log4NetConversionPattern,
                Optional = true,
                Description = "The definition that defines the format of a Log Line. This definition must follow the Apache Log PatternLayout configuration. See https://logging.apache.org/log4j/2.x/manual/configuration.html#Loggers"
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>("DSEVersion")
            {
                DefaultValue = ParserSettings.DSEVersion?.ToString(),
                Optional = true,
                Description = "The DSE Version to use for processing"
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>("DefaultClusterTimeZone")
            {
                Optional = true,
                DefaultValue = ParserSettings.DefaultClusterTZ,
                Description = "An IANA TimeZone name used as the default time zone for all data centers/nodes where the time zone could not be determined.",
                Example = "--DefaultClusterTimeZone \"America/Chicago\""
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>("DefaultDCTimeZone")
            {
                Optional = true,
                AllowMultiple = true,
                Description = "Key-Value pair where the key is the Data Center name and the value is the IANA time zone name. The Key and Value are separated by a comma.",
                Example = "--DefaultDCTimeZone \"DC1, America/Chicago\""
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>("DefaultNodeTimeZone")
            {
                Optional = true,
                AllowMultiple = true,
                Description = "Key-Value pair where the key is the Node's IP Address and the value is the IANA time zone name. The Key and Value are separated by a comma.",
                Example = "--DefaultNodeTimeZone \"10.0.0.1, America/Chicago\""
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>("ClusterName")
            {
                DefaultValue = ParserSettings.ClusterName,
                Optional = true,               
                Description = "The name of the cluster"                
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<int>("ClusterHashCode")
            {                
                Optional = true,
                Description = "The cluster's hash code."
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<bool>("LogEventMemoryMapping")
            {
                Optional = true,
                Description = "If true the Log Events are mapped into virtual memory, if false Log Events are kept in physical memory (better performance but can OOM)."
            });
            
            this._cmdLineParser.Arguments.Add(new ValueArgument<string>("Profile")
            {
                DefaultValue = ParserSettings.Profile,
                Optional = true,
                Description = string.Format("The Profile used to parse, transform, and analysis the data. Profile Names: \"{0}\"", string.Join(", ", Profiles.Names()))
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<TimeSpan>("LogAggrPeriod")
            {
                DefaultValue = ParserSettings.LogAggregationPeriod,
                Optional = true,
                Description = "Log Aggregation Period in HH:MM:SS format"
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<bool>("ExcelPackageCache")
            {
                DefaultValue = DSEDiagtnosticToExcel.LibrarySettings.ExcelPackageCache,
                Optional = true,
                Description = "Enables/Disables the Excel Package caching. If disabled each time a worksheet is generated the workbook is saved and reloaded (Excel Package is deleted and recreated)."
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<bool>("IncludeAttributeSymbols")
            {
                DefaultValue = ParserSettings.EnableAttrSymbols,
                Optional = true,
                Description = "Enables/Disables the generation of attribute symbols on the end of names (e.g., node)."
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<bool>("ExcelWorkSheetSave")
            {
                DefaultValue = DSEDiagtnosticToExcel.LibrarySettings.ExcelSaveWorkSheet,
                Optional = true,
                Description = "Enables/Disables the saving of a workbook each time a worksheet is added/modified. If ExcelPackageCache is disabled this is always enabled."
            });
            
            this._cmdLineParser.Arguments.Add(new ValueArgument<string>("IgnoreLogTagEvent")
            {
                Optional = true,
                AllowMultiple = true,
                Description = "A Log Event Tag Id that will cause that associated log event to be ignored during parsing. If an integral value (i.e., 10, no decimal) and a session event, all items in that session are ignored. Multiple arguments can be defined.",
                Example = @"10 -- all session items associated to this id tag (e.g., 10.0, 10.1, 10.2, etc.); 10.1 -- only the item associated to the id of 10.1."
            });

            this._cmdLineParser.Arguments.Add(new SwitchArgument("OldExcelWS", false)
            {
                Description = "Old Excel Worksheet Format Used"
            });

            this._cmdLineParser.Arguments.Add(new SwitchArgument("LogIgnoreParsingErrors", ParserSettings.LogIgnoreParsingErrors)
            {
                Description = "If defined, all parsing errors related to logs will be ignored. Warning: If defined unexpected results may occur including abnormal termination of the application, exceptions, and/or invalid/missing log event generation."
            });

            this._cmdLineParser.Arguments.Add(new SwitchArgument('B', "Batch", false)
            {
                Description = "Enables Batch Mode, which basically disables prompts and enables Exception Tracing"
            });

            this._cmdLineParser.Arguments.Add(new SwitchArgument("TraceException", false)
            {
                Description = ParserSettings.TraceExceptions ? "Disables exception tracing" : "Enables exception tracing where any exceptions are logged to a separate unique log file for this run"
            });

            this._cmdLineParser.Arguments.Add(new SwitchArgument("Debug", false)
            {
                Description = "Debug Mode"
            });

            if(ParserSettings.DisableParallelProcessing)
                this._cmdLineParser.Arguments.Add(new SwitchArgument("EnableParallelProcessing", false)
                {
                    Description = "Enable Parallel Processing"
                });
            else
                this._cmdLineParser.Arguments.Add(new SwitchArgument("DisableParallelProcessing", false)
                {
                    Description = "Disable Parallel Processing"
                });

            this._cmdLineParser.Arguments.Add(new SwitchArgument("ShowDefaults", false)
            {
                Optional = true,
                Description = "Show Arguments plus Default Values"
            });

            this._cmdLineParser.Arguments.Add(new SwitchArgument('?', "help", false)
            {
                Optional = true,
                Description = "Show Arguments plus Default Values"
            });

            this._cmdLineParser.Arguments.Add(new SwitchArgument("ShowVersion", false)
            {
                Optional = true,
                Description = "Shows the Application's Version Information"
            });
        }

        public bool ParseSetArguments(string[] args)
        {
            bool additionalFilesForParsingClassInitial = true;
            bool onlyNodesInitial = true;
            bool diagnosticPathPresent = false;
           
            if (args.Any(i => i.Any(c => c == '"')))
            {
                var argList = new List<string>();

                foreach (var arg in args)
                {
                    if (arg.Contains("\""))
                    {
                        var argItems = arg.Split('"', ' ');
                        bool nullFnd = false;

                        foreach(var item in argItems)
                        {
                            if (string.IsNullOrEmpty(item))
                            {
                                if (nullFnd)
                                {
                                    argList.Add(string.Empty);
                                    nullFnd = false;
                                }
                                else
                                    nullFnd = true;
                                continue;
                            }

                            nullFnd = false;
                            argList.Add(item.Trim());
                        }
                    }
                    else
                        argList.Add(arg);
                }
                args = argList.ToArray();
            }

            if(args.Any(a => a == "-?" || a == "--ShowDefaults" || a == "--Help" || a == "--help"))
            {
                this.ShowDefaults();
                return false;
            }

            if (args.Any(a => a == "--ShowVersion"))
            {
                this.ShowVersion();
                return false;
            }

            this._cmdLineParser.ParseCommandLine(args);

            if (!this._cmdLineParser.ParsingSucceeded)
            {                
                this._cmdLineParser.ShowUsage();
                return false;
            }

            var results = this._cmdLineParser.Arguments.Where(a => a.Parsed);

            foreach (var item in results)
            {
                switch (item.LongName)
                {
                    case "DiagnosticPath":                        
                        ParserSettings.DiagnosticPath = ParserSettings.MakeDirectoryPath(((DirectoryArgument)item).Value.ToString(), ParserSettings.DiagnosticPath);
                        diagnosticPathPresent = true;
                        break;
                    case "ExcelFilePath":
                        ParserSettings.ExcelFilePath = ParserSettings.MakeFilePath(((FileArgument) item).Value.ToString(), ParserSettings.ExcelFilePath?.ParentDirectoryPath, ParserSettings.ExcelFilePath);
                        break;
                    case "ExcelFileTemplatePath":
                        ParserSettings.ExcelFileTemplatePath = ((FileArgument)item).Value == null
                                                                    ? null
                                                                    : ParserSettings.MakeFilePath(((FileArgument)item).Value.ToString(), ParserSettings.ExcelFileTemplatePath?.ParentDirectoryPath, ParserSettings.ExcelFileTemplatePath);
                        break;
                    case "DiagFolderStruct":
                        ParserSettings.DiagFolderStruct = ((ValueArgument<ParserSettings.DiagFolderStructOptions>) item).Value;
                        break;
                    case "DiagCaptureTime":
                        ParserSettings.NodeToolCaptureTimestamp = ((ValueArgument<DateTimeOffset?>)item).Value;
                        break;
                    case "LogRangeBasedOnPrevHrs":
                        ParserSettings.OnlyIncludeXHrsofLogsFromDiagCaptureTime = ((ValueArgument<int>)item).Value;
                        ParserSettings.LogRestrictedTimeRange = null;
                        break;
                    case "LogTimeRange":
                        {
                            var value = ((ValueArgument<string>)item).Value;

                            if (string.IsNullOrEmpty(value))
                            {
                                ParserSettings.LogRestrictedTimeRange = null;
                            }
                            else
                            {
                                var matchRegEx = new System.Text.RegularExpressions.Regex(Properties.Settings.Default.CLParserLogTimeRangeRegEx, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                                var match = matchRegEx.Match(value);

                                if (match.Success)
                                {
                                    try
                                    {
                                        var startTR = match.Groups["STARTTS"].Value;
                                        var startTZ = match.Groups["STARTTZ"].Value;
                                        var endTR = match.Groups["ENDTS"].Value;
                                        var endTZ = match.Groups["ENDTZ"].Value;
                                        DateTimeOffset startDTOS;
                                        DateTimeOffset endDTOS;

                                        if(string.IsNullOrEmpty(startTZ))
                                        {
                                            if(string.IsNullOrEmpty(startTR))
                                            {
                                                startDTOS = DateTimeOffset.MinValue;
                                            }
                                            else
                                            {
                                                startDTOS = DateTimeOffset.Parse(startTR);
                                            }
                                        }
                                        else
                                        {
                                            DateTime startDT = string.IsNullOrEmpty(startTR) ? DateTime.MinValue : DateTime.Parse(startTR);
                                            startDTOS = DetermineDateTimeFromTZ(startDT, startTZ);
                                        }

                                        if (string.IsNullOrEmpty(endTZ))
                                        {
                                            if (string.IsNullOrEmpty(endTR))
                                            {
                                                endDTOS = DateTimeOffset.MaxValue;
                                            }
                                            else
                                            {
                                                endDTOS = DateTimeOffset.Parse(endTR);
                                            }
                                        }
                                        else
                                        {
                                            DateTime endDT = string.IsNullOrEmpty(endTR) ? DateTime.MaxValue : DateTime.Parse(endTR);
                                            endDTOS = DetermineDateTimeFromTZ(endDT, endTZ);
                                        }

                                        ParserSettings.LogRestrictedTimeRange = new DateTimeOffsetRange(startDTOS, endDTOS);
                                        ParserSettings.OnlyIncludeXHrsofLogsFromDiagCaptureTime = -1;
                                    }
                                    catch (System.Exception ex)
                                    {
                                        throw new CommandLineParser.Exceptions.CommandLineFormatException(string.Format("LogTimeRange's value received an exception of {0} with message of \"{1}\". Value given is \"{2}\"",
                                                                                                                            ex.GetType().Name,
                                                                                                                            ex.Message,
                                                                                                                            value));
                                    }
                                }
                                else
                                {
                                    throw new CommandLineParser.Exceptions.CommandLineFormatException(string.Format("LogTimeRange's value was not of the correct format. Value given is \"{0}\"", value));
                                }
                            }
                        }
                        break;
                    case "IgnoreKeySpaces":
                        ParserSettings.IgnoreKeySpaces = ParserSettings.CreateMergeList(((ValueArgument<string>)item).Value, ParserSettings.IgnoreKeySpaces);
                        break;
                    case "WarnWhenKSTblIsDetected":
                        ParserSettings.WarnWhenKSTblIsDetected = ParserSettings.CreateMergeList(((ValueArgument<string>)item).Value, ParserSettings.WarnWhenKSTblIsDetected);
                        break;
                    case "AlternativeLogFilePath":
                        {
                            if (additionalFilesForParsingClassInitial)
                            {
                                ParserSettings.AdditionalFilesForParsingClass.Clear();
                                additionalFilesForParsingClassInitial = false;
                            }

                            foreach (var filePath in ((ValueArgument<string>)item).Values)
                            {
                                var paths = filePath.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                                                    .Select(p => p.Last() == '"' ? p.Substring(0, p.Length - 1) + '\\' : p)
                                                    .Select(p => string.IsNullOrEmpty(System.IO.Path.GetExtension(p))
                                                                    ? (p.Last() == '\\' ? p : p + '\\') + Properties.Settings.Default.AppendFilePathForAddLogArgument
                                                                    : p);

                                ParserSettings.AdditionalFilesForParsingClass.AddRange(paths.Select(p => new KeyValuePair<string, IFilePath>("LogFile", PathUtils.BuildFilePath(p))));
                            }
                        }
                        break;
                    case "AlternativeDDLFilePath":
                        {
                            if (additionalFilesForParsingClassInitial)
                            {
                                ParserSettings.AdditionalFilesForParsingClass.Clear();
                                additionalFilesForParsingClassInitial = false;
                            }

                            foreach (var filePath in ((ValueArgument<string>)item).Values)
                            {
                                var paths = filePath.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);                                

                                ParserSettings.AdditionalFilesForParsingClass.AddRange(paths.Select(p => new KeyValuePair<string, IFilePath>("CQLFile", PathUtils.BuildFilePath(p))));
                            }
                        }
                        break;
                    case "AlternativeCompressionFilePath":
                        {
                            if (additionalFilesForParsingClassInitial)
                            {
                                ParserSettings.AdditionalFilesForParsingClass.Clear();
                                additionalFilesForParsingClassInitial = false;
                            }

                            foreach (var filePath in ((ValueArgument<string>)item).Values)
                            {
                                var paths = filePath.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                                
                                ParserSettings.AdditionalFilesForParsingClass.AddRange(paths.Select(p => new KeyValuePair<string, IFilePath>("ZipFile", PathUtils.BuildFilePath(p))));
                            }
                        }
                        break;
                    case "AlternativeFilePath":
                        {
                            if (additionalFilesForParsingClassInitial)
                            {
                                ParserSettings.AdditionalFilesForParsingClass.Clear();
                                additionalFilesForParsingClassInitial = false;
                            }

                            ParserSettings.AdditionalFilesForParsingClass.AddRange(((ValueArgument<string>)item).Values
                                                                                    .Select(i =>
                                                                                    {
                                                                                        var clsPath = i.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

                                                                                        return new KeyValuePair<string, IFilePath>(clsPath[0].Trim(), PathUtils.BuildFilePath(clsPath[1].Trim()));
                                                                                    }));
                        }
                        break;
                    case "OnlyNodes":
                        {
                            var nodes = new List<string>();

                            foreach(var node in ((ValueArgument<string>)item).Values)
                            {
                                nodes.AddRange(node.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries));
                            }

                            if (onlyNodesInitial)
                            {
                                ParserSettings.OnlyNodes.Clear();
                                onlyNodesInitial = false;
                            }

                            ParserSettings.OnlyNodes.AddRange(nodes);
                        }
                        break;
                    case "DisableSysDSEKSLoading":
                        {
                            if ( ((SwitchArgument) item).Value)
                            {
                                DSEDiagnosticFileParser.cql_ddl.DisabledSystemDSEDefaultKeyspacesInitialLoading();
                            }
                        }
                        break;
                    case "ProcessFileMappingPath":
                        ParserSettings.ProcessFileMappingValue = ((FileArgument)item).Value.ToString();
                        break;
                    case "LogFileGapAnalysis":
                        ParserSettings.LogFileInfoAnalysisGapTriggerInMins = ((ValueArgument<int>)item).Value;
                        break;
                    case "LogFileContinousAnalysis":
                        ParserSettings.LogFileInfoAnalysisContinousEventInDays = ((ValueArgument<int>)item).Value;
                        break;
                    case "AppendToExcelWorkSheet":
                        ParserSettings.AppendToWorkSheet = ((SwitchArgument)item).Value;
                        break;
                    case "LogLinePatternLayout":
                        {
                            var value = ((ValueArgument<string>)item).Value;

                            if(!string.IsNullOrEmpty(value))
                            {
                               ParserSettings.Log4NetConversionPattern = value;
                            }
                        }
                        break;
                    case "DSEVersion":
                        {
                            var value = ((ValueArgument<string>)item).Value;

                            if (string.IsNullOrEmpty(value))
                            {
                                ParserSettings.DSEVersion = null;
                            }
                            else
                            {
                                ParserSettings.DSEVersion = new Version(value);
                            }
                        }
                        break;
                    case "DefaultClusterTimeZone":
                        {
                            var value = ((ValueArgument<string>)item).Value;

                            if(!string.IsNullOrEmpty(value) 
                                    && Common.TimeZones.Find(value, ZoneNameTypes.IANATZName) == null)
                            {
                                throw new CommandLineParser.Exceptions.CommandLineFormatException(string.Format("DefaultClusterTimeZone IANA name was not found. Value given is \"{0}\"", value));
                            }

                            ParserSettings.DefaultClusterTZ = value;
                            break;
                        }
                    case "ClusterName":
                        {
                            var value = ((ValueArgument<string>)item).Value;
                            ParserSettings.ClusterName = value;
                            break;
                        }
                    case "ClusterHashCode":
                        {
                            var value = ((ValueArgument<int>)item).Value;
                            ParserSettings.ClusterHashCode = value;
                            break;
                        }
                    case "DefaultDCTimeZone":
                        {
                           
                            var items = ((ValueArgument<string>)item).Values
                                                                        .Select(i =>
                                                                        {
                                                                            var dctz = i.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

                                                                            return new DSEDiagnosticLibrary.DefaultAssocItemToTimeZone() { Item = dctz[0].Trim(), IANATZName = dctz[1].Trim() };
                                                                        });

                            foreach(var kvp in items)
                            {
                                if (!string.IsNullOrEmpty(kvp.IANATZName)
                                    && Common.TimeZones.Find(kvp.IANATZName, ZoneNameTypes.IANATZName) == null)
                                {
                                    throw new CommandLineParser.Exceptions.CommandLineFormatException(string.Format("DefaultDCTimeZone IANA name was not found for DC \"{0}\" with IANA name \"{1}\"",
                                                                                                                        kvp.Item, kvp.IANATZName));
                                }
                            }

                            ParserSettings.DCDefaultTimeZones = items.ToArray();
                        }
                        break;
                    case "DefaultNodeTimeZone":
                        {

                            var items = ((ValueArgument<string>)item).Values
                                                                        .Select(i =>
                                                                        {
                                                                            var nodetz = i.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

                                                                            return new DSEDiagnosticLibrary.DefaultAssocItemToTimeZone() { Item = nodetz[0].Trim(), IANATZName = nodetz[1].Trim() };
                                                                        });

                            foreach (var kvp in items)
                            {
                                if (!string.IsNullOrEmpty(kvp.IANATZName)
                                    && Common.TimeZones.Find(kvp.IANATZName, ZoneNameTypes.IANATZName) == null)
                                {
                                    throw new CommandLineParser.Exceptions.CommandLineFormatException(string.Format("DefaultNodeTimeZone IANA name was not found for Node \"{0}\" with IANA name \"{1}\"",
                                                                                                                        kvp.Item, kvp.IANATZName));
                                }
                            }

                            ParserSettings.NodeDefaultTimeZones = items.ToArray();
                        }
                        break;
                    case "LogEventMemoryMapping":
                        {                            
                            ParserSettings.LogEventsAreMemoryMapped = ((ValueArgument<bool>)item).Value;                            
                        }
                        break;                    
                    case "Profile":
                        {
                            var argValue = ((ValueArgument<string>)item).Value;

                            if(argValue == "?")
                            {
                                Console.WriteLine();
                                Console.WriteLine("Profiles are \"{0}\". Default is \"{1}\"", string.Join(", ", Profiles.Names()), Profiles.CurrentProfile?.ProfileName);
                                return false;
                            }

                            var profile = Profiles.SetProfile(argValue, false);

                            if(profile == null)
                            {
                                throw new CommandLineParser.Exceptions.CommandLineArgumentException(string.Format("Profile name \"{0}\" was not defined.",
                                                                                                                    argValue),
                                                                                                    "Profile");
                            }

                            DSEDiagnosticFileParser.LibrarySettings.DefaultLogLevelHandling = DSEDiagnosticLibrary.LibrarySettings.ParseEnum<DSEDiagnosticFileParser.file_cassandra_log4net.DefaultLogLevelHandlers>(profile.DefaultLogLevelHandling);
                            DSEDiagnosticFileParser.LibrarySettings.Log4NetParser = DSEDiagnosticFileParser.LibrarySettings.ReadJsonFileIntoObject<DSEDiagnosticFileParser.CLogTypeParser>(profile.Log4NetParser);
                            DSEDiagnosticFileParser.LibrarySettings.DebugLogProcessing = DSEDiagnosticLibrary.LibrarySettings.ParseEnum<DSEDiagnosticFileParser.file_cassandra_log4net.DebugLogProcessingTypes>(profile.DebugLogProcessingTypes);

                            if (!results.Any(i => i.LongName == "ProcessFileMappingPath"))
                            {
                                DSEDiagnosticFileParser.LibrarySettings.ProcessFileMappingValue = profile.ProcessFileMappings;
                            }                            
                            if (!results.Any(i => i.LongName == "LogEventMemoryMapping"))
                            {
                                ParserSettings.LogEventsAreMemoryMapped = profile.EnableVirtualMemory;
                            }
                        }
                        break;
                    case "LogAggrPeriod":
                        {
                            var value = ((ValueArgument<TimeSpan>)item).Value;
                            ParserSettings.LogAggregationPeriod = value;
                        }
                        break;
                    case "IgnoreLogTagEvent":
                        {
                            var items = ((ValueArgument<string>)item).Values
                                                                        .SelectMany(i =>
                                                                        {
                                                                            return i.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                                                                        });

                            foreach (var logTagId in items)
                            {
                                int valueInt;
                                var hasDecimal = logTagId.Contains('.');

                                if(!hasDecimal && int.TryParse(logTagId, out valueInt))
                                {
                                    var lower = (decimal)valueInt;
                                    var upper = (decimal) valueInt + 1;
                                    var logEvent = DSEDiagnosticFileParser.LibrarySettings.Log4NetParser.Parsers.FirstOrDefault(i => i.TagId >= lower && i.TagId < upper);
                                    
                                    if(logEvent != null)
                                    {
                                        logEvent.IgnoreEvent = true;
                                    }
                                }
                                else
                                {
                                    try
                                    {
                                        var valueDec = decimal.Parse(logTagId);
                                        var logEvent = DSEDiagnosticFileParser.LibrarySettings.Log4NetParser.Parsers.FirstOrDefault(i => i.TagId == valueDec);

                                        if (logEvent != null)
                                        {
                                            logEvent.IgnoreEvent = true;
                                        }
                                    }
                                    catch (System.Exception ex)
                                    {
                                        throw new CommandLineParser.Exceptions.CommandLineArgumentException(string.Format("Log Event Tag Id \"{0}\" was not valid. Exception is \"{1}\": {2}",
                                                                                                                    logTagId, 
                                                                                                                    ex.GetType().Name,
                                                                                                                    ex.Message),
                                                                                                    "IgnoreLogTagEvent");
                                    }
                                    
                                }
                            }

                            ParserSettings.IgnoreLogParsingTagEvents = items;
                        }
                        break;
                    case "OldExcelWS":
                        ParserSettings.OldExcelWorksheets = ((SwitchArgument)item).Value;
                        break;
                    case "LogIgnoreParsingErrors":
                        ParserSettings.LogIgnoreParsingErrors = ((SwitchArgument)item).Value;
                        break;
                    case "Batch":
                        ParserSettings.BatchMode = ((SwitchArgument)item).Value;
                        break;
                    case "TraceException":
                        ParserSettings.TraceExceptions = !ParserSettings.TraceExceptions;
                        break;
                    case "Debug":
                        this.Debug = ((SwitchArgument)item).Value;
                        break;
                    case "DisableParallelProcessing":
                        if (((SwitchArgument)item).Value)
                        {
                            ParserSettings.DisableParallelProcessing = true;
                        }
                        break;
                    case "EnableParallelProcessing":
                        if (((SwitchArgument)item).Value)
                        {
                            ParserSettings.DisableParallelProcessing = false;
                        }
                        break;
                    case "ExcelPackageCache":
                        DSEDiagtnosticToExcel.LibrarySettings.ExcelPackageCache = ((ValueArgument<bool>)item).Value;
                        break;
                    case "IncludeAttributeSymbols":
                        ParserSettings.EnableAttrSymbols = ((ValueArgument<bool>)item).Value;
                        break;
                    case "ExcelWorkSheetSave":
                        DSEDiagtnosticToExcel.LibrarySettings.ExcelSaveWorkSheet = ((ValueArgument<bool>)item).Value;
                        break;
                    case "ShowDefaults":
                    case "help":
                        ShowDefaults();
                        return false;
                    case "ShowVersion":
                        ShowVersion();
                        return false;
                    default:

                        throw new ArgumentException(string.Format("Do not know how to map {0} ({1}) to a setting!",
                                                                    item.LongName,
                                                                    item.GetType().Name),
                                                                    item.LongName);
                }
            }

            if(!diagnosticPathPresent)
            {
                throw new CommandLineParser.Exceptions.MandatoryArgumentNotSetException("DiagnosticPath was not found and is required.", "DiagnosticPath");
            }

            return true;
        }

        public void ShowDefaults()
        {
            Console.WriteLine();
            Console.WriteLine("Arguments including Default Values:");
            Console.WriteLine();

            foreach (var cmdArg in this._cmdLineParser.Arguments)
            {
                var defaultValue = (cmdArg as IArgumentWithDefaultValue)?.DefaultValue;
                var example = cmdArg.Example?.Trim();
                var required = cmdArg.Optional ? string.Empty : " (Required)";
                var defaultValueFmt = string.Format(" [Default Value \"{0}\"{1}]",
                                                        defaultValue == null || (defaultValue is string && ((string)defaultValue) == string.Empty)
                                                            ? "<none>"
                                                            : defaultValue,
                                                        cmdArg.AllowMultiple ? ", Multiple Allowed" : string.Empty);

                if (cmdArg.ShortName.HasValue)
                {
                    Console.WriteLine("\t-{0}, --{1}{2}{3} {4}",
                                        cmdArg.ShortName.Value,
                                        cmdArg.LongName,
                                        cmdArg is SwitchArgument
                                            ? string.Empty
                                            : defaultValueFmt,
                                        required,
                                        cmdArg.Description);
                }
                else
                {
                    Console.WriteLine("\t--{0}{1}{2} {3}",
                                        cmdArg.LongName,
                                        cmdArg is SwitchArgument
                                            ? string.Empty
                                            : defaultValueFmt,
                                         required,
                                        cmdArg.Description);
                }

                if (!string.IsNullOrEmpty(example))
                {
                    Console.WriteLine("\t\tExample: {0}", example);
                }
            }

            Console.WriteLine();
            Console.WriteLine("Steps to process an OpsCenter diagnostic tarball:");
            Console.WriteLine("\t1) Decompress the tarball into a target folder (e.g., \"c:\\MyCustomerName\")");
            Console.WriteLine("\t\tOnce decompressed the OpsCenter's diagnostic folder should be placed in the targeted folder (e.g., \"c:\\MyCustomerName\\CustClusterName-diagnostics-2019_01_21_16_30_46_UTC\")");
            Console.WriteLine("\t2) Open the \"Heath Check Console\" command prompt or a DOS command prompt");
            Console.WriteLine("\t3) Run the Health Check Engine.");
            Console.WriteLine("\t\tTypical Example: DSEDiagnosticConsoleApplication.exe -D \"c:\\MyCustomerName\\CustClusterName-diagnostics-2019_01_21_16_30_46_UTC\" --Profile AllFilesLogs");
            Console.WriteLine("\t4) Once the application starts execution, observe the console screen for progress, warnings, errors, and exceptions");
            Console.WriteLine("\t\tIf there are any warnings or errors, please review the \"parsing error\" and \"not handled\" worksheets in the generated Excel workbook for the details.");
            Console.WriteLine("\t\tIf an exception occurs, please notify Richard Andersen");
            Console.WriteLine("\t5) Once completed, the generated Excel workbook is placed under the OptCenter diagnostic folder (e.g., \"c:\\MyCustomerName\\CustClusterName-diagnostics-2019_01_21_16_30_46_UTC\\CustClusterName-2018-09-25-10-11-34-AllNoFlushComp-190123To0116.xlsm\")");
            Console.WriteLine("\t6) Open the generated workbook (MS-Excel version 16 and higher is required) and click on the \"Refresh\" button located in the \"Refresh\" worksheet to update the pivot-tables and caches in the workbook");
            Console.WriteLine("\t7) Save the workbook so that the updated pivot-tables have the current data");

            Console.WriteLine();
            Console.WriteLine("Notes: If the customer provided rolled/archived logs (either system or debug), they can be placed in an folder named \"AdditionalLogs\" under the OptCenter's diagnostic folder (e.g., \"c:\\MyCustomerName\\CustClusterName-diagnostics-2019_01_21_16_30_46_UTC\\AdditionalLogs\")");
            Console.WriteLine("\tThese additional logs need to be defined in a certain folder structure. Each set of logs need to be placed in a separate folder where that folder's name is the IP address of the node associated with the logs (e.g., \"c:\\MyCustomerName\\CustClusterName-diagnostics-2019_01_21_16_30_46_UTC\\AdditionalLogs\\10.0.0.1\\systemlog.3.228.zip\")");
            Console.WriteLine("\tIf there are any compressed files (handles any type of compressed file type) under the OptCenter's diagnostic fold, the engine will automatically decompress these files.");
            Console.WriteLine("\tThe engine can also consume non-OpsCenter diagnostic files, please contact Richard Andersen on details");

        }

        public void ShowVersion()
        {
            var appLogFile = Logger.Instance.GetSetEnvVarLoggerFile(false);

            if (string.IsNullOrEmpty(appLogFile))
                appLogFile = "N/A";
            else
                appLogFile = System.IO.Directory.GetParent(appLogFile).FullName;

            Console.WriteLine();
            Console.WriteLine("             Application: {0}", Common.Functions.Instance.ApplicationName);
            Console.WriteLine("                 Version: {0}", Common.Functions.Instance.ApplicationVersion);
            Console.WriteLine("               Timestamp: {0:yyyy-MM-dd HH\\:mm\\:ss}", System.IO.File.GetLastWriteTime(Common.Functions.Instance.ExeAssembly.Location));
            Console.WriteLine("     Excel Template File: {0}", ParserSettings.ExcelFileTemplatePath?.PathResolved ?? "N/A");
            Console.WriteLine("Excel Template Timestamp: {0:yyyy-MM-dd HH\\:mm\\:ss}", ParserSettings.ExcelFileTemplatePath?.GetLastWriteTime());
            Console.WriteLine("        Application Path: {0}", Common.Functions.Instance.ApplicationRunTimeDir);
            Console.WriteLine("    Application Log Path: {0}", appLogFile);
            Console.WriteLine();
        }

        public bool Debug
        {
            get;
            set;
        }

        public DateTimeOffset DetermineDateTimeFromTZ(DateTime dateTime, string tzName)
        {
            if(string.IsNullOrEmpty(tzName))
            {
                if (dateTime == DateTime.MinValue) return DateTimeOffset.MinValue;
                if (dateTime == DateTime.MaxValue) return DateTimeOffset.MaxValue;

                return dateTime.ConvertToOffSet();
            }

            return Common.TimeZones.ConvertToOffset(dateTime, tzName, ZoneNameTypes.IANATZName);
        }
    }
}
