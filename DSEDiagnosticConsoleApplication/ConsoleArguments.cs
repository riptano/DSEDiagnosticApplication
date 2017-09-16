using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Common.Path;
using CommandLineParser.Arguments;

namespace DSEDiagnosticConsoleApplication
{
    internal sealed class ConsoleArguments
    {
        private CommandLineParser.CommandLineParser _cmdLineParser = new CommandLineParser.CommandLineParser();

        public ConsoleArguments()
        {
            this._cmdLineParser.Arguments.Add(new DirectoryArgument('D', "DiagnosticPath")
            {
                Optional = false,
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
                DefaultValue = string.Join(", ", ParserSettings.AdditionalFilesForParsingClass.Where(a => a.Key == "LogFile" || a.Key == "file_cassandra_log4net_ReadTimeRange" || a.Key == "file_cassandra_log4net").Select(s => s.Value)),
                Description = "A list of file paths separated by comma that will be included in log parsing, if enabled. This can include wild card patterns.",
                Example = @"c:\additionallogs\system*.log, c:\additional logs\debug*.log"
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>('C', "AlternativeDDLFilePath")
            {
                Optional = true,
                DefaultValue = string.Join(", ", ParserSettings.AdditionalFilesForParsingClass.Where(a => a.Key == "CQLFile" || a.Key == "cql_ddl").Select(s => s.Value)),
                Description = "A list of file paths separated by comma that will be included in DLL (CQL) parsing, if enabled. This can include wild card patterns.",
                Example = @"c:\additionalDDL\describe_schema, c:\additional DDL\describe.cql"
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>('Z', "AlternativeCompressionFilePath")
            {
                Optional = true,
                DefaultValue = string.Join(", ", ParserSettings.AdditionalFilesForParsingClass.Where(a => a.Key == "ZipFile" || a.Key == "file_unzip").Select(s => s.Value)),
                Description = "A list of file paths separated by comma that will be included in the decompress process, if enabled. This can include wild card patterns.",
                Example = @"c:\additionalDDL\describe_schema, c:\additional DDL\describe.cql"
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>('A', "AlternativeFilePath")
            {
                Optional = true,
                AllowMultiple = true,
                DefaultValue = string.Join(", ", ParserSettings.AdditionalFilesForParsingClass.Where(a => !(a.Key == "LogFile" || a.Key == "file_cassandra_log4net_ReadTimeRange" || a.Key == "file_cassandra_log4net" || a.Key == "CQLFile" || a.Key == "cql_ddl" || a.Key == "ZipFile" || a.Key == "file_unzip")).Select(s => s.Key + ", " + s.Value)),
                Description = "Key-Value pair where the key is a File Parsing Class (e.g., file_cassandra_log4net_ReadTimeRange, cql_ddl, file_unzip)  or Category Type (e.g., LogFile, CQLFile, ZipFile) and the value is a file path. The file path can contain wild cards. The Key and Value are separated by a comma.",
                Example = @"ZipFile, c:\additionalfiles\*.tar.gz"
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<ParserSettings.DiagFolderStructOptions>('O', "DiagFolderStruct")
            {
                DefaultValue = ParserSettings.DiagFolderStructOptions.OpsCtrDiagStruct,
                Optional = true,
                Description = "Structure of the folders and file names used to determine the context of each file."
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<DateTimeOffset?>('T', "DiagCaptureTime")
            {
                DefaultValue = ParserSettings.NodeToolCaptureTimestamp,
                Optional = true,
                Description = "This machine's local Date/Time when either the OpsCenter Diagnostic TarBall was created or when the \"nodetool\" statical (e.g., cfstats) capture occurred. Null will use the Date embedded in the OpsCenter tar ball directory."
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<int>("LogRangeBasedOnPrevHrs")
            {
                Optional = true,
                DefaultValue = ParserSettings.OnlyIncludeXHrsofLogsFromDiagCaptureTime,
                Description = "Only import log entries based on the previous <X> hours from DiagCaptureTime. only valid if DiagCaptureTime is defined or is a OpsCtrDiagStruct. -1 disables this option"
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>('R', "LogTimeRange")
            {
                Optional = true,
                DefaultValue = ParserSettings.LogTimeRange == null ? null : string.Format("{0}, {1}", ParserSettings.LogTimeRange.Min, ParserSettings.LogTimeRange.Max),
                Description = "Only import log entries from/to this date/time range. Empty string will parse all entries. Syntax: \"<FromDateTimeOnly> [IANA TimeZone Name]\", \", <ToDateTimeOnly> [IANA TimeZone Name]\", or \"<FromDateTime> [IANA TimeZone Name],<ToDateTime> [IANA TimeZone Name]\". If [IANA TimeZone Name] is not given the local machine's TZ is used. Ignored if LogRangeBasedOnPrevHrs is defined."
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
                DefaultValue = null,
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

            this._cmdLineParser.Arguments.Add(new SwitchArgument("Debug", false)
            {
                Description = "Debug Mode"
            });

            this._cmdLineParser.Arguments.Add(new SwitchArgument("DisableParallelProcessing", false)
            {
                Description = "Disable Parallel Processing"
            });
        }

        public bool ParseSetArguments(string[] args)
        {
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
                        break;
                    case "LogTimeRange":
                        {
                            var value = ((ValueArgument<string>)item).Value;

                            if (string.IsNullOrEmpty(value))
                            {
                                ParserSettings.LogTimeRange = null;
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
                                        DateTime startDT = string.IsNullOrEmpty(startTR) ? DateTime.MinValue : DateTime.Parse(startTR);
                                        DateTime endDT = string.IsNullOrEmpty(endTR) ? DateTime.MaxValue : DateTime.Parse(endTR);

                                        ParserSettings.LogTimeRange = new DateTimeOffsetRange(string.IsNullOrEmpty(startTZ)
                                                                                                    ? startDT == DateTime.MinValue ? DateTimeOffset.MinValue : startDT.ConvertToOffSet()
                                                                                                    : startDT.ConvertToOffSet(startTZ),
                                                                                                string.IsNullOrEmpty(endTZ)
                                                                                                    ? endDT == DateTime.MaxValue ? DateTimeOffset.MaxValue : endDT.ConvertToOffSet()
                                                                                                    : endDT.ConvertToOffSet(endTZ));
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
                            var paths = ((ValueArgument<string>)item).Value.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

                            ParserSettings.AdditionalFilesForParsingClass.AddRange(paths.Select(p => new KeyValuePair<string, IFilePath>("LogFile", PathUtils.BuildFilePath(p))));
                        }
                        break;
                    case "AlternativeDDLFilePath":
                        {
                            var paths = ((ValueArgument<string>)item).Value.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

                            ParserSettings.AdditionalFilesForParsingClass.AddRange(paths.Select(p => new KeyValuePair<string, IFilePath>("CQLFile", PathUtils.BuildFilePath(p))));
                        }
                        break;
                    case "AlternativeCompressionFilePath":
                        {
                            var paths = ((ValueArgument<string>)item).Value.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

                            ParserSettings.AdditionalFilesForParsingClass.AddRange(paths.Select(p => new KeyValuePair<string, IFilePath>("ZipFile", PathUtils.BuildFilePath(p))));
                        }
                        break;
                    case "AlternativeFilePath":
                        {
                            ParserSettings.AdditionalFilesForParsingClass.AddRange(((ValueArgument<string>)item).Values
                                                                                    .Select(i =>
                                                                                    {
                                                                                        var clsPath = i.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

                                                                                        return new KeyValuePair<string, IFilePath>(clsPath[0].Trim(), PathUtils.BuildFilePath(clsPath[1].Trim()));
                                                                                    }));
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
                    case "Debug":
                        this.Debug = ((SwitchArgument)item).Value;
                        break;
                    case "DisableParallelProcessing":
                        ParserSettings.DisableParallelProcessing = ((SwitchArgument)item).Value;
                        break;
                    default:

                        throw new ArgumentException(string.Format("Do not know how to map {0} ({1}) to a setting!",
                                                                    item.LongName,
                                                                    item.GetType().Name),
                                                                    item.LongName);
                }
            }

            return true;
        }

        public void ShowUsage()
        {
            this._cmdLineParser.ShowUsage();
        }

        public bool Debug
        {
            get;
            set;
        }
    }
}
