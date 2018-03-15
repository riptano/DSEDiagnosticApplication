﻿using System;
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
            Profiles.SetProfile();

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
                Example = @"c:\additionalZip\system.log.1.zip, c:\additionalZip\system.2.zip"
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>('A', "AlternativeFilePath")
            {
                Optional = true,
                AllowMultiple = true,
                DefaultValue = string.Join(", ", ParserSettings.AdditionalFilesForParsingClass.Where(a => !(a.Key == "LogFile" || a.Key == "file_cassandra_log4net_ReadTimeRange" || a.Key == "file_cassandra_log4net" || a.Key == "CQLFile" || a.Key == "cql_ddl" || a.Key == "ZipFile" || a.Key == "file_unzip")).Select(s => s.Key + ", " + s.Value)),
                Description = "Key-Value pair where the key is a File Parsing Class (e.g., file_cassandra_log4net_ReadTimeRange, cql_ddl, file_unzip)  or Category Type (e.g., LogFile, CQLFile, ZipFile) and the value is a file path. The file path can contain wild cards. The Key and Value are separated by a comma.",
                Example = @"ZipFile, c:\additionalfiles\*.tar.gz"
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>("OnlyNodes")
            {
                Optional = true,
                AllowMultiple = true,
                DefaultValue = string.Join(", ", ParserSettings.OnlyNodes),
                Description = "Only process these nodes. This can be an IP Address separated by a comma or multiple argument commands",
                Example = @"10.0.0.1, 10.0.0.2"
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
                Description = "When the OpsCenter Diagnostic TarBall was created or when the \"nodetool\" statical (e.g., cfstats) capture occurred. Null will use the Date embedded in the OpsCenter tar ball directory. Syntax should be that of a date time offset (+|-HH[:MM] and no IANA name accepted). If time zone offset not given, current machine's offset is used."
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
                Description = "Only import log entries from/to this date/time range. Empty string will parse all entries. Syntax: \"<FromDateTimeOnly> [+|-HH[:MM]]|[IANA TimeZone Name]\", \", <ToDateTimeOnly> [+|-HH[:MM]]|[IANA TimeZone Name]\", or \"<FromDateTime> [+|-HH[:MM]]|[IANA TimeZone Name],<ToDateTime> [+|-HH[:MM]]|[IANA TimeZone Name]\". If [IANA TimeZone Name] or [+|-HH[:MM]] (timezone offset) is not given the local machine's TZ is used. Ignored if LogRangeBasedOnPrevHrs is defined.",
                Example = "\"2018-02-01 00:00:00 +00:00, 2018-02-21 00:00:00 UDT\" only logs between Feb01 to Feb21 2018 UDT -or- \",2018-02-21 00:00:00 UDT\" All logs up to Feb21 2018 UDT -or- \"2018-02-01 00:00:00 UDT\" only logs from Feb01 2018 UDT to last log entries"
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
                DefaultValue = DSEDiagnosticFileParser.LibrarySettings.Log4NetConversionPattern,
                Optional = true,
                Description = "The definition that defines the format of a Log Line. This definition must follow the Apache Log PatternLayout configuration."
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
                DefaultValue = DSEDiagnosticLibrary.LibrarySettings.DefaultClusterTZ,
                Description = "An IANA TimeZone name used as the default time zone for all data centers/nodes where the time zone could not be determined.",
                Example = @"America/Chicago"
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>("DefaultDCTimeZone")
            {
                Optional = true,
                AllowMultiple = true,
                Description = "Key-Value pair where the key is the Data Center name and the value is the IANA time zone name. The Key and Value are separated by a comma.",
                Example = @"DC1, America/Chicago"
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>("DefaultNodeTimeZone")
            {
                Optional = true,
                AllowMultiple = true,
                Description = "Key-Value pair where the key is the Node's IP Address and the value is the IANA time zone name. The Key and Value are separated by a comma.",
                Example = @"10.0.0.1, America/Chicago"
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>("ClusterName")
            {
                Optional = true,               
                Description = "The name of the cluster"                
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<int>("ClusterHashCode")
            {
                Optional = true,
                Description = "The cluster's hash code."
            });

            this._cmdLineParser.Arguments.Add(new SwitchArgument("DisableLogEventMemoryMapping", false)
            {
                Optional = true,
                Description = "If defined the Log Events are NOT mapped into virtual memory (uses physical memory with better performance but can OOM). The default is to map events into virtual memory resulting in smaller physical memory utilizations on the cost of performance."
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<string>("Profile")
            {
                DefaultValue = Profiles.CurrentProfile?.ProfileName,
                Optional = true,
                Description = "The Profile used to parse, transform, and analysis the data."
            });

            this._cmdLineParser.Arguments.Add(new ValueArgument<TimeSpan>("LogAggrPeriod")
            {
                DefaultValue = ParserSettings.LogAggregationPeriod,
                Optional = true,
                Description = "Log Aggregation Period in DD:HH:MM format"
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
            bool additionalFilesForParsingClassInitial = true;
            bool onlyNodesInitial = true;

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

                                        ParserSettings.LogTimeRange = new DateTimeOffsetRange(startDTOS, endDTOS);
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

                            if(additionalFilesForParsingClassInitial)
                            {
                                ParserSettings.AdditionalFilesForParsingClass.Clear();
                                additionalFilesForParsingClassInitial = false;
                            }

                            ParserSettings.AdditionalFilesForParsingClass.AddRange(paths.Select(p => new KeyValuePair<string, IFilePath>("LogFile", PathUtils.BuildFilePath(p))));
                        }
                        break;
                    case "AlternativeDDLFilePath":
                        {
                            var paths = ((ValueArgument<string>)item).Value.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

                            if(additionalFilesForParsingClassInitial)
                            {
                                ParserSettings.AdditionalFilesForParsingClass.Clear();
                                additionalFilesForParsingClassInitial = false;
                            }

                            ParserSettings.AdditionalFilesForParsingClass.AddRange(paths.Select(p => new KeyValuePair<string, IFilePath>("CQLFile", PathUtils.BuildFilePath(p))));
                        }
                        break;
                    case "AlternativeCompressionFilePath":
                        {
                            var paths = ((ValueArgument<string>)item).Value.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

                            if (additionalFilesForParsingClassInitial)
                            {
                                ParserSettings.AdditionalFilesForParsingClass.Clear();
                                additionalFilesForParsingClassInitial = false;
                            }

                            ParserSettings.AdditionalFilesForParsingClass.AddRange(paths.Select(p => new KeyValuePair<string, IFilePath>("ZipFile", PathUtils.BuildFilePath(p))));
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
                            var nodes = ((ValueArgument<string>)item).Value.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

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
                                DSEDiagnosticFileParser.LibrarySettings.Log4NetConversionPattern = value;
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

                            DSEDiagnosticLibrary.LibrarySettings.DefaultClusterTZ = value;
                            break;
                        }
                    case "ClusterName":
                        {
                            var value = ((ValueArgument<string>)item).Value;
                            DSEDiagnosticConsoleApplication.ParserSettings.ClusterName = value;
                            break;
                        }
                    case "ClusterHashCode":
                        {
                            var value = ((ValueArgument<int>)item).Value;
                            DSEDiagnosticConsoleApplication.ParserSettings.ClusterHashCode = value;
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

                            DSEDiagnosticLibrary.DataCenter.DefaultTimeZones = items.ToArray();
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

                            DSEDiagnosticLibrary.Node.DefaultTimeZones = items.ToArray();
                        }
                        break;
                    case "DisableLogEventMemoryMapping":
                        {
                            if (((SwitchArgument)item).Value)
                            {
                                DSEDiagnosticLibrary.LibrarySettings.LogEventsAreMemoryMapped = false;
                            }
                        }
                        break;
                    case "Profile":
                        {
                            var profile = Profiles.SetProfile(((ValueArgument<string>)item).Value, false);

                            if(profile == null)
                            {
                                throw new ArgumentException(string.Format("Profile name \"{0}\" was not defined.",
                                                                    ((ValueArgument<string>)item).Value));
                            }

                            DSEDiagnosticFileParser.LibrarySettings.DefaultLogLevelHandling = DSEDiagnosticLibrary.LibrarySettings.ParseEnum<DSEDiagnosticFileParser.file_cassandra_log4net.DefaultLogLevelHandlers>(profile.DefaultLogLevelHandling);
                            DSEDiagnosticFileParser.LibrarySettings.Log4NetParser = DSEDiagnosticFileParser.LibrarySettings.ReadJsonFileIntoObject<DSEDiagnosticFileParser.CLogTypeParser>(profile.Log4NetParser);

                            if (!results.Any(i => i.LongName == "ProcessFileMappingPath"))
                            {
                                DSEDiagnosticFileParser.LibrarySettings.ProcessFileMappingValue = profile.ProcessFileMappings;
                            }                            
                            if (!results.Any(i => i.LongName == "DisableLogEventMemoryMapping"))
                            {
                                DSEDiagnosticLibrary.LibrarySettings.LogEventsAreMemoryMapped = profile.EnableVirtualMemory;
                            }
                        }
                        break;
                    case "LogAggrPeriod":
                        {
                            var value = ((ValueArgument<TimeSpan>)item).Value;
                            ParserSettings.LogAggregationPeriod = value;
                        }
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
