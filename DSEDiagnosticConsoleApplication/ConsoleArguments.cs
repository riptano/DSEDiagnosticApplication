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
        public ConsoleArguments()
        { }

        [SwitchArgument("Debug", false, Description = "Debug Mode")]
        public bool Debug
        {
            get;
            set;
        }

        [DirectoryArgument('D', "DiagnosticPath",
                            Optional = false,
                            DirectoryMustExist = true,
                            Description = "The directory location of the diagnostic files. The structure of these folders and files is depending on the value of DiagnosticNoSubFolders. If Relative Path, this path is merged with current default directory path. The defaults are defined in the DiagnosticPath app-config file.")]
        public System.IO.DirectoryInfo DiagnosticPath
        {
            get
            {
                return ParserSettings.DiagnosticPath.DirectoryInfo();
            }
            set
            {
                if(value != null)
                {
                    ParserSettings.DiagnosticPath = ParserSettings.MakeDirectoryPath(value.ToString(), ParserSettings.DiagnosticPath);
                }
            }
        }

        [ValueArgument(typeof(ParserSettings.DiagFolderStructOptions),
                            'O', "DiagFolderStruct",
                            DefaultValue = ParserSettings.DiagFolderStructOptions.OpsCtrDiagStruct,
                            Optional = true,
                            Description = "Structure of the folders and file names used to determine the context of each file.")]
        public ParserSettings.DiagFolderStructOptions DiagFolderStruct
        {
            get { return ParserSettings.DiagFolderStruct; }
            set
            {
                ParserSettings.DiagFolderStruct = value;
            }
        }

        [FileArgument('P', "ExcelFilePath",
                            Optional = true,
                            FileMustExist = false,
                            Description = "Excel target file. If Relative Path, this path is merged with the current default file path. If the default file path is null, the DiagnosticPath is used for the merger. The defaults are defined in the ExcelFilePath app-config file.")]
        public System.IO.FileInfo ExcelFilePath
        {
            get
            {
                return ParserSettings.ExcelFilePath.FileInfo();
            }
            set
            {
                if (value != null)
                {
                    ParserSettings.ExcelFilePath = ParserSettings.MakeFilePath(value.ToString(), ParserSettings.ExcelFilePath?.ParentDirectoryPath, ParserSettings.ExcelFilePath);
                }
            }
        }

        [ValueArgument(typeof(DateTime?),
                           'T', "DiagCaptureTime",
                           DefaultValue = null,
                           Optional = true,
                           Description = "This machine's local Date/Time when either the OpsCenter Diagnostic TarBall was created or when the \"nodetool\" statical (e.g., cfstats) capture occurred. Null will use the Date embedded in the OpsCenter tar ball directory.")]
        public DateTime? DiagCaptureTime
        {
            get
            {
                return DSEDiagnosticLibrary.DSEInfo.NodeToolCaptureUTCTimestamp.HasValue
                                ? (DateTime?) Common.TimeZones.ConvertFromUTC(DSEDiagnosticLibrary.DSEInfo.NodeToolCaptureUTCTimestamp.Value).DateTime
                                : null;
            }
            set
            {
                DSEDiagnosticLibrary.DSEInfo.NodeToolCaptureUTCTimestamp = value.HasValue ? (DateTime?) Common.TimeZones.ConvertToOffset(value.Value).UtcDateTime : null;
            }
        }

        [ValueArgument(typeof(int),
                           "LogRangeBasedOnPrevHrs",
                           Optional = true,
                           Description = "Only import log entries based on the previous <X> hours from DiagCaptureTime. only valid if DiagCaptureTime is defined or is a OpsCtrDiagStruct. -1 disables this option")]
        public int LogRangeBasedOnPrevHrs
        {
            get { return ParserSettings.OnlyIncludeXHrsofLogsFromDiagCaptureTime; }
            set { ParserSettings.OnlyIncludeXHrsofLogsFromDiagCaptureTime = value; }
        }

        [ValueArgument(typeof(string),
                           "LogTimeRange",
                           Optional = true,
                           Description = "Only import log entries from/to this date/time range. Empty string will parse all entries. Syntax: \"<FromDateTimeOnly> [IANA TimeZone Name]\", \", <ToDateTimeOnly> [IANA TimeZone Name]\", or \"<FromDateTime> [IANA TimeZone Name],<ToDateTime> [IANA TimeZone Name]\". If [IANA TimeZone Name] is not given the local machine's TZ is used. Ignored if LogRangeBasedOnPrevHrs is defined.")]

        public string LogTimeRange
        {
            get
            {
                return DSEDiagnosticFileParser.file_cassandra_log4net.LogTimeRangeUTC == null
                          ? null
                          : string.Format("{0} UTC, {1} UTC", DSEDiagnosticFileParser.file_cassandra_log4net.LogTimeRangeUTC.Min, DSEDiagnosticFileParser.file_cassandra_log4net.LogTimeRangeUTC.Max);
            }
            set
            {
                if (string.IsNullOrEmpty(value))
                {
                    DSEDiagnosticFileParser.file_cassandra_log4net.LogTimeRangeUTC = null;
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
                            DateTime startDT = DateTime.Parse(startTZ);
                            DateTime endDT = DateTime.Parse(endTZ);

                            DSEDiagnosticFileParser.file_cassandra_log4net.LogTimeRangeUTC = new DateTimeRange(string.IsNullOrEmpty(startTZ)
                                                                                                                    ? startDT.Convert("UTC")
                                                                                                                    : startDT.Convert(startTZ, "UTC"),
                                                                                                                string.IsNullOrEmpty(endTZ)
                                                                                                                    ? endDT.Convert("UTC")
                                                                                                                    : endDT.Convert(endTZ, "UTC"));
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
        }

        [ValueArgument(typeof(string),
                           "IgnoreKeySpaces",
                           Optional = true,
                           Description = "A list of keyspaces that will be ignored separated by a comma. If a keyspace begins with a \"+\" or \"-\" it will be either added or removed from the current default ignored list. The defaults are defined in the IgnoreKeySpaces app-config file.")]
        public string IgnoreKeySpaces
        {
            get { return string.Join(", ", ParserSettings.IgnoreKeySpaces); }
            set
            {
                ParserSettings.IgnoreKeySpaces = ParserSettings.CreateMergeList(value, ParserSettings.IgnoreKeySpaces);
            }
        }

        private bool _loadSystemDSEKeyspaces = true;
        [SwitchArgument("DisableSysDSEKSLoading", false, Optional = true, Description = "If defined the system/DSE keyspaces are not auto-loaded. The default is to load these keyspaces. Note that the app-config file element DSESystemDDL of DSEDiagnosticFileParser lib contains the DDL of the keyspaces/tables loaded.")]
        public bool DisableSysDSEKSLoading
        {
            get { return !this._loadSystemDSEKeyspaces; }
            set
            {
                if(value != this._loadSystemDSEKeyspaces)
                {
                    this._loadSystemDSEKeyspaces = value;
                    if(!this._loadSystemDSEKeyspaces)
                    {
                        DSEDiagnosticFileParser.cql_ddl.DisabledSystemDSEDefaultKeyspacesInitialLoading();
                    }
                }
            }
        }


    }
}
