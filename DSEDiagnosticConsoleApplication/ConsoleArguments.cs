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
                           Description = "The local Date/Time when either the OpsCenter Diagnostic TarBall was created or when the \"nodetool\" statical (e.g., cfstats) capture occurred. Null will use the Date embedded in the OpsCenter tar ball directory.")]
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
