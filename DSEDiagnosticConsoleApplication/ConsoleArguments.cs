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
                            Description = "The directory location of the diagnostic files. The structure of these folders and files is depending on the value of DiagnosticNoSubFolders. If Relative Path, this path is merged with default value.")]
        public System.IO.DirectoryInfo DiagnosticPath
        {
            get
            {
                return ParserSettings.DiagnosticPath.DirectoryInfo();
            }
            set
            {
                if(value == null)
                {
                    ParserSettings.DiagnosticPath = null;
                }
                else
                {
                    ParserSettings.DiagnosticPath = ParserSettings.MakeDirectoryPath(value.ToString());
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
    }
}
