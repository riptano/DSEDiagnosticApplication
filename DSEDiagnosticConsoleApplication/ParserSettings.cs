using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Common.Path;

namespace DSEDiagnosticConsoleApplication
{
    public static class ParserSettings
    {
        #region Functions

        public static DateTimeRange ToDateTimeRange(this DateTime dt)
        {
            return new DateTimeRange(dt, DateTime.MaxValue);
        }

        public static T ParseEnumString<T>(string enumString)
        {
            enumString = enumString.Replace("|", ",").Replace(" + ", ",").Replace(" - ", ", !").Replace(" ~", ", !");

            var enumValues = enumString.Split(',');
            T enumValue = (T)Enum.Parse(typeof(T), string.Join(",", enumValues.Where(i => !i.TrimStart().StartsWith("!"))), true);
            string removeValues = string.Join(",", enumValues.Where(i => i.TrimStart().StartsWith("!")).Select(i => i.TrimStart().Substring(1).TrimStart()));

            if (!string.IsNullOrEmpty(removeValues))
            {
                enumValue &= ~(dynamic)(T)Enum.Parse(typeof(T), removeValues, true);
            }

            return enumValue;
        }

        public static T ParseEnumString<T>(string enumString, T appendValue)
        {
            enumString = enumString?.Trim();

            if (string.IsNullOrEmpty(enumString))
            {
                return default(T);
            }

            if (enumString[0] == '|'
                    || enumString[0] == ','
                    || enumString[0] == '~')
            {
                return ParseEnumString<T>(appendValue.ToString()
                                                + " "
                                                + enumString);
            }

            if (enumString[0] == '+'
                    || enumString[0] == '-')
            {
                return ParseEnumString<T>(appendValue.ToString()
                                                + " "
                                                + enumString[0]
                                                + " "
                                                + enumString.Substring(1));
            }

            if (enumString[0] == '!')
            {
                return ParseEnumString<T>(appendValue.ToString() + ", " + enumString);
            }

            return ParserSettings.ParseEnumString<T>(enumString);
        }

        public static IDirectoryPath MakeDirectoryPath(string dirPath) { return PathUtils.BuildDirectoryPath(dirPath); }

        #endregion

        #region Enums

        public enum DiagFolderStructOptions
        {
            /// <summary>
            /// OpsCenter Diagnostic Tar-Ball structure
            /// </summary>
            OpsCtrDiagStruct = 0,
            /// <summary>
            /// Each file has the Node&apos;s IP Adress (prefixed or suffix) with the Nodetool/DSETool command.
            /// Example: 10.0.0.1-cfstats, 10.0.0.1-system.log, 10.0.0.1-cassandra.yaml, etc.
            /// </summary>
            IndivFiles = 1,
            /// <summary>
            /// Each file is within a folder where the Node&apos;s IP Adress (prefixed or suffix) is within the folder name. All files within this folder
            /// are prefixed by the command (e.g., dsetool, nodetool, etc.) followed by the command&apos;s subcommand/action. Logs and configuration files are just normal.
            /// Example: 10.0.0.1Folder\nodetool.ring, 10.0.0.1Folder\nodetool.cfstats, 10.0.0.1Folder\dsetool.ring, 10.0.0.1Folder\cqlsh.describe.cql, 10.0.0.1Folder\system.log, 10.0.0.1Folder\cassandra.yaml
            /// </summary>
            NodeSubFldStruct = 2
        }

        #endregion

        #region Settings

        public static IDirectoryPath DiagnosticPath = MakeDirectoryPath(Properties.Settings.Default.DiagnosticPath);
        public static DiagFolderStructOptions DiagFolderStruct = (DiagFolderStructOptions)Enum.Parse(typeof(DiagFolderStructOptions), Properties.Settings.Default.DiagFolderStruct);

        public static List<string> IgnoreKeySpaces = Properties.Settings.Default.IgnoreKeySpaces.ToList(false);

        public static string ExcelWorkBookFileExtension = null;
        public static bool DivideWorksheetIfExceedMaxRows = true;
        #endregion
    }
}
