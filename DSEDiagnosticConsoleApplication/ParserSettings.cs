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

        public static List<string> CreateMergeList(string strList, IEnumerable<string> defaultCollection = null)
        {
            var splitItems = Common.StringFunctions.Split(strList, ',');
            var merge = splitItems.Any(i => !string.IsNullOrEmpty(i) && (i[0] == '+' || i[0] == '-'));

            if(merge)
            {
                bool defaultActionAdd = splitItems[0][0] != '-';

                if(defaultCollection == null)
                {
                    return splitItems.Where(i => !string.IsNullOrEmpty(i) && (i[0] == '+' || defaultActionAdd)).Select(i => i[0] != '+' ? i : i.Substring(1)).ToList();
                }

                var mergeList = defaultCollection.ToList();

                foreach(var item in splitItems)
                {
                    if(!string.IsNullOrEmpty(item))
                    {
                        if(item[0] == '+')
                        {
                            mergeList.Add(item.Substring(1));
                        }
                        else if (item[0] == '-')
                        {
                            mergeList.Remove(item.Substring(1));
                        }
                        else if(defaultActionAdd)
                        {
                            mergeList.Add(item);
                        }
                        else
                        {
                            mergeList.Remove(item);
                        }
                    }
                }

                return mergeList;
            }

            return splitItems.ToList();
        }

        public static IDirectoryPath MakeDirectoryPath(string dirPath, IDirectoryPath defaultPath = null)
        {
            if (string.IsNullOrEmpty(dirPath)) return null;

            var newPath = PathUtils.BuildDirectoryPath(dirPath);

            if(defaultPath != null && newPath.IsRelativePath && defaultPath.IsAbsolutePath)
            {                
                if(newPath.MakePathFrom((IAbsolutePath) defaultPath, out IAbsolutePath absPath))
                {
                    newPath = (IDirectoryPath) absPath;
                }
            }

            return newPath;
        }
        public static IFilePath MakeFilePath(string filePath, IDirectoryPath defaultDirPath = null, IFilePath defaultFile = null)
        {
            if (string.IsNullOrEmpty(filePath)) return null;

#pragma warning disable IDE0019 // Use pattern matching
            var newFile = PathUtils.BuildPath(filePath,
                                                defaultDirPath?.Path,
                                                defaultFile?.FileExtension,
                                                true,
                                                true,
                                                true,
                                                false) as IFilePath;
#pragma warning restore IDE0019 // Use pattern matching

            if(newFile == null)
            {
                newFile = PathUtils.BuildFilePath(filePath);
            }

            return newFile;
        }

        public static string SettingValues()
        {
            StringBuilder settingsValues = new StringBuilder();
            var settingValueList = SettingValueList();

            foreach (var settingItem in settingValueList)
            {               
                settingsValues.AppendLine(string.Format("\t{0} ({1}): {2}", settingItem.Item1, settingItem.Item2, settingItem.Item3));
            }

            return settingsValues.ToString();
        }

        public static IEnumerable<Tuple<string,string,string>> SettingValueList()
        {
            var settingsValues = new List<Tuple<string,string,string>>();
            var fields = typeof(ParserSettings).GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);

            foreach (var fld in fields)
            {
                var item = fld.GetValue(null);
                var strItem = item is Enum ? item.ToString() : Common.JSONExtensions.ToJSON(item, true);
                var strType = fld.FieldType.IsGenericType
                                    ? string.Format("{0}<{1}>",
                                                        fld.FieldType.Name,
                                                        string.Join(", ", fld.FieldType.GenericTypeArguments.Select(t => t.Name)))
                                    : fld.FieldType.Name;

                settingsValues.Add(new Tuple<string,string,string>(fld.Name, strType, strItem));
            }

            var props = typeof(ParserSettings).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);

            foreach (var prop in props)
            {
                var item = prop.GetValue(null);
                var strItem = item is Enum ? item.ToString() : Common.JSONExtensions.ToJSON(item, true);
                var strType = prop.PropertyType.IsGenericType
                                    ? string.Format("{0}<{1}>",
                                                        prop.PropertyType.Name,
                                                        string.Join(", ", prop.PropertyType.GenericTypeArguments.Select(t => t.Name)))
                                    : prop.PropertyType.Name;

                settingsValues.Add(new Tuple<string, string, string>(prop.Name, strType, strItem));
            }

            return settingsValues;
        }
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
            NodeSubFldStruct = 2,
            /// <summary>
            /// The diagnostic file structure generated by a node's OpsCenter Agent
            /// </summary>
            NodeAgentDiagStruct = 3
        }

        #endregion

        #region Settings

        public static IDirectoryPath DiagnosticPath = MakeDirectoryPath(Properties.Settings.Default.DiagnosticPath);
        public static IFilePath ExcelFilePath = MakeFilePath(Properties.Settings.Default.ExcelFilePath, DiagnosticPath);
        public static DiagFolderStructOptions DiagFolderStruct = (DiagFolderStructOptions)Enum.Parse(typeof(DiagFolderStructOptions), Properties.Settings.Default.DiagFolderStruct);
        public static List<string> IgnoreKeySpaces = Properties.Settings.Default.IgnoreKeySpaces.ToList(false);
        public static int OnlyIncludeXHrsofLogsFromDiagCaptureTime = Properties.Settings.Default.OnlyIncludeXHrsofLogsFromDiagCaptureTime;
        public static List<KeyValuePair<string, IFilePath>> AdditionalFilesForParsingClass = new List<KeyValuePair<string, IFilePath>>();
        public static List<string> WarnWhenKSTblIsDetected = Properties.Settings.Default.WarnWhenKSTblIsDetected.ToList(false);
        public static IFilePath ExcelFileTemplatePath = MakeFilePath(Properties.Settings.Default.ExcelFileTemplatePath, ExcelFilePath?.ParentDirectoryPath);
        public static TimeSpan LogAggregationPeriod
        {
            get { return DSEDiagnosticAnalytics.LibrarySettings.LogAggregationPeriod; }
            set { DSEDiagnosticAnalytics.LibrarySettings.LogAggregationPeriod = value; }
        }

        public static DateTimeOffsetRange LogRestrictedTimeRange
        {
            get { return DSEDiagnosticFileParser.LibrarySettings.LogRestrictedTimeRange; }
            set { DSEDiagnosticFileParser.LibrarySettings.LogRestrictedTimeRange = value; }
        }
        public static DateTimeOffset? NodeToolCaptureTimestamp
        {
            get { return DSEDiagnosticLibrary.LibrarySettings.NodeToolCaptureTimestamp; }
            set { DSEDiagnosticLibrary.LibrarySettings.NodeToolCaptureTimestamp = value; }
        }
        public static string ProcessFileMappingValue
        {
            get { return DSEDiagnosticFileParser.LibrarySettings.ProcessFileMappingValue; }
            set { DSEDiagnosticFileParser.LibrarySettings.ProcessFileMappingValue = value; }
        }
        public static bool DisableParallelProcessing
        {
            get { return DSEDiagnosticFileParser.DiagnosticFile.DisableParallelProcessing; }
            set { DSEDiagnosticFileParser.DiagnosticFile.DisableParallelProcessing = value; }
        }

        public static int LogFileInfoAnalysisGapTriggerInMins
        {
            get { return (int) DSEDiagnosticAnalytics.LibrarySettings.LogFileInfoAnalysisGapTriggerInMins; }
            set { DSEDiagnosticAnalytics.LibrarySettings.LogFileInfoAnalysisGapTriggerInMins = value; }
        }

        public static int LogFileInfoAnalysisContinousEventInDays
        {
            get { return (int)DSEDiagnosticAnalytics.LibrarySettings.LogFileInfoAnalysisContinousEventInDays; }
            set { DSEDiagnosticAnalytics.LibrarySettings.LogFileInfoAnalysisContinousEventInDays = value; }
        }

        public static bool AppendToWorkSheet
        {
            get { return DSEDiagtnosticToExcel.LibrarySettings.AppendToWorkSheet; }
            set { DSEDiagtnosticToExcel.LibrarySettings.AppendToWorkSheet = value; }
        }

        public static DSEDiagnosticFileParser.file_cassandra_log4net.DebugLogProcessingTypes DebugLogProcessing
        {
            get { return DSEDiagnosticFileParser.LibrarySettings.DebugLogProcessing; }
            set { DSEDiagnosticFileParser.LibrarySettings.DebugLogProcessing = value; }
        }

        public static bool LoadSystemDSEKeyspaces
        {            
            get { return DSEDiagnosticFileParser.cql_ddl.LoadSystemDSEKeyspaces; }           
        }

        public static string Log4NetConversionPattern
        {
            get { return DSEDiagnosticFileParser.LibrarySettings.Log4NetConversionPattern; }
            set { DSEDiagnosticFileParser.LibrarySettings.Log4NetConversionPattern = value; }
        }

        public static string DefaultClusterTZ
        {
            get { return DSEDiagnosticLibrary.LibrarySettings.DefaultClusterTZ; }
            set { DSEDiagnosticLibrary.LibrarySettings.DefaultClusterTZ = value; }
        }

        public static string ClusterName
        {
            get;
            set;
        }

        public static int? ClusterHashCode
        {
            get;
            set;
        }

        public static DSEDiagnosticLibrary.DefaultAssocItemToTimeZone[] DCDefaultTimeZones
        {
            get { return DSEDiagnosticLibrary.DataCenter.DefaultTimeZones; }
            set { DSEDiagnosticLibrary.DataCenter.DefaultTimeZones = value; }
        }

        public static DSEDiagnosticLibrary.DefaultAssocItemToTimeZone[] NodeDefaultTimeZones
        {
            get { return DSEDiagnosticLibrary.Node.DefaultTimeZones; }
            set { DSEDiagnosticLibrary.Node.DefaultTimeZones = value; }
        }

        public static bool LogEventsAreMemoryMapped
        {
            get { return DSEDiagnosticLibrary.LibrarySettings.LogEventsAreMemoryMapped; }
            set { DSEDiagnosticLibrary.LibrarySettings.LogEventsAreMemoryMapped = value; }
        }

        public static string Profile
        {
            get { return Profiles.CurrentProfile?.ProfileName ?? Profiles.DefaultProfileName; }            
        }

        public static DSEDiagnosticFileParser.file_cassandra_log4net.DebugLogProcessingTypes DebugLogProcessingType
        {
            get { return DSEDiagnosticFileParser.LibrarySettings.DebugLogProcessing; }
            set { DSEDiagnosticFileParser.LibrarySettings.DebugLogProcessing = value; }
        }

        public static DSEDiagnosticFileParser.file_cassandra_log4net.DefaultLogLevelHandlers DefaultLogLevelHandling
        {
            get { return DSEDiagnosticFileParser.LibrarySettings.DefaultLogLevelHandling; }
            set { DSEDiagnosticFileParser.LibrarySettings.DefaultLogLevelHandling = value; }
        }

        public static List<string> OnlyNodes = new List<string>();

        public static Version DSEVersion = null;

        private static List<string> _IgnoreLogParsingTagEvents = Properties.Settings.Default.IgnoreLogParsingTagEvents?.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries).ToList();
        public static IEnumerable<string> IgnoreLogParsingTagEvents
        {
            get { return _IgnoreLogParsingTagEvents; }
            set
            {
                _IgnoreLogParsingTagEvents = value?.ToList();
            }
        }

        public static bool OldExcelWorksheets
        {
            get;
            set;
        }

        public static bool LogIgnoreParsingErrors
        {
            get { return DSEDiagnosticFileParser.LibrarySettings.LogIgnoreParsingErrors; }
            set { DSEDiagnosticFileParser.LibrarySettings.LogIgnoreParsingErrors = value; }
        }

        public static bool EnableAttrSymbols
        {
            get { return DSEDiagnosticLibrary.LibrarySettings.EnableAttrSymbols; }
            set { DSEDiagnosticLibrary.LibrarySettings.EnableAttrSymbols = value; }
        }

        private static bool batchMode = false;
        public static bool BatchMode
        {
            get { return batchMode; }
            set
            {
                batchMode = value;

                if (batchMode)
                    TraceExceptions = true;
                else
                    TraceExceptions = Properties.Settings.Default.TraceExceptions;                
            }
        }

        public static bool TraceExceptions
        {
            get;
            set;
        } = Properties.Settings.Default.TraceExceptions;

        #endregion
    }
}
