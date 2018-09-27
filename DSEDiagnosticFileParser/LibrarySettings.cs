using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Newtonsoft.Json;
using DSEDiagnosticLogger;

namespace DSEDiagnosticFileParser
{
    public static class LibrarySettings
    {
        static LibrarySettings() { }

        public static RegExLexicon RegExLexiconValues = RegExLexicon.Instance;
        public static bool DetectDuplicatedLogEvents = Properties.Settings.Default.DetectDuplicatedLogEvents;
        public static Tuple<string, string>[] ExtractFilesWithExtensions = JsonConvert.DeserializeObject<Tuple<string, string>[]>(Properties.Settings.Default.ExtractFilesWithExtensions);
        public static FileMapper[] ProcessFileMappings = ReadJsonFileIntoObject<FileMapper[]>(Properties.Settings.Default.ProcessFileMappings);
        public static Dictionary<string, RegExParseString> DiagnosticFileRegExAssocations = ReadJsonFileIntoObject<Dictionary<string, RegExParseString>>(Properties.Settings.Default.DiagnosticFileRegExAssocations);
        public static string[] ObscureFiledValues = Properties.Settings.Default.ObscureFiledValues.ToArray();
        public static Dictionary<string, string> SnitchFileMappings = CreateDictionary(Properties.Settings.Default.SnitchFileMappings);
        public static string Log4NetConversionPattern = Properties.Settings.Default.Log4NetConversionPattern;
        public static CLogTypeParser Log4NetParser = ReadJsonFileIntoObject<CLogTypeParser>(Properties.Settings.Default.Log4NetParser);
        public static string MasterLog4NetParserPathJSON = Properties.Settings.Default.MasterLog4NetParser;
        public static string[] CodeDomAssemblies = Properties.Settings.Default.CodeDomAssemblies.ToArray();
        public static file_create_folder_structure.Mappings FileCreateFolderTargetSourceMappings = ReadJsonFileIntoObject<file_create_folder_structure.Mappings>(Properties.Settings.Default.FileCreateFolderTargetSourceMappings);
        public static string[] IgnoreFileWExtensions = Properties.Settings.Default.IgnoreFileWExtensions.ToArray().Select(i => i.Trim().ToLower()).ToArray();
        public static System.Text.RegularExpressions.Regex LogExceptionRegExMatches = new System.Text.RegularExpressions.Regex(Properties.Settings.Default.LogExceptionRegExMatches, System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);
        public static file_cassandra_log4net.DefaultLogLevelHandlers DefaultLogLevelHandling = DSEDiagnosticLibrary.LibrarySettings.ParseEnum<file_cassandra_log4net.DefaultLogLevelHandlers>(Properties.Settings.Default.DefaultLogLevelHandling);
        public static file_cassandra_log4net.DebugLogProcessingTypes DebugLogProcessing = DSEDiagnosticLibrary.LibrarySettings.ParseEnum<file_cassandra_log4net.DebugLogProcessingTypes>(Properties.Settings.Default.DebugLogProcessing);
        public static IEnumerable<string> IgnoreWarningsErrosInKeySpaces = Properties.Settings.Default.IgnoreWarningsErrosInKeySpaces.ToArray();
        public static System.Text.RegularExpressions.Regex CaptureTimeFrameMatches = string.IsNullOrEmpty(Properties.Settings.Default.CaptureTimeFrameRegEx) ? null : new System.Text.RegularExpressions.Regex(Properties.Settings.Default.CaptureTimeFrameRegEx, System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.Singleline);
        public static DateTimeOffsetRange CaptureDateTimeRange = null;
        public static System.Text.RegularExpressions.Regex ClusterCaptureTimeFrameMatches = string.IsNullOrEmpty(Properties.Settings.Default.ClusterCaptureTimeFrameRegEx) ? null : new System.Text.RegularExpressions.Regex(Properties.Settings.Default.ClusterCaptureTimeFrameRegEx, System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.Singleline);
        public static bool LogIgnoreParsingErrors = Properties.Settings.Default.LogIgnoreParsingErrors;

        public static DateTimeOffsetRange LogRestrictedTimeRange
        {
            get;
            set;
        }
       
        private static string defaultProcessFileMappingValue = Properties.Settings.Default.ProcessFileMappings;
        public static string ProcessFileMappingValue
        {
            get
            {
                if(string.IsNullOrEmpty(defaultProcessFileMappingValue) || defaultProcessFileMappingValue[0] == '{' || defaultProcessFileMappingValue.IndexOf('{') > 0)
                {
                    return null;
                }

                return defaultProcessFileMappingValue;
            }
            set
            {
                if (string.IsNullOrEmpty(value) || value[0] == '{' || value.IndexOf('{') > 0)
                {
                    return;
                }

                ProcessFileMappings = ReadJsonFileIntoObject<FileMapper[]>(defaultProcessFileMappingValue = value);
            }
        }

        private static IFilePath MakeFilePath(string filePath)
        {
            return string.IsNullOrEmpty(filePath) ? null : Common.Path.PathUtils.BuildFilePath(filePath);
        }

        public static Dictionary<string, string> CreateDictionary(string configString)
        {
            var configObj = Newtonsoft.Json.JsonConvert.DeserializeObject<Tuple<string, string>[]>(configString);
            var dict = new Dictionary<string, string>();

            if (configObj != null)
            {
                foreach (var item in configObj)
                {
                    dict.Add(item.Item1, item.Item2);
                }
            }

            return dict;
        }

        public static T ReadJsonFileIntoObject<T>(string jsonStringOrFile)
        {
            if (string.IsNullOrEmpty(jsonStringOrFile))
            {
                Logger.Instance.DebugFormat("ReadJsonFileIntoObject detected a Null or empty string. \"jsonStringOrFile\" Value is \"{0}\", returning default value", jsonStringOrFile);
                return default(T);
            }

            try
            {

                if (jsonStringOrFile[0] == '{' || jsonStringOrFile.IndexOf('{') > 0)
                {
                    //string removeComments = jsonStringOrFile;
                    //DSEDiagnosticLibrary.StringHelpers.RemoveInLineComment(jsonStringOrFile, out removeComments);

                    return JsonConvert.DeserializeObject<T>(jsonStringOrFile);
                }

                IFilePath jsonPath = null;
                List<string> triedPaths = new List<string>();

                foreach (var filePath in jsonStringOrFile.Split(','))
                {
                    jsonPath = Common.Path.PathUtils.BuildFilePath(filePath);

                    if (jsonPath.Exist())
                    {
                        break;
                    }
                    triedPaths.Add(jsonPath.PathResolved);
                    jsonPath = null;
                }

                if (jsonPath == null)
                {
                    throw new System.IO.FileNotFoundException(string.Format("Json File Path was not found. Tried: {{{0}}}", string.Join(", ", triedPaths)), jsonStringOrFile);
                }

                return ReadJsonFileIntoObject<T>(jsonPath.ReadAllText());
            }
            catch (Exception e)
            {
                DiagnosticFile.InvokeExceptionEvent("Invalid Json Value or File Path",
                                                    e,
                                                    null,
                                                    new object[] { jsonStringOrFile });

                Logger.Instance.Error(string.Format("Invalid Json Value or File Path. Value is: \"{0}\"", jsonStringOrFile), e);
                throw;
            }
        }

        private static CLogTypeParser MasterLog4NetParserCache = null;
        public static CLogTypeParser MasterLog4NetParser()
        {
            return MasterLog4NetParserCache == null
                    ? MasterLog4NetParserCache = ReadJsonFileIntoObject<CLogTypeParser>(MasterLog4NetParserPathJSON)
                    : MasterLog4NetParserCache;
        }

        public static Tuple<string, DateTimeOffset, string> DetermineClusterCaptureInfo(string strInfo)
        {
            var regexMatch = LibrarySettings.ClusterCaptureTimeFrameMatches.Match(strInfo);

            if (regexMatch.Success)
            {
                var cluster = regexMatch.Groups["cluster"].Value;
                var year = regexMatch.Groups["year"].Value;
                var month = regexMatch.Groups["month"].Value;
                var day = regexMatch.Groups["day"].Value;
                var hour = regexMatch.Groups["hour"].Value;
                var min = regexMatch.Groups["min"].Value;
                var second = regexMatch.Groups["second"].Value;
                var diagTZ = regexMatch.Groups["timezone"].Value;
                DateTime diagDateTime = new DateTime(int.Parse(year), int.Parse(month), int.Parse(day),
                                                        int.Parse(hour), int.Parse(min), int.Parse(second));
                var tzInstance = DSEDiagnosticLibrary.StringHelpers.FindTimeZone(diagTZ);

                return new Tuple<string, DateTimeOffset, string>(cluster,
                                                                    tzInstance == null
                                                                        ? new DateTimeOffset(diagDateTime, TimeSpan.Zero)
                                                                        : Common.TimeZones.ConvertToOffset(diagDateTime, tzInstance),
                                                                    tzInstance == null ? "UTC?" : diagTZ);

            }

            return null;
        }

        public static Tuple<DateTimeOffset, string> DetermineNodeCaptureInfo(string strInfo)
        {
            var regexMatch = LibrarySettings.CaptureTimeFrameMatches.Match(strInfo);

            if (regexMatch.Success)
            {
                var year = regexMatch.Groups["year"].Value;
                var month = regexMatch.Groups["month"].Value;
                var day = regexMatch.Groups["day"].Value;
                var hour = regexMatch.Groups["hour"].Value;
                var min = regexMatch.Groups["min"].Value;
                var second = regexMatch.Groups["second"].Value;
                var diagTZ = regexMatch.Groups["timezone"].Value;
                DateTime diagDateTime = new DateTime(int.Parse(year), int.Parse(month), int.Parse(day),
                                                        int.Parse(hour), int.Parse(min), int.Parse(second));
                var tzInstance = DSEDiagnosticLibrary.StringHelpers.FindTimeZone(diagTZ);

                return new Tuple<DateTimeOffset, string>(tzInstance == null 
                                                            ? new DateTimeOffset(diagDateTime, TimeSpan.Zero)
                                                            : Common.TimeZones.ConvertToOffset(diagDateTime, tzInstance),
                                                            tzInstance == null ? "UTC?" : diagTZ);
               
            }

            return null;
        }
    }
}
