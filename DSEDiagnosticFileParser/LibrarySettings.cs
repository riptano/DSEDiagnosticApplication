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
        public static Dictionary<string,RegExParseString> DiagnosticFileRegExAssocations = ReadJsonFileIntoObject<Dictionary<string, RegExParseString>>(Properties.Settings.Default.DiagnosticFileRegExAssocations);
        public static string[] ObscureFiledValues = Properties.Settings.Default.ObscureFiledValues.ToArray();
        public static Dictionary<string, string> SnitchFileMappings = CreateDictionary(Properties.Settings.Default.SnitchFileMappings);
        public static string Log4NetConversionPattern = Properties.Settings.Default.Log4NetConversionPattern;
        public static CLogTypeParser Log4NetParser = ReadJsonFileIntoObject<CLogTypeParser>(Properties.Settings.Default.Log4NetParser);
        public static string[] CodeDomAssemblies = Properties.Settings.Default.CodeDomAssemblies.ToArray();
        public static file_create_folder_structure.Mappings FileCreateFolderTargetSourceMappings = ReadJsonFileIntoObject<file_create_folder_structure.Mappings>(Properties.Settings.Default.FileCreateFolderTargetSourceMappings);
        public static string[] IgnoreFileWExtensions = Properties.Settings.Default.IgnoreFileWExtensions.ToArray().Select(i => i.Trim().ToLower()).ToArray();
        public static System.Text.RegularExpressions.Regex LogExceptionRegExMatches = new System.Text.RegularExpressions.Regex(Properties.Settings.Default.LogExceptionRegExMatches, System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Singleline);
        public static file_cassandra_log4net.DefaultLogLevelHandlers DefaultLogLevelHandling = DSEDiagnosticLibrary.LibrarySettings.ParseEnum<file_cassandra_log4net.DefaultLogLevelHandlers>(Properties.Settings.Default.DefaultLogLevelHandling);
        public static file_cassandra_log4net.DebugLogProcessingTypes DebugLogProcessing = DSEDiagnosticLibrary.LibrarySettings.ParseEnum<file_cassandra_log4net.DebugLogProcessingTypes>(Properties.Settings.Default.DebugLogProcessing);
       
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
                    string removeComments;
                    DSEDiagnosticLibrary.StringHelpers.RemoveInLineComment(jsonStringOrFile, out removeComments);

                    return JsonConvert.DeserializeObject<T>(removeComments);
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
    }
}
