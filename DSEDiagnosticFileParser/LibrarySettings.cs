using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Newtonsoft.Json;

namespace DSEDiagnosticFileParser
{
    public static class LibrarySettings
    {
        public static Tuple<string, string>[] ExtractFilesWithExtensions = JsonConvert.DeserializeObject<Tuple<string, string>[]>(Properties.Settings.Default.ExtractFilesWithExtensions);
        public static FileMapper[] ProcessFileMappings = ReadJsonFileIntoObject<FileMapper[]>(Properties.Settings.Default.ProcessFileMappings);
        public static Dictionary<string,RegExParseString> DiagnosticFileRegExAssocations = ReadJsonFileIntoObject<Dictionary<string, RegExParseString>>(Properties.Settings.Default.DiagnosticFileRegExAssocations);
        public static string[] ObscureFiledValues = Properties.Settings.Default.ObscureFiledValues.ToArray();
        public static Dictionary<string, string> SnitchFileMappings = CreateDictionary(Properties.Settings.Default.SnitchFileMappings);
        public static string Log4NetConversionPattern = Properties.Settings.Default.Log4NetConversionPattern;
        public static CLogTypeParser Log4NetParser = ReadJsonFileIntoObject<CLogTypeParser>(Properties.Settings.Default.Log4NetParser);
        public static string[] CodeDomAssemblies = Properties.Settings.Default.CodeDomAssemblies.ToArray();

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
                return default(T);
            }

            try
            {

                if (jsonStringOrFile[0] == '{' || jsonStringOrFile.IndexOf('{') > 0)
                {
                    return JsonConvert.DeserializeObject<T>(jsonStringOrFile);
                }

                IFilePath jsonPath = null;

                foreach (var filePath in jsonStringOrFile.Split(','))
                {
                    jsonPath = Common.Path.PathUtils.BuildFilePath(filePath);

                    if (jsonPath.Exist())
                    {
                        break;
                    }
                    jsonPath = null;
                }

                if (jsonPath == null)
                {
                    throw new System.IO.FileNotFoundException("Json File Path was not found.", jsonStringOrFile);
                }

                var jsonString = jsonPath.ReadAllText();

                return ReadJsonFileIntoObject<T>(jsonString);
            }
            catch (Exception e)
            {
                Logger.Instance.Error(string.Format("Invalid Json Value or File Path. Value is: \"{0}\"", jsonStringOrFile), e);
                throw;
            }
        }
    }
}
