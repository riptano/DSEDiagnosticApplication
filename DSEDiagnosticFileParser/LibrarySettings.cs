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
        public static FileMapper[] ProcessFileMappings = JsonConvert.DeserializeObject<FileMapper[]>(Properties.Settings.Default.ProcessFileMappings);
        public static Dictionary<string,RegExParseString> DiagnosticFileRegExAssocations = JsonConvert.DeserializeObject<Dictionary<string, RegExParseString>>(Properties.Settings.Default.DiagnosticFileRegExAssocations);
        public static string[] ObscureFiledValues = Properties.Settings.Default.ObscureFiledValues.ToArray();
        public static Dictionary<string, string> SnitchFileMappings = CreateDictionary(Properties.Settings.Default.SnitchFileMappings);

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
                    dict.Add(item.Item1.ToLower(), item.Item2);
                }
            }

            return dict;
        }
    }
}
