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
        public static string[] ExtractFilesWithExtensions = Properties.Settings.Default.ExtractFilesWithExtensions.ToArray(false);
        public static FileMapper[] ProcessFileMappings = JsonConvert.DeserializeObject<FileMapper[]>(Properties.Settings.Default.ProcessFileMappings);
        public static IFilePath SevenZipCOMFilePath = MakeFilePath(Properties.Settings.Default.SevenZipCOMFilePath);
        public static Dictionary<string,RegExParseString> DiagnosticFileRegExAssocations = JsonConvert.DeserializeObject<Dictionary<string, RegExParseString>>(Properties.Settings.Default.DiagnosticFileRegExAssocations);

        private static IFilePath MakeFilePath(string filePath)
        {
            return string.IsNullOrEmpty(filePath) ? null : Common.Path.PathUtils.BuildFilePath(filePath);
        }
    }
}
