using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticConsoleApplication
{
    public static class Profiles
    {
        public static IEnumerable<Profile> ProfileCollection = DSEDiagnosticParamsSettings.Helpers.ReadJsonFileIntoObject<Profile[]>(Properties.Settings.Default.Profiles);

        public static string DefaultProfileName = Properties.Settings.Default.DefaultProfile;

        public static Profile CurrentProfile = null;

        public static Profile SetProfile(string name = null, bool setAssocatedOptions = true)
        {
            CurrentProfile = name == null
                                        ? ProfileCollection.FirstOrDefault(p => DefaultProfileName == p.ProfileName)
                                        : ProfileCollection.FirstOrDefault(p => p.ProfileName == name);

            if(CurrentProfile != null && setAssocatedOptions)
            {
                DSEDiagnosticFileParser.LibrarySettings.DefaultLogLevelHandling = DSEDiagnosticParamsSettings.Helpers.ParseEnumString<DSEDiagnosticFileParser.file_cassandra_log4net.DefaultLogLevelHandlers>(CurrentProfile.DefaultLogLevelHandling);
                DSEDiagnosticFileParser.LibrarySettings.Log4NetParser = DSEDiagnosticParamsSettings.Helpers.ReadJsonFileIntoObject<DSEDiagnosticFileParser.CLogTypeParser>(CurrentProfile.Log4NetParser);
                DSEDiagnosticFileParser.LibrarySettings.ProcessFileMappingValue = CurrentProfile.ProcessFileMappings;
                DSEDiagnosticLibrary.LibrarySettings.LogEventsAreMemoryMapped = CurrentProfile.EnableVirtualMemory;
                DSEDiagnosticFileParser.LibrarySettings.DebugLogProcessing = DSEDiagnosticParamsSettings.Helpers.ParseEnumString<DSEDiagnosticFileParser.file_cassandra_log4net.DebugLogProcessingTypes>(CurrentProfile.DebugLogProcessingTypes);

                if(CurrentProfile.IgnoreLogTagEvents != null)
                    ParserSettings.IgnoreLogParsingTagEvents = CurrentProfile.IgnoreLogTagEvents.Split(new Char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            }

            return CurrentProfile;
        }

        public static IEnumerable<string> Names()
        {
            return ProfileCollection.Select(p => p.ProfileName);
        }
    }

    public sealed class Profile
    {        
        public string ProfileName;
        public string Log4NetParser;
        public string ProcessFileMappings;
        public bool EnableVirtualMemory;
        public string DefaultLogLevelHandling;
        public string DebugLogProcessingTypes;
        public string IgnoreLogTagEvents;
    }
}
