using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticConsoleApplication
{
    public static class Profiles
    {
        public static IEnumerable<Profile> ProfileCollection = DSEDiagnosticFileParser.LibrarySettings.ReadJsonFileIntoObject<Profile[]>(Properties.Settings.Default.Profiles);

        public static string DefaultProfileName = Properties.Settings.Default.DefaultProfile;

        public static Profile CurrentProfile = null;

        public static Profile SetProfile(string name = null, bool setAssocatedOptions = true)
        {
            CurrentProfile = name == null
                                        ? ProfileCollection.FirstOrDefault(p => DefaultProfileName == p.ProfileName)
                                        : ProfileCollection.FirstOrDefault(p => p.ProfileName == name);

            if(CurrentProfile != null && setAssocatedOptions)
            {
                DSEDiagnosticFileParser.LibrarySettings.DefaultLogLevelHandling = DSEDiagnosticLibrary.LibrarySettings.ParseEnum<DSEDiagnosticFileParser.file_cassandra_log4net.DefaultLogLevelHandlers>(CurrentProfile.DefaultLogLevelHandling);
                DSEDiagnosticFileParser.LibrarySettings.Log4NetParser = DSEDiagnosticFileParser.LibrarySettings.ReadJsonFileIntoObject<DSEDiagnosticFileParser.CLogTypeParser>(CurrentProfile.Log4NetParser);
                DSEDiagnosticFileParser.LibrarySettings.ProcessFileMappingValue = CurrentProfile.ProcessFileMappings;
                DSEDiagnosticLibrary.LibrarySettings.LogEventsAreMemoryMapped = CurrentProfile.EnableVirtualMemory;
                DSEDiagnosticFileParser.LibrarySettings.DebugLogProcessing = DSEDiagnosticLibrary.LibrarySettings.ParseEnum<DSEDiagnosticFileParser.file_cassandra_log4net.DebugLogProcessingTypes>(CurrentProfile.DebugLogProcessingTypes);
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
    }
}
