using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticAnalytics
{
    public static class LibrarySettings
    {
        public static double LogFileInfoAnalysisGapTriggerInMins = Properties.Settings.Default.LogFileInfoAnalysisGapTriggerInMins;
        public static double LogFileInfoAnalysisContinousEventInDays = Properties.Settings.Default.LogFileInfoAnalysisContinousEventInDays;
        public static Dictionary<string, AggregateGroups> AggregateGroups = DSEDiagnosticFileParser.LibrarySettings.ReadJsonFileIntoObject<Dictionary<string, AggregateGroups>>(Properties.Settings.Default.AggregateGroups);

    }
}
