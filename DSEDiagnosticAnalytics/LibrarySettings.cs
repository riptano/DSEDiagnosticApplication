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
        public static Dictionary<string, AggregateGroups> AggregateGroups = DSEDiagnosticParamsSettings.Helpers.ReadJsonFileIntoObject<Dictionary<string, AggregateGroups>>(Properties.Settings.Default.AggregateGroups);

        public static TimeSpan GCTimeFrameDetection = Properties.Settings.Default.GCTimeFrameDetection;
        public static double GCTimeFrameDetectionThreholdMin = Properties.Settings.Default.GCTimeFrameDetectionThreholdMin;

        public static double GCBackToBackToleranceMS = Properties.Settings.Default.GCBackToBackToleranceMS;

        public static TimeSpan LogAggregationPeriod = Properties.Settings.Default.LogAggregationPeriod;
    }
}

