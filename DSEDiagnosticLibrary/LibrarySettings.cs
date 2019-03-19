using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Newtonsoft.Json;

namespace DSEDiagnosticLibrary
{
    public static class LibrarySettings
    {
        static LibrarySettings() { }

        public static char[] HostNamePathNameCharSeparators = Properties.Settings.Default.HostNamePathNameCharSeparators.ToEnumerable()
                                                                    .Select(s => string.IsNullOrEmpty(s) ? ' ' : s[0])
                                                                    .DuplicatesRemoved(i => i)
                                                                    .ToArray();
        public static UnitOfMeasure.Types DefaultStorageSizeUnit = ParseEnum<UnitOfMeasure.Types>(Properties.Settings.Default.DefaultStorageSizeUnit);
        public static UnitOfMeasure.Types DefaultMemorySizeUnit = ParseEnum<UnitOfMeasure.Types>(Properties.Settings.Default.DefaultMemorySizeUnit);
        public static UnitOfMeasure.Types DefaultMemoryRate = ParseEnum<UnitOfMeasure.Types>(Properties.Settings.Default.DefaultMemoryRate);
        public static UnitOfMeasure.Types DefaultStorageRate = ParseEnum<UnitOfMeasure.Types>(Properties.Settings.Default.DefaultStorageRate);
        public static UnitOfMeasure.Types DefaultTimeUnit = ParseEnum<UnitOfMeasure.Types>(Properties.Settings.Default.DefaultTimeUnit);
        public static string IPAdressRegEx = Properties.Settings.Default.IPAdressRegEx;
        public static List<YamlConfigurationLine.ConfigTypeMapper> ConfigTypeMappers = JsonConvert.DeserializeObject<List<YamlConfigurationLine.ConfigTypeMapper>>(Properties.Settings.Default.ConfigTypeMappers);
        public static int UnitOfMeasureRoundDecimals = Properties.Settings.Default.UnitOfMeasureRoundDecimals;
        public static string[] CQLCollectionTypes = Properties.Settings.Default.CQLCollectionTypes.ToArray();
        public static string TupleRegExStr = Properties.Settings.Default.TupleRegEx;
        public static string FrozenRegExStr = Properties.Settings.Default.FrozenRegEx;
        public static string StaticRegExStr = Properties.Settings.Default.StaticRegEx;
        public static string PrimaryKeyRegExStr = Properties.Settings.Default.PrimaryKeyRegEx;
        public static string BlobRegExStr = Properties.Settings.Default.BlobRegEx;
        public static string CounterRegExStr = Properties.Settings.Default.CounterRegEx;
        public static string[] SystemKeyspaces = Properties.Settings.Default.SystemKeyspaces.ToArray();
        public static string[] DSEKeyspaces = Properties.Settings.Default.DSEKeyspaces.ToArray();
        public static string[] PerformanceKeyspaces = Properties.Settings.Default.PerformanceKeyspaces.ToArray();
        public static string[] TablesUsageFlag = Properties.Settings.Default.TablesUsageFlag.ToArray();
        public static string[] IsSasIIIndexClasses = Properties.Settings.Default.IsSasIIIndexClasses.ToArray();
        public static string[] IsSolrIndexClass = Properties.Settings.Default.IsSolrIndexClass.ToArray();
        public static string[] SSTableVersionMarkers = Properties.Settings.Default.SSTableVersionMarkers.ToArray();
        public static int LogMessageToStringMaxLength = Properties.Settings.Default.LogMessageToStringMaxLength;
        public static string DefaultClusterTZ = Properties.Settings.Default.DefaultClusterTZ;
        public static bool LogEventsAreMemoryMapped = Properties.Settings.Default.LogEventsAreMemoryMapped;
        public static bool EnableAttrSymbols = Properties.Settings.Default.EnableAttrSymbols;
        public static TimeSpan NodeDetectedLongPuaseThreshold = Properties.Settings.Default.NodeDetectedLongPuaseThreshold;
        
        public static DateTimeOffset? NodeToolCaptureTimestamp
        {
            get { return DSEDiagnosticLibrary.DSEInfo.NodeToolCaptureTimestamp; }
            set { DSEDiagnosticLibrary.DSEInfo.NodeToolCaptureTimestamp = value; }
        }

        public static T ParseEnum<T>(string enumValue)
            where T : struct
        {
            T enumItem;

            if(Enum.TryParse<T>(enumValue, out enumItem))
            {
                return enumItem;
            }

            return default(T);
        }

        public static IEnumerable<Tuple<char,string>> GetAttributeSymbols()
        {
            return new List<Tuple<char, string>>()
            {
                new Tuple<char, string>((char)Properties.Settings.Default.HighAttrChar, "Indicates high value"),
                new Tuple<char, string>((char)Properties.Settings.Default.LowAttrChar, "Indicates low value"),
                new Tuple<char, string>((char)Properties.Settings.Default.MidAttrChar, "Indicates value within typical/normal ranges"),
                new Tuple<char, string>((char)Properties.Settings.Default.PHAttrChar, "No Value/NaN"),
                new Tuple<char, string>((char)Properties.Settings.Default.SolrAttrChar, "solr"),
                new Tuple<char, string>((char)Properties.Settings.Default.AnalyticsAttrChar, "Analytics"),
                new Tuple<char, string>((char)Properties.Settings.Default.GraphAttrChar, "Graph"),
                new Tuple<char, string>((char)Properties.Settings.Default.IndexAttrChar, "Index"),
                new Tuple<char, string>((char)Properties.Settings.Default.MVAttrChar, "Materialized Value"),
                new Tuple<char, string>((char)Properties.Settings.Default.MVTblAttrChar, "Source of a Materialized View(s)"),
                new Tuple<char, string>((char)Properties.Settings.Default.TriggerAttrChar, "Trigger"),
                new Tuple<char, string>((char)Properties.Settings.Default.UpTimeLogMismatchAttrChar, "Mismatch")
            };
        }

    }
}
