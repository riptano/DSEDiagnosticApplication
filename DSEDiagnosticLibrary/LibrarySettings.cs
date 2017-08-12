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
        public static string[] PerformanceKeyspaces = Properties.Settings.Default.DSEKeyspaces.ToArray();
        public static string[] TablesUsageFlag = Properties.Settings.Default.TablesUsageFlag.ToArray();
        public static string[] IsSasIIIndexClasses = Properties.Settings.Default.IsSasIIIndexClasses.ToArray();
        public static string[] IsSolrIndexClass = Properties.Settings.Default.IsSolrIndexClass.ToArray();
        public static string[] SSTableVersionMarkers = Properties.Settings.Default.SSTableVersionMarkers.ToArray();
        public static int LogMessageToStringMaxLength = Properties.Settings.Default.LogMessageToStringMaxLength;

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

    }
}
