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
        public static char[] HostNamePathNameCharSeparators = Properties.Settings.Default.HostNamePathNameCharSeparators.ToEnumerable()
                                                                    .Where(s => !string.IsNullOrEmpty(s))
                                                                    .Select(s => s[0])
                                                                    .ToArray();
        public static UnitOfMeasure.Types DefaultStorageSizeUnit = ParseEnum<UnitOfMeasure.Types>(Properties.Settings.Default.DefaultStorageSizeUnit);
        public static UnitOfMeasure.Types DefaultMemorySizeUnit = ParseEnum<UnitOfMeasure.Types>(Properties.Settings.Default.DefaultMemorySizeUnit);
        public static UnitOfMeasure.Types DefaultMemoryRate = ParseEnum<UnitOfMeasure.Types>(Properties.Settings.Default.DefaultMemoryRate);
        public static UnitOfMeasure.Types DefaultStorageRate = ParseEnum<UnitOfMeasure.Types>(Properties.Settings.Default.DefaultStorageRate);
        public static UnitOfMeasure.Types DefaultTimeUnit = ParseEnum<UnitOfMeasure.Types>(Properties.Settings.Default.DefaultTimeUnit);
        public static string IPAdressRegEx = Properties.Settings.Default.IPAdressRegEx;
        public static Dictionary<string, string> SnitchFileMappings = CreateSnitchDictionary(Properties.Settings.Default.SnitchFileMappings);
        public static List<YamlConfigurationLine.ConfigTypeMapper> ConfigTypeMappers = JsonConvert.DeserializeObject<List<YamlConfigurationLine.ConfigTypeMapper>>(Properties.Settings.Default.ConfigTypeMappers);
        public static int UnitOfMeasureRoundDecimals = Properties.Settings.Default.UnitOfMeasureRoundDecimals;
        public static string[] CQLCollectionTypes = Properties.Settings.Default.CQLCollectionTypes.ToArray();
        public static string TupleRegExStr = Properties.Settings.Default.TupleRegEx;
        public static string FrozenRegExStr = Properties.Settings.Default.FrozenRegEx;
        public static string StaticRegExStr = Properties.Settings.Default.StaticRegEx;
        public static string PrimaryKeyRegExStr = Properties.Settings.Default.PrimaryKeyRegEx;
        public static string BlobRegExStr = Properties.Settings.Default.BlobRegEx;
        public static string CounterRegExStr = Properties.Settings.Default.CounterRegEx;

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

        public static Dictionary<string, string> CreateSnitchDictionary(string configString)
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
