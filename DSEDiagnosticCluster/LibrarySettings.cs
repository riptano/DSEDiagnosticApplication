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
        public static UnitOfMeasure.Types DefaultMemoryStorageSizeUnit = ParseEnum<UnitOfMeasure.Types>(Properties.Settings.Default.DefaultMemoryStorageSizeUnit);
        public static UnitOfMeasure.Types DefaultTimeUnit = ParseEnum<UnitOfMeasure.Types>(Properties.Settings.Default.DefaultTimeUnit);
        public static string IPAdressRegEx = Properties.Settings.Default.IPAdressRegEx;
        public static Dictionary<string, string> SnitchFileMappings = CreateSnitchDictionary(Properties.Settings.Default.SnitchFileMappings);
        public static List<YamlConfigurationLine.ConfigTypeMapper> ConfigTypeMappers = JsonConvert.DeserializeObject<List<YamlConfigurationLine.ConfigTypeMapper>>(Properties.Settings.Default.ConfigTypeMappers);

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
