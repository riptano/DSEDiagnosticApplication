using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

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
