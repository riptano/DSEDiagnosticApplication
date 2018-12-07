using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Net;

namespace DSEDiagnosticLibrary
{
    public struct DefaultAssocItemToTimeZone
    {
        public string Item;
        public string IANATZName;
    }

    public static partial class MiscHelpers
    {
        public static bool IsNumber(this object value)
        {
            return !(value is string)
                    && (value is sbyte
                        || value is byte
                        || value is short
                        || value is ushort
                        || value is int
                        || value is uint
                        || value is long
                        || value is ulong
                        || value is float
                        || value is double
                        || value is decimal);
        }

        public static object GetPropertyValue(this IProperties table, string key)
        {
            return GetPropertyValue(table.Properties, key);
        }

        public static object GetPropertyValue(this IDictionary<string,object> table, string key)
        {           
            if (table.TryGetValue(key, out object value))
            {
                if (value is DSEDiagnosticLibrary.UnitOfMeasure)
                {
                    return (((DSEDiagnosticLibrary.UnitOfMeasure)value).UnitType & DSEDiagnosticLibrary.UnitOfMeasure.Types.TimeUnits) != 0
                                ? (object)(TimeSpan?)((DSEDiagnosticLibrary.UnitOfMeasure)value)
                                : (object)(decimal?)((DSEDiagnosticLibrary.UnitOfMeasure)value);
                }
                return value;
            }
            return null;
        }

        public static object GetPropertyValueInMSLong(this IProperties table, string key)
        {
            return GetPropertyValueInMSLong(table.Properties, key);
        }

        public static object GetPropertyValueInMSLong(this IDictionary<string, object> table, string key)
        {            
            if (table.TryGetValue(key, out object value))
            {
                if (value is DSEDiagnosticLibrary.UnitOfMeasure)
                {
                    return (((DSEDiagnosticLibrary.UnitOfMeasure)value).UnitType & DSEDiagnosticLibrary.UnitOfMeasure.Types.TimeUnits) != 0
                                ? (object)((DSEDiagnosticLibrary.UnitOfMeasure)value).ConvertToLong(UnitOfMeasure.Types.MS)
                                : (object)(long?)((decimal?)((DSEDiagnosticLibrary.UnitOfMeasure)value));
                }
                return (long) ((dynamic) value);
            }
            return null;
        }

        public static Common.Patterns.TimeZoneInfo.IZone FindDefaultTimeZone(this DefaultAssocItemToTimeZone[] associates, string keyItem)
        {
            if (associates == null || string.IsNullOrEmpty(keyItem)) return null;
            return StringHelpers.FindTimeZone(associates.FirstOrDefault(i => i.Item == keyItem).IANATZName);
        }
    }
}
