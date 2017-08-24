using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Net;

namespace DSEDiagnosticLibrary
{
    public static partial class MiscHelpers
    {
        public static bool IsNumber(this object value)
        {
            return value is sbyte
                    || value is byte
                    || value is short
                    || value is ushort
                    || value is int
                    || value is uint
                    || value is long
                    || value is ulong
                    || value is float
                    || value is double
                    || value is decimal;
        }

        public static object GetPropertyValue(this IProperties table, string key)
        {
            return GetPropertyValue(table.Properties, key);
        }

        public static object GetPropertyValue(this IDictionary<string,object> table, string key)
        {
            object value = null;

            if (table.TryGetValue(key, out value))
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
    }
}
