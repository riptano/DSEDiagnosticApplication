using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticToDataTable
{
    public static class MiscHelpers
    {

        public static DataRow SetFieldToDecimal(this DataRow dataRow, string columnName, UnitOfMeasure uom, UnitOfMeasure.Types uomType = UnitOfMeasure.Types.Unknown, bool convertFromPercent = false)
        {
            if(uom != null && !uom.NaN)
            {
                if (convertFromPercent && (uom.UnitType & UnitOfMeasure.Types.Percent) != 0)
                {
                    dataRow.SetField(columnName, (uomType == UnitOfMeasure.Types.Unknown ? uom.Value : uom.ConvertTo(uomType)) / 100m);
                }
                else
                {
                    dataRow.SetField(columnName, uomType == UnitOfMeasure.Types.Unknown ? uom.Value : uom.ConvertTo(uomType));
                }
            }

            return dataRow;
        }

        public static DataRow SetFieldToInt(this DataRow dataRow, string columnName, UnitOfMeasure uom, UnitOfMeasure.Types uomType = UnitOfMeasure.Types.Unknown)
        {
            if (uom != null && !uom.NaN)
            {
                dataRow.SetField(columnName, (int) (uomType == UnitOfMeasure.Types.Unknown ? uom.Value : uom.ConvertTo(uomType)));
            }

            return dataRow;
        }

        public static DataRow SetFieldToLong(this DataRow dataRow, string columnName, UnitOfMeasure uom, UnitOfMeasure.Types uomType = UnitOfMeasure.Types.Unknown)
        {
            if (uom != null && !uom.NaN)
            {
                if (uom.Value > long.MaxValue)
                {
                    dataRow.SetField(columnName, (ulong)(uomType == UnitOfMeasure.Types.Unknown ? uom.Value : uom.ConvertTo(uomType)));
                }
                else
                {
                    dataRow.SetField(columnName, (long)(uomType == UnitOfMeasure.Types.Unknown ? uom.Value : uom.ConvertTo(uomType)));
                }
            }

            return dataRow;
        }

        public static DataRow SetFieldToTimeSpan(this DataRow dataRow, string columnName, UnitOfMeasure uom)
        {
            if (uom != null && !uom.NaN)
            {
                dataRow.SetField(columnName, (TimeSpan) uom);
            }

            return dataRow;
        }

        public static DataRow SetFieldToTimeSpan(this DataRow dataRow, string columnName, UnitOfMeasure uom, string tostringFormat)
        {
            if (uom != null && !uom.NaN)
            {
                dataRow.SetField(columnName, ((TimeSpan)uom).ToString(tostringFormat));
            }

            return dataRow;
        }

        public static DataRow SetFieldToTZOffset(this DataRow dataRow, string columnName, DateTimeOffset dtOffset, bool dataColumnIsTypeTimespan = true)
        {
            return dataRow.SetFieldToTZOffset(columnName, dtOffset.Offset, dataColumnIsTypeTimespan);
        }

        public static DataRow SetFieldToTZOffset(this DataRow dataRow, string columnName, TimeSpan offset, bool dataColumnIsTypeTimespan = true)
        {
            if (dataColumnIsTypeTimespan)
            {
                dataRow.SetField(columnName, offset);
            }
            else
            {
                dataRow.SetField(columnName,
                                    string.Format(@"{0}{1:hh\:mm}", offset < TimeSpan.Zero ? "-" : "+", offset));
            }

            return dataRow;
        }

        public static DataRow SetFieldToTZOffset(this DataRow dataRow, string columnName, DateTimeOffset dtOffsetMin, DateTimeOffset dtOffsetMax)
        {
            return dataRow.SetFieldToTZOffset(columnName, dtOffsetMin.Offset, dtOffsetMax.Offset);
        }

        public static DataRow SetFieldToTZOffset(this DataRow dataRow, string columnName, TimeSpan offsetMin, TimeSpan offsetMax)
        {
            if (offsetMin == offsetMax)
            {
                dataRow.SetField(columnName,
                                    string.Format(@"{0}{1:hh\:mm}", offsetMax < TimeSpan.Zero ? "-" : "+", offsetMax));
            }
            else
            {
                dataRow.SetField(columnName,
                                    string.Format(@"{0}{1:hh\:mm}\{2}{3:hh\:mm}",
                                                    offsetMin < TimeSpan.Zero ? "-" : "+",
                                                    offsetMin,
                                                    offsetMax < TimeSpan.Zero ? "-" : "+",
                                                    offsetMax));
            }

            return dataRow;
        }

        public static DataRow SetFieldToTZOffset(this DataRow dataRow, string columnName, Common.DateTimeOffsetRange dtRange)
        {
            return dataRow.SetFieldToTZOffset(columnName, dtRange.Min, dtRange.Max);
        }

    }
}
