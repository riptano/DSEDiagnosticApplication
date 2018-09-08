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
        public static DataColumn AllowDBNull(this DataColumn dataColumn, bool enabled = true)
        {
            dataColumn.AllowDBNull = enabled;
            return dataColumn;
        }

        public static DataColumn SetCaption(this DataColumn dataColumn, string caption)
        {
            dataColumn.Caption = caption;
            return dataColumn;
        }

        /// <summary>
        /// Sets a formula using a R1C1 content. Placeholders can be uses where &apos;{0}&apos; is the first row in the worksheet, &apos;{1}&apos; is the last row in the worksheet, and &apos;{2}&apos; is this data column&apos;s position in the worksheet.
        /// <param name="dataColumn"></param>
        /// <param name="r1c1Formula"></param>
        /// <returns></returns>
        /// <example>
        /// dtDCInfo.Columns.Add(&quot;Distribution Storage From&quot;, typeof(decimal)).SetFormulaR1C1(&quot;sum(R{0}C{2}:R{1}C{2})&quot;);
        /// </example>
        public static DataColumn SetFormulaR1C1(this DataColumn dataColumn, string r1c1Formula)
        {
            if (dataColumn.ExtendedProperties.ContainsKey("R1C1Formula"))
            {
                if (string.IsNullOrEmpty(r1c1Formula))
                    dataColumn.ExtendedProperties.Remove("R1C1Formula");
                else
                    dataColumn.ExtendedProperties["R1C1Formula"] = r1c1Formula;
            }
            else
                dataColumn.ExtendedProperties.Add("R1C1Formula", string.Format(r1c1Formula, "{0}", "{1}", dataColumn.Ordinal + 1));

            return dataColumn;
        }

        public static DataColumn SetNumericFormat(this DataColumn dataColumn, string numericFormat)
        {
            if (dataColumn.ExtendedProperties.ContainsKey("NumericStyle"))
            {
                if (string.IsNullOrEmpty(numericFormat))
                    dataColumn.ExtendedProperties.Remove("NumericStyle");
                else
                    dataColumn.ExtendedProperties["NumericStyle"] = numericFormat;
            }
            else
                dataColumn.ExtendedProperties.Add("NumericStyle", numericFormat);

            return dataColumn;
        }

        public static DataColumn SetHeaderText(this DataColumn dataColumn, string headerText, int rowOffset, bool defineBoarders = false)
        {
            var key = string.Format("HeaderText-{0}", rowOffset);

            if (dataColumn.ExtendedProperties.ContainsKey(key))
            {
                if (string.IsNullOrEmpty(headerText))
                    dataColumn.ExtendedProperties.Remove(key);
                else
                    dataColumn.ExtendedProperties[key] = new Tuple<string, int, bool>(headerText, rowOffset, defineBoarders);
            }
            else
                dataColumn.ExtendedProperties.Add(key, new Tuple<string,int,bool>(headerText, rowOffset, defineBoarders));

            return dataColumn;
        }

        public static IEnumerable<DataColumn> SetGroupHeader(this DataTable dataTable, string headerText, int rowOffset, bool defineBoarders, params DataColumn[] dataColumns)
        {           
            if (dataColumns.Length == 0) return dataColumns;
            if(dataColumns.Length == 1)
            {
                SetHeaderText(dataColumns[0], headerText, rowOffset, defineBoarders);
                return dataColumns;
            }

            var colPositions = dataColumns.OrderBy(c => c.Ordinal);
            var key = string.Format("GroupHeadetText-{0}-{1}-{2}",
                                    colPositions.First().ColumnName,
                                    colPositions.Last().ColumnName,
                                    rowOffset);

            if (dataTable.ExtendedProperties.ContainsKey(key))
            {
                if (string.IsNullOrEmpty(headerText))
                    dataTable.ExtendedProperties.Remove(key);
                else
                    dataTable.ExtendedProperties[key] = new Tuple<string, int, bool, int, int>(headerText,
                                                                                                rowOffset,
                                                                                                defineBoarders,
                                                                                                colPositions.First().Ordinal + 1,
                                                                                                colPositions.Last().Ordinal + 1);
            }
            else
                dataTable.ExtendedProperties.Add(key,
                                                    new Tuple<string, int, bool, int, int>(headerText,
                                                                                            rowOffset,
                                                                                            defineBoarders,
                                                                                            colPositions.First().Ordinal + 1,
                                                                                            colPositions.Last().Ordinal + 1));

            return dataColumns;
        }

        public static IEnumerable<DataColumn> SetGroupHeader(this DataTable dataTable, string headerText, int rowOffset, bool defineBoarders, IEnumerable<DataColumn> prevGroup, params DataColumn[] dataColumns)
        {
            return SetGroupHeader(dataTable, headerText, rowOffset, defineBoarders, prevGroup.Concat(dataColumns).ToArray());
        }

        public static IEnumerable<DataColumn> SetGroupHeader(this DataTable dataTable, string headerText, int rowOffset, bool defineBoarders, params IEnumerable<DataColumn>[] dataColumns)
        {
            return SetGroupHeader(dataTable, headerText, rowOffset, defineBoarders, dataColumns.SelectMany(i => i).ToArray());
        }

        static readonly char[] Leters = new char[] { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z' };

        /// <summary>
        /// 
        /// </summary>
        /// <param name="colIndex">zero position </param>
        /// <returns></returns>
        public static string ExcelTranslateColumnFromColPostoLeter(int colIndex)
        {
            var level = colIndex / 26;
            string colLetter;

            if (level == 0)
            {
                colLetter = Leters[colIndex].ToString();
            }
            else
            {
                colLetter = Leters[level - 1].ToString();
                colLetter += Leters[colIndex - (level * 26)].ToString();
            }

            return colLetter;
        }

        /// <summary>
        /// Sets a formula using an A1 content. Placeholders can be uses where &apos;{0}&apos; is the first row in the worksheet, &apos;{1}&apos; is the last row in the worksheet, and &apos;{2}&apos; is this data column&apos;s position in the worksheet.
        /// <param name="dataColumn"></param>
        /// <param name="formula"></param>
        /// <returns></returns>
        /// <example>
        /// dtDCInfo.Columns.Add(&quot;Distribution Storage From&quot;, typeof(decimal)).SetFormulaR1C1(&quot;sum(R{0}C{2}:R{1}C{2})&quot;);
        /// </example>
        public static DataColumn SetFormula(this DataColumn dataColumn, string formula)
        {
            if (dataColumn.ExtendedProperties.ContainsKey("Formula"))
            {
                if (string.IsNullOrEmpty(formula))
                    dataColumn.ExtendedProperties.Remove("Formula");
                else
                    dataColumn.ExtendedProperties["Formula"] = formula;
            }
            else
                dataColumn.ExtendedProperties.Add("Formula", string.Format(formula, "{0}", "{1}", ExcelTranslateColumnFromColPostoLeter(dataColumn.Ordinal)));

            return dataColumn;
        }

        public static DataColumn TotalColumn(this DataColumn dataColumn, bool haveSumTotalLine = true, bool remove = false)
        {
            if (dataColumn.ExtendedProperties.ContainsKey("TotalColumn"))
            {
                if (remove)
                    dataColumn.ExtendedProperties.Remove("TotalColumn");
                else
                    dataColumn.ExtendedProperties["TotalColumn"] = haveSumTotalLine;
            }
            else
                dataColumn.ExtendedProperties.Add("TotalColumn", haveSumTotalLine);

            return dataColumn;
        }

        public static DataColumn EnableUnique(this DataColumn dataColumn, bool enabled = true)
        {
            dataColumn.Unique = enabled;
            return dataColumn;
        }

        public static DataColumn GetColumn(this DataTable dataTable, string columnName)
        {
            return dataTable.Columns[columnName];
        }

        public static string GetExcelColumnLetter(this DataColumn dataColumn)
        {
            return ExcelTranslateColumnFromColPostoLeter(dataColumn.Ordinal);
        }

        public static DataRow SetFieldToDecimal(this DataRow dataRow, string columnName, UnitOfMeasure uom, UnitOfMeasure.Types uomType = UnitOfMeasure.Types.Unknown, bool convertFromPercent = false)
        {
            if(!uom.NaN)
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
            if (!uom.NaN)
            {
                dataRow.SetField(columnName, (int) (uomType == UnitOfMeasure.Types.Unknown ? uom.Value : uom.ConvertTo(uomType)));
            }

            return dataRow;
        }

        public static DataRow SetFieldToLong(this DataRow dataRow, string columnName, UnitOfMeasure uom, UnitOfMeasure.Types uomType = UnitOfMeasure.Types.Unknown)
        {
            if (!uom.NaN)
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

        public static DataRow SetFieldToULong(this DataRow dataRow, string columnName, UnitOfMeasure uom, UnitOfMeasure.Types uomType = UnitOfMeasure.Types.Unknown)
        {
            if (!uom.NaN)
            {
                if (uom.Value > long.MaxValue || uom.Value < 0)
                {
                    unchecked
                    {
                        dataRow.SetField(columnName, (ulong)(uomType == UnitOfMeasure.Types.Unknown ? uom.Value : uom.ConvertTo(uomType)));
                    }
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
            if (!uom.NaN)
            {
                dataRow.SetField(columnName, (TimeSpan) uom);
            }

            return dataRow;
        }

        public static DataRow SetFieldToTimeSpan(this DataRow dataRow, string columnName, UnitOfMeasure uom, string tostringFormat)
        {
            if (!uom.NaN)
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
                                    string.Format(@"{0}{1:hh\:mm}",
                                                    offset < TimeSpan.Zero
                                                        ? "-"
                                                        : (offset == TimeSpan.Zero ? string.Empty : "+"),
                                                    offset));
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
                                    string.Format(@"{0}{1:hh\:mm}",
                                    offsetMax < TimeSpan.Zero
                                        ? "-"
                                        : (offsetMax == TimeSpan.Zero ? string.Empty : "+"),
                                    offsetMax));
            }
            else
            {
                dataRow.SetField(columnName,
                                    string.Format(@"{0}{1:hh\:mm}\{2}{3:hh\:mm}",
                                                    offsetMin < TimeSpan.Zero ? "-" : (offsetMax == TimeSpan.Zero ? string.Empty : "+"),
                                                    offsetMin,
                                                    offsetMax < TimeSpan.Zero ? "-" : (offsetMax == TimeSpan.Zero ? string.Empty : "+"),
                                                    offsetMax));
            }

            return dataRow;
        }

        public static DataRow SetFieldToTZOffset(this DataRow dataRow, string columnName, Common.DateTimeOffsetRange dtRange)
        {
            return dataRow.SetFieldToTZOffset(columnName, dtRange.Min, dtRange.Max);
        }

        public static DataRow SetFieldStringLimit(this DataRow dataRow, string columnName, string stringValue, int maxStringLength = 32767)
        {
            if (!string.IsNullOrEmpty(stringValue))
            {
                dataRow.SetField(columnName, stringValue.Length > maxStringLength ? stringValue.Substring(0, maxStringLength - 4) + "..." : stringValue);
            }

            return dataRow;
        }

        public static DataRow SetFieldStringLimit(this DataRow dataRow, string columnName, object value, int maxStringLength = 32767)
        {
            if(value is string) return SetFieldStringLimit(dataRow, columnName, (string) value, maxStringLength);

            if(dataRow.Table.Columns[columnName].DataType == typeof(string)) return SetFieldStringLimit(dataRow, columnName, value?.ToString(), maxStringLength);

            dataRow.SetField(columnName, value);
            return dataRow;
        }
    }
}
