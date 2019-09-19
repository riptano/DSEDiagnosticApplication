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

        public static DataTable CaseSensitive(this DataTable dataTable, bool isCaseSensitive = true)
        {
            dataTable.CaseSensitive = isCaseSensitive;
            return dataTable;
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

        public static IEnumerable<DataColumn> AddColumnsToTable(this DataTable dataTable, IEnumerable<string> columnNames, Type dataType)
        {
            var columns = new List<DataColumn>(columnNames.Count());
            DataColumn dataColumn;

            foreach (var name in columnNames)
            {
                dataColumn = new DataColumn(name, dataType);
                dataTable.Columns.Add(dataColumn);
                columns.Add(dataColumn);
            }

            return columns;
        }

        public static IEnumerable<DataColumn> AddColumnsToTable(this DataTable dataTable, IEnumerable<DataColumn> columns)
        {
            var copiedColumns = new List<DataColumn>(columns.Count());
            DataColumn dataColumn;

            foreach (var oldColumn in columns)
            {
                dataColumn = new DataColumn(oldColumn.ColumnName, oldColumn.DataType, oldColumn.Expression, oldColumn.ColumnMapping)
                {
                    DateTimeMode = oldColumn.DateTimeMode,
                    AllowDBNull = oldColumn.AllowDBNull,
                    AutoIncrement = oldColumn.AutoIncrement,
                    AutoIncrementSeed = oldColumn.AutoIncrementSeed,
                    AutoIncrementStep = oldColumn.AutoIncrementStep,
                    Caption = oldColumn.Caption,
                    DefaultValue = oldColumn.DefaultValue,
                    MaxLength = oldColumn.MaxLength,
                    Namespace = oldColumn.Namespace,
                    Prefix = oldColumn.Prefix,
                    ReadOnly = oldColumn.ReadOnly,
                    Unique = oldColumn.Unique                  
                };

                if(oldColumn.ExtendedProperties != null)
                {
                    foreach (var key in oldColumn.ExtendedProperties.Keys)                    
                    {
                        dataColumn.ExtendedProperties.Add(key, oldColumn.ExtendedProperties[key]);
                    }
                }
                
                dataTable.Columns.Add(dataColumn);
                copiedColumns.Add(dataColumn);
            }

            return copiedColumns;
        }

        private static ILogEvent[] EventsReadOnlyCache = new ILogEvent[0];
        internal static IEnumerable<ILogEvent> GetAllEventsReadOnly(this Cluster cluster,
                                                                        bool useCache = true,
                                                                        DSEDiagnosticLibrary.LogCassandraEvent.ElementCreationTypes elementCreationTypes = DSEDiagnosticLibrary.LogCassandraEvent.ElementCreationTypes.DCNodeKSDDLTypeClassOnly)
        {
            if(useCache)
            {
                if(EventsReadOnlyCache.Length == 0)
                {
                    lock (EventsReadOnlyCache)
                    {
                        if (EventsReadOnlyCache.Length == 0)
                            EventsReadOnlyCache = cluster.Nodes
                                                        .SelectMany(d => ((DSEDiagnosticLibrary.Node)d).LogEventsRead(elementCreationTypes, false)
                                                                            .ToArray()
                                                                            .Select(v => v.Value))
                                                        .ToArray();
                    }
                }

                return EventsReadOnlyCache;
            }

            return cluster.Nodes
                    .SelectMany(d => ((DSEDiagnosticLibrary.Node)d).LogEventsRead(elementCreationTypes, false)
                                        .ToArray()
                                        .Select(v => v.Value))
                    .ToArray();
        }
    }
}
