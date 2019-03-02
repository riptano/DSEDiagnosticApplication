using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;
using Common;
using OfficeOpenXml;
using DataTableToExcel;
using OfficeOpenXml.Table;
using DT = DSEDiagnosticToDataTable;

namespace DSEDiagtnosticToExcel
{
    public sealed class TaggedItemsExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.TaggedItems;

        public TaggedItemsExcel(DataTable keyspaceDataTable,
                                    IFilePath excelTargetWorkbook,
                                    IFilePath excelTemplateWorkbook,
                                    string worksheetName,
                                    bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        { }

        public TaggedItemsExcel(DataTable keyspaceDataTable,
                                    IFilePath excelTargetWorkbook,
                                    IFilePath excelTemplateWorkbook = null)
            : this(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, null, true)
        { }

        const string ReadLatencyTable = "Read Latency";
        const string WriteLatencyTable = "Write Latency";
        const string SSTablesTables = "SSTables";
        const string TombstonesTable = "Tombstones";
        const string LargePartitionsTable = "Large Partition Wide Rows";

        public struct Columns
        {
            public const string KSCSType = "Keyspace/Compaction/Type";
            public const string BaseTableFactor = "Base Table Factor";
        }
        
        DataColumn[] AddDCColumns(DataTable targetTable, DataTable sourceTable)
        {           
            var dataCenters = (from dataRow in sourceTable.AsEnumerable()
                               let dataCenterName = dataRow.Field<string>(DT.ColumnNames.DataCenter)
                               where !string.IsNullOrEmpty(dataCenterName)
                               select dataCenterName)
                               .Distinct()
                               .OrderBy(i => i);
            var dataCenterAbbr = dataCenters.Select(n => DSEDiagnosticLibrary.StringHelpers.AbbrName(n));

            var dcDataColumns = DSEDiagnosticToDataTable.MiscHelpers.AddColumnsToTable(targetTable, dataCenters, typeof(decimal)).ToArray();

            for(int nidx = 0; nidx < dcDataColumns.Length; ++nidx)
            {
                dcDataColumns[nidx]
                    .SetComment(dataCenters.ElementAt(nidx))
                    .SetNumericFormat("#,###,###,##0.00")
                    .SetCaption(dataCenterAbbr.ElementAt(nidx))
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                    .AllowDBNull = true;
            }

            return dcDataColumns;
        }


        public DataTable CreateInitializationReadLatencyTable(DataTable sourceTable)
        {
            var dtOverview = new DataTable(ReadLatencyTable, DSEDiagnosticToDataTable.TableNames.Namespace);

            if (this.DataTable.Columns.Contains(DSEDiagnosticToDataTable.ColumnNames.SessionId)) dtOverview.Columns.Add(DSEDiagnosticToDataTable.ColumnNames.SessionId, typeof(Guid));

            dtOverview.Columns.Add(Columns.KSCSType, typeof(string)).SetDBNull();
            dtOverview.Columns.Add(DSEDiagnosticToDataTable.ColumnNames.Table, typeof(string)).SetDBNull();

            dtOverview.SetGroupHeader("DataCenters", -1, true,
                this.AddDCColumns(dtOverview, sourceTable)
            );

            dtOverview.SetGroupHeader("Read (ms)", -1, true,               
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.ReadMax, typeof(decimal))
                    .SetNumericFormat("#,###,###,##0.000")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonReadLatencyMax)
                    .SetDBNull()
                    .SetCaption("Max"),
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.ReadMin, typeof(decimal))
                    .SetNumericFormat("#,###,###,##0.000")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonReadLatencyMin)
                    .SetDBNull()
                    .SetCaption("Min"),
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.ReadAvg, typeof(decimal))
                    .SetNumericFormat("#,###,###,##0.000")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonReadLatencyAvg)
                    .SetDBNull()
                    .SetCaption("Avg"),
                 dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.ReadPercent, typeof(decimal))
                    .SetDBNull()
                    .SetNumericFormat("##0.00%")
                    .SetCaption("Percent")
                    );            

            dtOverview.SetGroupHeader(string.Empty, -1, false,
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.KeysPercent, typeof(decimal))
                    .SetNumericFormat("##0.00%")
                    .SetDBNull(),
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.StoragePercent, typeof(decimal))
                    .SetNumericFormat("##0.00%")
                    .SetDBNull());

            dtOverview.SetGroupHeader("Informational", -2, true,
               dtOverview.SetGroupHeader("Write", -1, true,
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.WriteFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull()
                        .SetCaption("Factor"),
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.WritePercent, typeof(decimal))
                        .SetDBNull()
                        .SetNumericFormat("##0.00%")
                        .SetCaption("Percent")
                        ));

            dtOverview.SetGroupHeader("Influencer", -2, true,
               dtOverview.SetGroupHeader("SSTable", -1, true,
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.SSTablesFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull()
                        .SetCaption("Factor"),
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.SSTablePercent, typeof(decimal))
                        .SetDBNull()
                        .SetNumericFormat("##0.00%")
                        .SetCaption("Percent")
                        ),
               dtOverview.SetGroupHeader(string.Empty, -1, false,
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull(),

                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.PartitionSizeFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull(),

                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.SecondaryIndexes, typeof(int))
                        .SetNumericFormat("##0")
                        .SetDBNull(),
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.MaterializedViews, typeof(int))
                        .SetNumericFormat("##0")
                        .SetDBNull()),

                dtOverview.SetGroupHeader("Common Partition Keys", -1, true,
                   dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.CommonKey, typeof(string))
                        .SetDBNull()
                        .SetCaption("Key"),
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.CommonKeyFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull()
                        .SetCaption("Factor")
                    ),
                dtOverview.SetGroupHeader(string.Empty, -1, false,
                    dtOverview.Columns.Add(Columns.BaseTableFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull()
                        )
                );           

            decimal? WeightAvg(IEnumerable<DataRow> dataRows, string columnName)
            {
                var factorValues = dataRows.Where(r => !r.IsNull(columnName)).Select(r => r.Field<decimal>(columnName)).ToArray();

                if (factorValues.Length == 0) return null;

                var aggregate = factorValues.Sum();
                var maxFactor = factorValues.Max();

                return (aggregate + maxFactor) / ((decimal) factorValues.Length + 1m);
            }

            decimal? Sum(IEnumerable<DataRow> dataRows, string columnName)
            {
                var factorValue = dataRows.Where(r => !r.IsNull(columnName))
                                            .Select(r => r.Field<decimal>(columnName))
                                            .DefaultIfEmpty()
                                            .Sum();

                if (factorValue == 0) return null;

                return factorValue;
            }

            var taggedItems = from dataRow in sourceTable.AsEnumerable()
                              where !dataRow.IsNull(DT.TaggedItemsDataTable.Columns.ReadFactor)
                              let keySpace = dataRow.Field<string>(DT.ColumnNames.KeySpace)
                              let compactionType = dataRow.Field<string>(DT.TaggedItemsDataTable.Columns.CompactionStrategy)
                              let cqlType = dataRow.Field<string>(DT.TaggedItemsDataTable.Columns.CQLType)
                              let tableName = dataRow.Field<string>(DT.ColumnNames.Table)
                              group dataRow by new { keySpace, compactionType, cqlType, tableName } into grp
                              let dcfactors = grp.Select(i => new
                              {
                                  DC = i.Field<string>(DT.ColumnNames.DataCenter),
                                  Factor = i.Field<decimal>(DT.TaggedItemsDataTable.Columns.ReadFactor)
                              })
                              where dcfactors.Any(i => i.Factor >= Properties.Settings.Default.TriggerReadFactor)
                              orderby grp.Key.keySpace ascending, grp.Key.compactionType ascending, grp.Key.cqlType ascending, grp.Key.tableName ascending
                              select new
                              {
                                  Keyspace = grp.Key.keySpace,
                                  Compaction = grp.Key.compactionType,
                                  CQLType = grp.Key.cqlType,
                                  Table = grp.Key.tableName,                                  
                                  DCFactor = dcfactors,
                                  ReadPercent = grp.Sum(r => r.Field<decimal>(DT.TaggedItemsDataTable.Columns.ReadPercent)),
                                  ReadMax = grp.Max(r => r.Field<decimal>(DT.TaggedItemsDataTable.Columns.ReadMax)),
                                  ReadMin = grp.Where(r => !r.IsNull(DT.TaggedItemsDataTable.Columns.ReadMin))
                                                .ActionIfNotEmpty(i => i.Min(r => r.Field<decimal>(DT.TaggedItemsDataTable.Columns.ReadMin)), default(decimal?)),
                                  ReadAvg = grp.Where(r => !r.IsNull(DT.TaggedItemsDataTable.Columns.ReadPercent))
                                                .ActionIfNotEmpty(i => i.Min(r => r.Field<decimal>(DT.TaggedItemsDataTable.Columns.ReadPercent)), default(decimal?)),
                                  WriteFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.WriteFactor),
                                  WritePercent = Sum(grp, DT.TaggedItemsDataTable.Columns.WritePercent),
                                  KeyPercent = Sum(grp, DT.TaggedItemsDataTable.Columns.KeysPercent),
                                  SSTableFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.SSTablesFactor),
                                  SSTablePercent = Sum(grp, DT.TaggedItemsDataTable.Columns.SSTablePercent),
                                  StoragePercent = Sum(grp, DT.TaggedItemsDataTable.Columns.StoragePercent),
                                  TombstoneFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor),
                                  PartitionFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.PartitionSizeFactor),
                                  SecondaryIndexes = grp.First().Field<int?>(DT.TaggedItemsDataTable.Columns.SecondaryIndexes),
                                  Views = grp.First().Field<int?>(DT.TaggedItemsDataTable.Columns.MaterializedViews),
                                  CommonKey = grp.First().Field<string>(DT.TaggedItemsDataTable.Columns.CommonKey),
                                  CommonKeyFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.CommonKeyFactor),
                                  BaseTableFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.BaseTableWeightedFactorMax)
                              };

            DataRow newDataRow;
            string lstKeySpace = null;
            string lstCompaction = null;
            string lstType = null;

            foreach (var factors in taggedItems)
            {
                newDataRow = dtOverview.NewRow();

                if(lstKeySpace != factors.Keyspace)
                {
                    var ksRow = dtOverview.NewRow();
                    ksRow.SetField(Columns.KSCSType, lstKeySpace = factors.Keyspace);                    
                    dtOverview.Rows.Add(ksRow);
                    lstCompaction = lstType = null;
                }
                if (lstCompaction != factors.Compaction)
                {
                    var csRow = dtOverview.NewRow();
                    csRow.SetField(Columns.KSCSType, "   " + (lstCompaction = factors.Compaction));
                    dtOverview.Rows.Add(csRow);
                    lstType = null;
                }
                if (lstType != factors.CQLType)
                {
                    var csRow = dtOverview.NewRow();
                    csRow.SetField(Columns.KSCSType, "      " + (lstType = factors.CQLType));
                    dtOverview.Rows.Add(csRow);
                }

                newDataRow.SetField(DSEDiagnosticToDataTable.ColumnNames.Table, factors.Table);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.ReadPercent, factors.ReadPercent);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.ReadMax, factors.ReadMax);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.ReadMin, factors.ReadMin);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.ReadAvg, factors.ReadAvg);

                foreach (var dcFactor in factors.DCFactor)
                {
                    newDataRow.SetField(dcFactor.DC, dcFactor.Factor);
                }
                
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.WriteFactor, factors.WriteFactor);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.WritePercent, factors.WritePercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.KeysPercent, factors.KeyPercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.SSTablesFactor, factors.SSTableFactor);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.SSTablePercent, factors.SSTablePercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.StoragePercent, factors.StoragePercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor, factors.TombstoneFactor);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.PartitionSizeFactor, factors.PartitionFactor);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.SecondaryIndexes, factors.SecondaryIndexes);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.MaterializedViews, factors.Views);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.CommonKey, factors.CommonKey);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.CommonKeyFactor, factors.CommonKeyFactor);

                newDataRow.SetField(Columns.BaseTableFactor, factors.BaseTableFactor);

                dtOverview.Rows.Add(newDataRow);
            }


            return dtOverview;
        }

        public DataTable CreateInitializationWriteLatencyTable(DataTable sourceTable)
        {
            var dtOverview = new DataTable(WriteLatencyTable, DSEDiagnosticToDataTable.TableNames.Namespace);

            if (this.DataTable.Columns.Contains(DSEDiagnosticToDataTable.ColumnNames.SessionId)) dtOverview.Columns.Add(DSEDiagnosticToDataTable.ColumnNames.SessionId, typeof(Guid));

            dtOverview.Columns.Add(Columns.KSCSType, typeof(string)).SetDBNull();
            dtOverview.Columns.Add(DSEDiagnosticToDataTable.ColumnNames.Table, typeof(string)).SetDBNull();

            dtOverview.SetGroupHeader("DataCenters", -1, true,
                this.AddDCColumns(dtOverview, sourceTable)
            );

            dtOverview.SetGroupHeader("Write (ms)", -1, true,
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.WriteMax, typeof(decimal))
                    .SetNumericFormat("#,###,###,##0.000")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonWriteLatencyMax)
                    .SetDBNull()
                    .SetCaption("Max"),
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.WriteMin, typeof(decimal))
                    .SetNumericFormat("#,###,###,##0.000")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonWriteLatencyAvg)
                    .SetDBNull()
                    .SetCaption("Min"),
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.WriteAvg, typeof(decimal))
                    .SetNumericFormat("#,###,###,##0.000")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonWriteLatencyAvg)
                    .SetDBNull()
                    .SetCaption("Avg"),
                 dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.WritePercent, typeof(decimal))
                    .SetDBNull()
                    .SetNumericFormat("##0.00%")
                    .SetCaption("Percent")
                    );

            dtOverview.SetGroupHeader(string.Empty, -1, false,
                        dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.KeysPercent, typeof(decimal))
                            .SetNumericFormat("##0.00%")
                            .SetDBNull(),
                        dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.StoragePercent, typeof(decimal))
                           .SetNumericFormat("##0.00%")
                           .SetDBNull());

            dtOverview.SetGroupHeader("Influencer", -2, true,
                dtOverview.SetGroupHeader(string.Empty, -1, true,
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.PartitionSizeFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull(),
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.SecondaryIndexes, typeof(int))
                        .SetNumericFormat("##0")
                        .SetDBNull(),
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.MaterializedViews, typeof(int))
                        .SetNumericFormat("##0")
                        .SetDBNull()),

                    dtOverview.SetGroupHeader("Common Partition Keys", -1, true,
                       dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.CommonKey, typeof(string))
                            .SetDBNull()
                            .SetCaption("Key"),
                        dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.CommonKeyFactor, typeof(decimal))
                            .SetNumericFormat("#,###,###,##0.00")
                            .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                            .SetDBNull()
                            .SetCaption("Factor")
                        ));               

            dtOverview.SetGroupHeader("Informational", -2, true,
                dtOverview.SetGroupHeader("Read", -1, true,
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.ReadFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull()
                        .SetCaption("Factor"),
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.ReadPercent, typeof(decimal))
                        .SetDBNull()
                        .SetNumericFormat("##0.00%")
                        .SetCaption("Percent")
                        ),

                dtOverview.SetGroupHeader("SSTable", -1, true,
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.SSTablesFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull()
                        .SetCaption("Factor"),
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.SSTablePercent, typeof(decimal))
                        .SetDBNull()
                        .SetNumericFormat("##0.00%")
                        .SetCaption("Percent")
                        ),
                dtOverview.SetGroupHeader(string.Empty, -1, false,
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull(),
                    dtOverview.Columns.Add(Columns.BaseTableFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull())
            );

            decimal? WeightAvg(IEnumerable<DataRow> dataRows, string columnName)
            {
                var factorValues = dataRows.Where(r => !r.IsNull(columnName)).Select(r => r.Field<decimal>(columnName)).ToArray();

                if (factorValues.Length == 0) return null;

                var aggregate = factorValues.Sum();
                var maxFactor = factorValues.Max();

                return (aggregate + maxFactor) / ((decimal)factorValues.Length + 1m);
            }

            decimal? Sum(IEnumerable<DataRow> dataRows, string columnName)
            {
                var factorValue = dataRows.Where(r => !r.IsNull(columnName))
                                            .Select(r => r.Field<decimal>(columnName))
                                            .DefaultIfEmpty()
                                            .Sum();

                if (factorValue == 0) return null;

                return factorValue;
            }

            var taggedItems = from dataRow in sourceTable.AsEnumerable()
                              where !dataRow.IsNull(DT.TaggedItemsDataTable.Columns.WriteFactor)
                              let keySpace = dataRow.Field<string>(DT.ColumnNames.KeySpace)
                              let compactionType = dataRow.Field<string>(DT.TaggedItemsDataTable.Columns.CompactionStrategy)
                              let cqlType = dataRow.Field<string>(DT.TaggedItemsDataTable.Columns.CQLType)
                              let tableName = dataRow.Field<string>(DT.ColumnNames.Table)
                              group dataRow by new { keySpace, compactionType, cqlType, tableName } into grp
                              let dcfactors = grp.Select(i => new
                              {
                                  DC = i.Field<string>(DT.ColumnNames.DataCenter),
                                  Factor = i.Field<decimal>(DT.TaggedItemsDataTable.Columns.WriteFactor)
                              })
                              where dcfactors.Any(i => i.Factor >= Properties.Settings.Default.TriggerWriteFactor)
                              orderby grp.Key.keySpace ascending, grp.Key.compactionType ascending, grp.Key.cqlType ascending, grp.Key.tableName ascending
                              select new
                              {
                                  Keyspace = grp.Key.keySpace,
                                  Compaction = grp.Key.compactionType,
                                  CQLType = grp.Key.cqlType,
                                  Table = grp.Key.tableName,
                                  DCFactor = dcfactors,
                                  WritePercent = grp.Sum(r => r.Field<decimal>(DT.TaggedItemsDataTable.Columns.WritePercent)),
                                  WriteMax = grp.Max(r => r.Field<decimal>(DT.TaggedItemsDataTable.Columns.WriteMax)),                                  
                                  WriteMin = grp.Where(r => !r.IsNull(DT.TaggedItemsDataTable.Columns.WriteMin))
                                                .ActionIfNotEmpty(i => i.Min(r => r.Field<decimal>(DT.TaggedItemsDataTable.Columns.WriteMin)), default(decimal?)),
                                  WriteAvg = grp.Where(r => !r.IsNull(DT.TaggedItemsDataTable.Columns.WritePercent))
                                                .ActionIfNotEmpty(i => i.Min(r => r.Field<decimal>(DT.TaggedItemsDataTable.Columns.WritePercent)), default(decimal?)),
                                  ReadFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.ReadFactor),
                                  ReadPercent = Sum(grp, DT.TaggedItemsDataTable.Columns.ReadPercent),
                                  KeyPercent = Sum(grp, DT.TaggedItemsDataTable.Columns.KeysPercent),
                                  SSTableFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.SSTablesFactor),
                                  SSTablePercent = Sum(grp, DT.TaggedItemsDataTable.Columns.SSTablePercent),
                                  StoragePercent = Sum(grp, DT.TaggedItemsDataTable.Columns.StoragePercent),
                                  TombstoneFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor),
                                  PartitionFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.PartitionSizeFactor),
                                  SecondaryIndexes = grp.First().Field<int?>(DT.TaggedItemsDataTable.Columns.SecondaryIndexes),
                                  Views = grp.First().Field<int?>(DT.TaggedItemsDataTable.Columns.MaterializedViews),
                                  CommonKey = grp.First().Field<string>(DT.TaggedItemsDataTable.Columns.CommonKey),
                                  CommonKeyFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.CommonKeyFactor),
                                  BaseTableFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.BaseTableWeightedFactorMax)
                              };

            DataRow newDataRow;
            string lstKeySpace = null;
            string lstCompaction = null;
            string lstType = null;

            foreach (var factors in taggedItems)
            {
                newDataRow = dtOverview.NewRow();

                if (lstKeySpace != factors.Keyspace)
                {
                    var ksRow = dtOverview.NewRow();
                    ksRow.SetField(Columns.KSCSType, lstKeySpace = factors.Keyspace);
                    dtOverview.Rows.Add(ksRow);
                    lstCompaction = lstType = null;
                }
                if (lstCompaction != factors.Compaction)
                {
                    var csRow = dtOverview.NewRow();
                    csRow.SetField(Columns.KSCSType, "   " + (lstCompaction = factors.Compaction));
                    dtOverview.Rows.Add(csRow);
                    lstType = null;
                }
                if (lstType != factors.CQLType)
                {
                    var csRow = dtOverview.NewRow();
                    csRow.SetField(Columns.KSCSType, "      " + (lstType = factors.CQLType));
                    dtOverview.Rows.Add(csRow);
                }

                newDataRow.SetField(DSEDiagnosticToDataTable.ColumnNames.Table, factors.Table);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.WritePercent, factors.WritePercent);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.WriteMax, factors.WriteMax);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.WriteMin, factors.WriteMin);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.WriteAvg, factors.WriteAvg);

                foreach (var dcFactor in factors.DCFactor)
                {
                    newDataRow.SetField(dcFactor.DC, dcFactor.Factor);
                }

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.ReadFactor, factors.ReadFactor);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.ReadPercent, factors.ReadPercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.KeysPercent, factors.KeyPercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.SSTablesFactor, factors.SSTableFactor);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.SSTablePercent, factors.SSTablePercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.StoragePercent, factors.StoragePercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor, factors.TombstoneFactor);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.PartitionSizeFactor, factors.PartitionFactor);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.SecondaryIndexes, factors.SecondaryIndexes);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.MaterializedViews, factors.Views);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.CommonKey, factors.CommonKey);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.CommonKeyFactor, factors.CommonKeyFactor);

                newDataRow.SetField(Columns.BaseTableFactor, factors.BaseTableFactor);

                dtOverview.Rows.Add(newDataRow);
            }


            return dtOverview;
        }

        public DataTable CreateInitializationSSTablesLatencyTable(DataTable sourceTable)
        {
            var dtOverview = new DataTable(SSTablesTables, DSEDiagnosticToDataTable.TableNames.Namespace);

            if (this.DataTable.Columns.Contains(DSEDiagnosticToDataTable.ColumnNames.SessionId)) dtOverview.Columns.Add(DSEDiagnosticToDataTable.ColumnNames.SessionId, typeof(Guid));

            dtOverview.Columns.Add(Columns.KSCSType, typeof(string)).SetDBNull();
            dtOverview.Columns.Add(DSEDiagnosticToDataTable.ColumnNames.Table, typeof(string)).SetDBNull();

            dtOverview.SetGroupHeader("DataCenters", -1, true,
                this.AddDCColumns(dtOverview, sourceTable)
            );

            dtOverview.SetGroupHeader("SSTables (count)", -1, true,
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.SSTablesMax, typeof(long))
                    .SetNumericFormat("#,###,###,##0")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonSSTablesTable)
                    .SetDBNull()
                    .SetCaption("Max"),
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.SSTablesMin, typeof(long))
                    .SetNumericFormat("#,###,###,##0")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonSSTablesTable)
                    .SetDBNull()
                    .SetCaption("Min"),
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.SSTablesAvg, typeof(decimal))
                    .SetNumericFormat("#,###,###,##0.000")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonSSTablesTable)
                    .SetDBNull()
                    .SetCaption("Avg"),
                 dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.SSTablePercent, typeof(decimal))
                    .SetDBNull()
                    .SetNumericFormat("##0.00%")
                    .SetCaption("Percent")
                    );

            dtOverview.SetGroupHeader(string.Empty, -1, false,
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.KeysPercent, typeof(decimal))
                    .SetNumericFormat("##0.00%")
                    .SetDBNull(),
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.StoragePercent, typeof(decimal))
                    .SetNumericFormat("##0.00%")
                    .SetDBNull());

            dtOverview.SetGroupHeader("Informational", -2, true,
               dtOverview.SetGroupHeader("Read", -1, true,
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.ReadFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull()
                        .SetCaption("Factor"),
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.ReadPercent, typeof(decimal))
                        .SetDBNull()
                        .SetNumericFormat("##0.00%")
                        .SetCaption("Percent")
                        ),            
                    dtOverview.SetGroupHeader("Write", -1, true,
                        dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.WriteFactor, typeof(decimal))
                            .SetNumericFormat("#,###,###,##0.00")
                            .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                            .SetDBNull()
                            .SetCaption("Factor"),
                        dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.WritePercent, typeof(decimal))
                            .SetDBNull()
                            .SetNumericFormat("##0.00%")
                            .SetCaption("Percent")
                            ),
                    dtOverview.SetGroupHeader(string.Empty, -1, false,
                        dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor, typeof(decimal))
                            .SetNumericFormat("#,###,###,##0.00")
                            .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                            .SetDBNull(),

                        dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.PartitionSizeFactor, typeof(decimal))
                             .SetNumericFormat("#,###,###,##0.00")
                            .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                            .SetDBNull(),
                        dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.SecondaryIndexes, typeof(int))
                            .SetNumericFormat("##0")
                            .SetDBNull(),
                        dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.MaterializedViews, typeof(int))
                            .SetNumericFormat("##0")
                            .SetDBNull()),

                    dtOverview.SetGroupHeader("Common Partition Keys", -1, true,
                       dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.CommonKey, typeof(string))
                            .SetDBNull()
                            .SetCaption("Key"),
                        dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.CommonKeyFactor, typeof(decimal))
                            .SetNumericFormat("#,###,###,##0.00")
                            .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                            .SetDBNull()
                            .SetCaption("Factor")
                        ),
                    dtOverview.SetGroupHeader(string.Empty, -1, false,
                        dtOverview.Columns.Add(Columns.BaseTableFactor, typeof(decimal))
                            .SetNumericFormat("#,###,###,##0.00")
                            .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                            .SetDBNull())
            );

            decimal? WeightAvg(IEnumerable<DataRow> dataRows, string columnName)
            {
                var factorValues = dataRows.Where(r => !r.IsNull(columnName)).Select(r => r.Field<decimal>(columnName)).ToArray();

                if (factorValues.Length == 0) return null;

                var aggregate = factorValues.Sum();
                var maxFactor = factorValues.Max();

                return (aggregate + maxFactor) / ((decimal)factorValues.Length + 1m);
            }

            decimal? Sum(IEnumerable<DataRow> dataRows, string columnName)
            {
                var factorValue = dataRows.Where(r => !r.IsNull(columnName))
                                            .Select(r => r.Field<decimal>(columnName))
                                            .DefaultIfEmpty()
                                            .Sum();

                if (factorValue == 0) return null;

                return factorValue;
            }

            var taggedItems = from dataRow in sourceTable.AsEnumerable()
                              where !dataRow.IsNull(DT.TaggedItemsDataTable.Columns.SSTablesFactor)
                              let keySpace = dataRow.Field<string>(DT.ColumnNames.KeySpace)
                              let compactionType = dataRow.Field<string>(DT.TaggedItemsDataTable.Columns.CompactionStrategy)
                              let cqlType = dataRow.Field<string>(DT.TaggedItemsDataTable.Columns.CQLType)
                              let tableName = dataRow.Field<string>(DT.ColumnNames.Table)
                              group dataRow by new { keySpace, compactionType, cqlType, tableName } into grp
                              let dcfactors = grp.Select(i => new
                              {
                                  DC = i.Field<string>(DT.ColumnNames.DataCenter),
                                  Factor = i.Field<decimal>(DT.TaggedItemsDataTable.Columns.SSTablesFactor)
                              })
                              where dcfactors.Any(i => i.Factor >= Properties.Settings.Default.TriggerSSTablesFactor)
                              orderby grp.Key.keySpace ascending, grp.Key.compactionType ascending, grp.Key.cqlType ascending, grp.Key.tableName ascending
                              select new
                              {
                                  Keyspace = grp.Key.keySpace,
                                  Compaction = grp.Key.compactionType,
                                  CQLType = grp.Key.cqlType,
                                  Table = grp.Key.tableName,
                                  DCFactor = dcfactors,
                                  SSTablePercent = grp.Sum(r => r.Field<decimal>(DT.TaggedItemsDataTable.Columns.SSTablePercent)),
                                  SSTableMax = grp.Max(r => r.Field<long>(DT.TaggedItemsDataTable.Columns.SSTablesMax)),                                 
                                  SSTableMin = grp.Where(r => !r.IsNull(DT.TaggedItemsDataTable.Columns.SSTablesMin))
                                                .ActionIfNotEmpty(i => i.Min(r => r.Field<long>(DT.TaggedItemsDataTable.Columns.SSTablesMin)), default(long?)),
                                  SSTableAvg = grp.Where(r => !r.IsNull(DT.TaggedItemsDataTable.Columns.SSTablePercent))
                                                .ActionIfNotEmpty(i => i.Min(r => r.Field<decimal>(DT.TaggedItemsDataTable.Columns.SSTablePercent)), default(decimal?)),
                                  ReadFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.ReadFactor),
                                  ReadPercent = Sum(grp, DT.TaggedItemsDataTable.Columns.ReadPercent),
                                  KeyPercent = Sum(grp, DT.TaggedItemsDataTable.Columns.KeysPercent),
                                  WriteFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.WriteFactor),
                                  WritePercent = Sum(grp, DT.TaggedItemsDataTable.Columns.WritePercent),
                                  StoragePercent = Sum(grp, DT.TaggedItemsDataTable.Columns.StoragePercent),
                                  TombstoneFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor),
                                  PartitionFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.PartitionSizeFactor),
                                  SecondaryIndexes = grp.First().Field<int?>(DT.TaggedItemsDataTable.Columns.SecondaryIndexes),
                                  Views = grp.First().Field<int?>(DT.TaggedItemsDataTable.Columns.MaterializedViews),
                                  CommonKey = grp.First().Field<string>(DT.TaggedItemsDataTable.Columns.CommonKey),
                                  CommonKeyFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.CommonKeyFactor),
                                  BaseTableFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.BaseTableWeightedFactorMax)
                              };

            DataRow newDataRow;
            string lstKeySpace = null;
            string lstCompaction = null;
            string lstType = null;

            foreach (var factors in taggedItems)
            {
                newDataRow = dtOverview.NewRow();

                if (lstKeySpace != factors.Keyspace)
                {
                    var ksRow = dtOverview.NewRow();
                    ksRow.SetField(Columns.KSCSType, lstKeySpace = factors.Keyspace);
                    dtOverview.Rows.Add(ksRow);
                    lstCompaction = lstType = null;
                }
                if (lstCompaction != factors.Compaction)
                {
                    var csRow = dtOverview.NewRow();
                    csRow.SetField(Columns.KSCSType, "   " + (lstCompaction = factors.Compaction));
                    dtOverview.Rows.Add(csRow);
                    lstType = null;
                }
                if (lstType != factors.CQLType)
                {
                    var csRow = dtOverview.NewRow();
                    csRow.SetField(Columns.KSCSType, "      " + (lstType = factors.CQLType));
                    dtOverview.Rows.Add(csRow);
                }

                newDataRow.SetField(DSEDiagnosticToDataTable.ColumnNames.Table, factors.Table);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.SSTablePercent, factors.SSTablePercent);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.SSTablesMax, factors.SSTableMax);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.SSTablesMin, factors.SSTableMin);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.SSTablesAvg, factors.SSTableAvg);

                foreach (var dcFactor in factors.DCFactor)
                {
                    newDataRow.SetField(dcFactor.DC, dcFactor.Factor);
                }

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.ReadFactor, factors.ReadFactor);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.ReadPercent, factors.ReadPercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.KeysPercent, factors.KeyPercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.WriteFactor, factors.WriteFactor);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.WritePercent, factors.WritePercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.StoragePercent, factors.StoragePercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor, factors.TombstoneFactor);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.PartitionSizeFactor, factors.PartitionFactor);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.SecondaryIndexes, factors.SecondaryIndexes);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.MaterializedViews, factors.Views);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.CommonKey, factors.CommonKey);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.CommonKeyFactor, factors.CommonKeyFactor);

                newDataRow.SetField(Columns.BaseTableFactor, factors.BaseTableFactor);

                dtOverview.Rows.Add(newDataRow);
            }


            return dtOverview;
        }

        public DataTable CreateInitializationTombstonesTable(DataTable sourceTable)
        {
            var dtOverview = new DataTable(TombstonesTable, DSEDiagnosticToDataTable.TableNames.Namespace);

            if (this.DataTable.Columns.Contains(DSEDiagnosticToDataTable.ColumnNames.SessionId)) dtOverview.Columns.Add(DSEDiagnosticToDataTable.ColumnNames.SessionId, typeof(Guid));

            dtOverview.Columns.Add(Columns.KSCSType, typeof(string)).SetDBNull();
            dtOverview.Columns.Add(DSEDiagnosticToDataTable.ColumnNames.Table, typeof(string)).SetDBNull();

            dtOverview.SetGroupHeader("DataCenters", -1, true,
                this.AddDCColumns(dtOverview, sourceTable)
            );

            dtOverview.SetGroupHeader("Tombstone/Live Ratio", -1, true,
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.TombstoneRatioMax, typeof(decimal))
                    .SetNumericFormat("##0.00%")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonTombstoneLiveRatio)
                    .SetDBNull()
                    .SetCaption("Max"),
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.TombstoneRatioMin, typeof(decimal))
                    .SetNumericFormat("##0.00%")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonTombstoneLiveRatio)
                    .SetDBNull()
                    .SetCaption("Min"),
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.TombstoneRatioAvg, typeof(decimal))
                    .SetNumericFormat("##0.00%")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonTombstoneLiveRatio)
                    .SetDBNull()
                    .SetCaption("Avg"),
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.TombstonesRead, typeof(long))
                    .SetNumericFormat("#,###,###,##0")
                    .SetDBNull()
                    .SetCaption("Read"),
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.LiveRead, typeof(long))
                    .SetNumericFormat("#,###,###,##0")
                    .SetDBNull()
                    .SetCaption("Live Read")                
                    );

            dtOverview.SetGroupHeader(string.Empty, -1, false,
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.KeysPercent, typeof(decimal))
                    .SetNumericFormat("##0.00%")
                    .SetDBNull(),
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.StoragePercent, typeof(decimal))
                    .SetNumericFormat("##0.00%")
                    .SetDBNull());

            dtOverview.SetGroupHeader("Informational", -2, true,
                dtOverview.SetGroupHeader("Read", -1, true,
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.ReadFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull()
                        .SetCaption("Factor"),
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.ReadPercent, typeof(decimal))
                        .SetDBNull()
                        .SetNumericFormat("##0.00%")
                        .SetCaption("Percent")
                        ),
                
                dtOverview.SetGroupHeader("Write", -1, true,
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.WriteFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull()
                        .SetCaption("Factor"),
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.WritePercent, typeof(decimal))
                        .SetDBNull()
                        .SetNumericFormat("##0.00%")
                        .SetCaption("Percent")
                        ),
                 dtOverview.SetGroupHeader(string.Empty, -1, false,
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.SSTablesFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull(),

                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.PartitionSizeFactor, typeof(decimal))
                       .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull(),

                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.SecondaryIndexes, typeof(int))
                        .SetNumericFormat("##0")
                        .SetDBNull(),
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.MaterializedViews, typeof(int))
                        .SetNumericFormat("##0")
                        .SetDBNull()),

                dtOverview.SetGroupHeader("Common Partition Keys", -1, true,
                   dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.CommonKey, typeof(string))
                        .SetDBNull()
                        .SetCaption("Key"),
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.CommonKeyFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull()
                        .SetCaption("Factor")
                    ),
                dtOverview.SetGroupHeader(string.Empty, -1, false,
                    dtOverview.Columns.Add(Columns.BaseTableFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull())
            );

            decimal? WeightAvg(IEnumerable<DataRow> dataRows, string columnName)
            {
                var factorValues = dataRows.Where(r => !r.IsNull(columnName)).Select(r => r.Field<decimal>(columnName)).ToArray();

                if (factorValues.Length == 0) return null;

                var aggregate = factorValues.Sum();
                var maxFactor = factorValues.Max();

                return (aggregate + maxFactor) / ((decimal)factorValues.Length + 1m);
            }

            decimal? Sum(IEnumerable<DataRow> dataRows, string columnName)
            {
                var factorValue = dataRows.Where(r => !r.IsNull(columnName))
                                            .Select(r => r.Field<decimal>(columnName))
                                            .DefaultIfEmpty()
                                            .Sum();

                if (factorValue == 0) return null;

                return factorValue;
            }

            long? SumL(IEnumerable<DataRow> dataRows, string columnName)
            {
                var factorValue = dataRows.Where(r => !r.IsNull(columnName))
                                            .Select(r => r.Field<long>(columnName))
                                            .DefaultIfEmpty()
                                            .Sum();

                if (factorValue == 0) return null;

                return factorValue;
            }

            var taggedItems = from dataRow in sourceTable.AsEnumerable()
                              where !dataRow.IsNull(DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor)
                              let keySpace = dataRow.Field<string>(DT.ColumnNames.KeySpace)
                              let compactionType = dataRow.Field<string>(DT.TaggedItemsDataTable.Columns.CompactionStrategy)
                              let cqlType = dataRow.Field<string>(DT.TaggedItemsDataTable.Columns.CQLType)
                              let tableName = dataRow.Field<string>(DT.ColumnNames.Table)
                              group dataRow by new { keySpace, compactionType, cqlType, tableName } into grp
                              let dcfactors = grp.Select(i => new
                              {
                                  DC = i.Field<string>(DT.ColumnNames.DataCenter),
                                  Factor = i.Field<decimal>(DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor)
                              })
                              where dcfactors.Any(i => i.Factor >= Properties.Settings.Default.TriggerTombstoneRatioFactor)
                              orderby grp.Key.keySpace ascending, grp.Key.compactionType ascending, grp.Key.cqlType ascending, grp.Key.tableName ascending
                              select new
                              {
                                  Keyspace = grp.Key.keySpace,
                                  Compaction = grp.Key.compactionType,
                                  CQLType = grp.Key.cqlType,
                                  Table = grp.Key.tableName,
                                  DCFactor = dcfactors,
                                  TRMax = grp.Max(r => r.Field<decimal>(DT.TaggedItemsDataTable.Columns.TombstoneRatioMax)),                                 
                                  TRMin = grp.Where(r => !r.IsNull(DT.TaggedItemsDataTable.Columns.TombstoneRatioMin))
                                                .ActionIfNotEmpty(i => i.Min(r => r.Field<decimal>(DT.TaggedItemsDataTable.Columns.TombstoneRatioMin)), default(decimal?)),
                                  TRAvg = grp.Where(r => !r.IsNull(DT.TaggedItemsDataTable.Columns.TombstoneRatioAvg))
                                                .ActionIfNotEmpty(i => i.Min(r => r.Field<decimal>(DT.TaggedItemsDataTable.Columns.TombstoneRatioAvg)), default(decimal?)),
                                  Tombstones = SumL(grp, DT.TaggedItemsDataTable.Columns.TombstonesRead),
                                  LiveRead = SumL(grp, DT.TaggedItemsDataTable.Columns.LiveRead),
                                  ReadFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.ReadFactor),
                                  ReadPercent = Sum(grp, DT.TaggedItemsDataTable.Columns.ReadPercent),
                                  KeyPercent = Sum(grp, DT.TaggedItemsDataTable.Columns.KeysPercent),
                                  WriteFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.WriteFactor),
                                  WritePercent = Sum(grp, DT.TaggedItemsDataTable.Columns.WritePercent),
                                  StoragePercent = Sum(grp, DT.TaggedItemsDataTable.Columns.StoragePercent),
                                  SSTableFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.SSTablesFactor),
                                  PartitionFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.PartitionSizeFactor),
                                  SecondaryIndexes = grp.First().Field<int?>(DT.TaggedItemsDataTable.Columns.SecondaryIndexes),
                                  Views = grp.First().Field<int?>(DT.TaggedItemsDataTable.Columns.MaterializedViews),
                                  CommonKey = grp.First().Field<string>(DT.TaggedItemsDataTable.Columns.CommonKey),
                                  CommonKeyFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.CommonKeyFactor),
                                  BaseTableFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.BaseTableWeightedFactorMax)
                              };

            DataRow newDataRow;
            string lstKeySpace = null;
            string lstCompaction = null;
            string lstType = null;

            foreach (var factors in taggedItems)
            {
                newDataRow = dtOverview.NewRow();

                if (lstKeySpace != factors.Keyspace)
                {
                    var ksRow = dtOverview.NewRow();
                    ksRow.SetField(Columns.KSCSType, lstKeySpace = factors.Keyspace);
                    dtOverview.Rows.Add(ksRow);
                    lstCompaction = lstType = null;
                }
                if (lstCompaction != factors.Compaction)
                {
                    var csRow = dtOverview.NewRow();
                    csRow.SetField(Columns.KSCSType, "   " + (lstCompaction = factors.Compaction));
                    dtOverview.Rows.Add(csRow);
                    lstType = null;
                }
                if (lstType != factors.CQLType)
                {
                    var csRow = dtOverview.NewRow();
                    csRow.SetField(Columns.KSCSType, "      " + (lstType = factors.CQLType));
                    dtOverview.Rows.Add(csRow);
                }

                newDataRow.SetField(DSEDiagnosticToDataTable.ColumnNames.Table, factors.Table);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.TombstoneRatioMax, factors.TRMax);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.TombstoneRatioMin, factors.TRMin);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.TombstoneRatioAvg, factors.TRAvg);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.TombstonesRead, factors.Tombstones);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.LiveRead, factors.LiveRead);

                foreach (var dcFactor in factors.DCFactor)
                {
                    newDataRow.SetField(dcFactor.DC, dcFactor.Factor);
                }

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.ReadFactor, factors.ReadFactor);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.ReadPercent, factors.ReadPercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.KeysPercent, factors.KeyPercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.WriteFactor, factors.WriteFactor);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.WritePercent, factors.WritePercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.StoragePercent, factors.StoragePercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.SSTablesFactor, factors.SSTableFactor);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.PartitionSizeFactor, factors.PartitionFactor);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.SecondaryIndexes, factors.SecondaryIndexes);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.MaterializedViews, factors.Views);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.CommonKey, factors.CommonKey);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.CommonKeyFactor, factors.CommonKeyFactor);

                newDataRow.SetField(Columns.BaseTableFactor, factors.BaseTableFactor);

                dtOverview.Rows.Add(newDataRow);
            }


            return dtOverview;
        }

        public DataTable CreateInitializationLargePartitionsTable(DataTable sourceTable)
        {
            var dtOverview = new DataTable(LargePartitionsTable, DSEDiagnosticToDataTable.TableNames.Namespace);

            if (this.DataTable.Columns.Contains(DSEDiagnosticToDataTable.ColumnNames.SessionId)) dtOverview.Columns.Add(DSEDiagnosticToDataTable.ColumnNames.SessionId, typeof(Guid));

            dtOverview.Columns.Add(Columns.KSCSType, typeof(string)).SetDBNull();
            dtOverview.Columns.Add(DSEDiagnosticToDataTable.ColumnNames.Table, typeof(string)).SetDBNull();

            dtOverview.SetGroupHeader("DataCenters", -1, true,
                this.AddDCColumns(dtOverview, sourceTable)
            );

            dtOverview.SetGroupHeader("Large Partition/Wide Rows", -1, true,
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.PartitionSizeMax, typeof(decimal))
                    .SetNumericFormat("#,###,###,##0.0000")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonPartitionSize)
                    .SetDBNull()
                    .SetCaption("Max"),
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.PartitionSizeMin, typeof(decimal))
                    .SetNumericFormat("#,###,###,##0.0000")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonPartitionSize)
                    .SetDBNull()
                    .SetCaption("Min"),
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.PartitionSizeAvg, typeof(decimal))
                    .SetNumericFormat("#,###,###,##0.0000")
                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonPartitionSize)
                    .SetDBNull()
                    .SetCaption("Avg")                
                    );

            dtOverview.SetGroupHeader(string.Empty, -1, false,
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.KeysPercent, typeof(decimal))
                    .SetNumericFormat("##0.00%")
                    .SetDBNull(),
                dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.StoragePercent, typeof(decimal))
                    .SetNumericFormat("##0.00%")
                    .SetDBNull());

            dtOverview.SetGroupHeader("Informational", -2, true,
               dtOverview.SetGroupHeader("Read", -1, true,
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.ReadFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull()
                        .SetCaption("Factor"),
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.ReadPercent, typeof(decimal))
                        .SetDBNull()
                        .SetNumericFormat("##0.00%")
                        .SetCaption("Percent")
                        ),
               
                dtOverview.SetGroupHeader("Write", -1, true,
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.WriteFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull()
                        .SetCaption("Factor"),
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.WritePercent, typeof(decimal))
                        .SetDBNull()
                        .SetNumericFormat("##0.00%")
                        .SetCaption("Percent")
                        ),
                dtOverview.SetGroupHeader(string.Empty, -1, false,
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull(),

                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.SSTablesFactor, typeof(decimal))
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                        .SetDBNull(),

                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.SecondaryIndexes, typeof(int))
                        .SetNumericFormat("##0")
                        .SetDBNull(),
                    dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.MaterializedViews, typeof(int))
                        .SetNumericFormat("##0")
                        .SetDBNull()),

                    dtOverview.SetGroupHeader("Common Partition Keys", -1, true,
                       dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.CommonKey, typeof(string))
                            .SetDBNull()
                            .SetCaption("Key"),
                        dtOverview.Columns.Add(DT.TaggedItemsDataTable.Columns.CommonKeyFactor, typeof(decimal))
                            .SetNumericFormat("#,###,###,##0.00")
                            .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                            .SetDBNull()
                            .SetCaption("Factor")
                        ),
                    dtOverview.SetGroupHeader(string.Empty, -1, false,
                        dtOverview.Columns.Add(Columns.BaseTableFactor, typeof(decimal))
                            .SetNumericFormat("#,###,###,##0.00")
                            .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor)
                            .SetDBNull())
            );

            decimal? WeightAvg(IEnumerable<DataRow> dataRows, string columnName)
            {
                var factorValues = dataRows.Where(r => !r.IsNull(columnName)).Select(r => r.Field<decimal>(columnName)).ToArray();

                if (factorValues.Length == 0) return null;

                var aggregate = factorValues.Sum();
                var maxFactor = factorValues.Max();

                return (aggregate + maxFactor) / ((decimal)factorValues.Length + 1m);
            }

            decimal? Sum(IEnumerable<DataRow> dataRows, string columnName)
            {
                var factorValue = dataRows.Where(r => !r.IsNull(columnName))
                                            .Select(r => r.Field<decimal>(columnName))
                                            .DefaultIfEmpty()
                                            .Sum();

                if (factorValue == 0) return null;

                return factorValue;
            }
            
            var taggedItems = from dataRow in sourceTable.AsEnumerable()
                              where !dataRow.IsNull(DT.TaggedItemsDataTable.Columns.PartitionSizeFactor)
                              let keySpace = dataRow.Field<string>(DT.ColumnNames.KeySpace)
                              let compactionType = dataRow.Field<string>(DT.TaggedItemsDataTable.Columns.CompactionStrategy)
                              let cqlType = dataRow.Field<string>(DT.TaggedItemsDataTable.Columns.CQLType)
                              let tableName = dataRow.Field<string>(DT.ColumnNames.Table)
                              group dataRow by new { keySpace, compactionType, cqlType, tableName } into grp
                              let dcfactors = grp.Select(i => new
                              {
                                  DC = i.Field<string>(DT.ColumnNames.DataCenter),
                                  Factor = i.Field<decimal>(DT.TaggedItemsDataTable.Columns.PartitionSizeFactor)
                              })
                              where dcfactors.Any(i => i.Factor >= Properties.Settings.Default.TriggerPartitionSizeFactor)
                              orderby grp.Key.keySpace ascending, grp.Key.compactionType ascending, grp.Key.cqlType ascending, grp.Key.tableName ascending
                              select new
                              {
                                  Keyspace = grp.Key.keySpace,
                                  Compaction = grp.Key.compactionType,
                                  CQLType = grp.Key.cqlType,
                                  Table = grp.Key.tableName,
                                  DCFactor = dcfactors,
                                  PSMax = grp.Max(r => r.Field<decimal>(DT.TaggedItemsDataTable.Columns.PartitionSizeMax)),                                  
                                  PSMin = grp.Where(r => !r.IsNull(DT.TaggedItemsDataTable.Columns.PartitionSizeMin))
                                                .ActionIfNotEmpty(i => i.Min(r => r.Field<decimal>(DT.TaggedItemsDataTable.Columns.PartitionSizeMin)), default(decimal?)),
                                  PSAvg = grp.Where(r => !r.IsNull(DT.TaggedItemsDataTable.Columns.PartitionSizeAvg))
                                                .ActionIfNotEmpty(i => i.Min(r => r.Field<decimal>(DT.TaggedItemsDataTable.Columns.PartitionSizeAvg)), default(decimal?)),
                                  ReadFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.ReadFactor),
                                  ReadPercent = Sum(grp, DT.TaggedItemsDataTable.Columns.ReadPercent),
                                  KeyPercent = Sum(grp, DT.TaggedItemsDataTable.Columns.KeysPercent),
                                  WriteFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.WriteFactor),
                                  WritePercent = Sum(grp, DT.TaggedItemsDataTable.Columns.WritePercent),
                                  StoragePercent = Sum(grp, DT.TaggedItemsDataTable.Columns.StoragePercent),
                                  TombstoneFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor),
                                  SSTablesFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.SSTablesFactor),
                                  SecondaryIndexes = grp.First().Field<int?>(DT.TaggedItemsDataTable.Columns.SecondaryIndexes),
                                  Views = grp.First().Field<int?>(DT.TaggedItemsDataTable.Columns.MaterializedViews),
                                  CommonKey = grp.First().Field<string>(DT.TaggedItemsDataTable.Columns.CommonKey),
                                  CommonKeyFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.CommonKeyFactor),
                                  BaseTableFactor = WeightAvg(grp, DT.TaggedItemsDataTable.Columns.BaseTableWeightedFactorMax)
                              };

            DataRow newDataRow;
            string lstKeySpace = null;
            string lstCompaction = null;
            string lstType = null;

            foreach (var factors in taggedItems)
            {
                newDataRow = dtOverview.NewRow();

                if (lstKeySpace != factors.Keyspace)
                {
                    var ksRow = dtOverview.NewRow();
                    ksRow.SetField(Columns.KSCSType, lstKeySpace = factors.Keyspace);
                    dtOverview.Rows.Add(ksRow);
                    lstCompaction = lstType = null;
                }
                if (lstCompaction != factors.Compaction)
                {
                    var csRow = dtOverview.NewRow();
                    csRow.SetField(Columns.KSCSType, "   " + (lstCompaction = factors.Compaction));
                    dtOverview.Rows.Add(csRow);
                    lstType = null;
                }
                if (lstType != factors.CQLType)
                {
                    var csRow = dtOverview.NewRow();
                    csRow.SetField(Columns.KSCSType, "      " + (lstType = factors.CQLType));
                    dtOverview.Rows.Add(csRow);
                }

                newDataRow.SetField(DSEDiagnosticToDataTable.ColumnNames.Table, factors.Table);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.PartitionSizeMax, factors.PSMax);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.PartitionSizeMin, factors.PSMin);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.PartitionSizeAvg, factors.PSAvg);
                
                foreach (var dcFactor in factors.DCFactor)
                {
                    newDataRow.SetField(dcFactor.DC, dcFactor.Factor);
                }

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.ReadFactor, factors.ReadFactor);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.ReadPercent, factors.ReadPercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.KeysPercent, factors.KeyPercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.WriteFactor, factors.WriteFactor);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.WritePercent, factors.WritePercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.StoragePercent, factors.StoragePercent);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor, factors.TombstoneFactor);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.SSTablesFactor, factors.SSTablesFactor);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.SecondaryIndexes, factors.SecondaryIndexes);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.MaterializedViews, factors.Views);

                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.CommonKey, factors.CommonKey);
                newDataRow.SetField(DT.TaggedItemsDataTable.Columns.CommonKeyFactor, factors.CommonKeyFactor);

                newDataRow.SetField(Columns.BaseTableFactor, factors.BaseTableFactor);

                dtOverview.Rows.Add(newDataRow);
            }


            return dtOverview;
        }

        public override Tuple<IFilePath, string, int> Load()
        {
            int nbrRows = 0;

            this.CallActionEvent("PreLoading");

            {
                var linqDetailViewTask = Task.Factory.StartNew( () =>
                {
                    var rows = this.DataTable.AsEnumerable()
                                .Where(dataRow => !dataRow.IsNull(DT.ColumnNames.NodeIPAddress))
                                .OrderBy(dataRow => dataRow.Field<string>(DT.ColumnNames.KeySpace))
                                .ThenBy(dataRow => dataRow.Field<string>(DT.ColumnNames.Table))
                                .ThenBy(dataRow => dataRow.Field<string>(DT.ColumnNames.DataCenter))
                                .ThenBy(dataRow => dataRow.Field<string>(DT.ColumnNames.NodeIPAddress));

                    return rows.IsEmpty() ? this.DataTable.Clone() : rows.CopyToDataTable();
                });
                var linqDCViewTask = Task.Factory.StartNew(() =>
                {
                    var rows = this.DataTable.AsEnumerable()
                                .Where(dataRow => dataRow.IsNull(DT.ColumnNames.NodeIPAddress))
                                .OrderBy(dataRow => dataRow.Field<string>(DT.ColumnNames.KeySpace))
                                .ThenBy(dataRow => dataRow.Field<string>(DT.ColumnNames.Table))
                                .ThenBy(dataRow => dataRow.Field<string>(DT.ColumnNames.DataCenter));
                    return rows.IsEmpty() ? this.DataTable.Clone() : rows.CopyToDataTable();
                });

                var linqDetailView = linqDetailViewTask.Result;
                var linqDCView = linqDCViewTask.Result;

                linqDetailView.TableName = "TaggedTablesDetail";
                linqDCView.TableName = "TaggedTables";
                linqDCView.Columns.Remove(DT.ColumnNames.NodeIPAddress);

                nbrRows += this.GenerateWorkSheet("TaggedTablesDetail", linqDetailView, this.UseDataTableDefaultView);                
                nbrRows += this.GenerateWorkSheet("TaggedTables", linqDCView, this.UseDataTableDefaultView);

                var readViewTask = Task.Factory.StartNew(() =>
                {
                    return this.CreateInitializationReadLatencyTable(linqDCView);
                });
                var writeViewTask = Task.Factory.StartNew(() =>
                {
                    return this.CreateInitializationWriteLatencyTable(linqDCView);
                });
                var sstablesiewTask = Task.Factory.StartNew(() =>
                {
                    return this.CreateInitializationSSTablesLatencyTable(linqDCView);
                });
                var tombstonesViewTask = Task.Factory.StartNew(() =>
                {
                    return this.CreateInitializationTombstonesTable(linqDCView);
                });
                var partitionsViewTask = Task.Factory.StartNew(() =>
                {
                    return this.CreateInitializationLargePartitionsTable(linqDCView);
                });
                
                nbrRows += this.GenerateTaggedTypeWorkSheet(readViewTask.Result, this.UseDataTableDefaultView, 3);
                nbrRows += this.GenerateTaggedTypeWorkSheet(writeViewTask.Result, this.UseDataTableDefaultView, 3);
                nbrRows += this.GenerateTaggedTypeWorkSheet(sstablesiewTask.Result, this.UseDataTableDefaultView, 3);
                nbrRows += this.GenerateTaggedTypeWorkSheet(tombstonesViewTask.Result, this.UseDataTableDefaultView, 3);
                nbrRows += this.GenerateTaggedTypeWorkSheet(partitionsViewTask.Result, this.UseDataTableDefaultView, 3);
            }
           
            return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, nbrRows);
        }

        int GenerateWorkSheet(string wsName, DataTable sourceDataTable, bool useDefaultDataView)
        {            
            return DataTableToExcel.Helpers.WorkBook(this.ExcelTargetWorkbook.PathResolved, wsName, sourceDataTable,
                                                        (stage, orgFilePath, targetFilePath, workSheetName, excelPackage, excelDataTable, rowCount, loadRange) =>
                                                        {
                                                            switch (stage)
                                                            {
                                                                case WorkBookProcessingStage.PreProcess:
                                                                case WorkBookProcessingStage.PrepareFileName:
                                                                case WorkBookProcessingStage.PreProcessDataTable:
                                                                    break;
                                                                case WorkBookProcessingStage.PreLoad:
                                                                    this.CallActionEvent("Begin Loading");
                                                                    break;
                                                                case WorkBookProcessingStage.PreSave:
                                                                    {
                                                                        var workSheet = excelPackage.Workbook.Worksheets[workSheetName];

                                                                        if(workSheet != null && rowCount > 0)
                                                                            workSheet.AltFileFillRow(2,
                                                                                                       excelDataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.KeySpace),
                                                                                                       excelDataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.Table));

                                                                    }
                                                                    this.CallActionEvent("Loaded");
                                                                    break;
                                                                case WorkBookProcessingStage.Saved:
                                                                    this.CallActionEvent("Workbook Saved");
                                                                    break;
                                                                case WorkBookProcessingStage.PostProcess:
                                                                    break;
                                                                default:
                                                                    break;
                                                            }
                                                        },
                                                         workSheet =>
                                                         {
                                                             //workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                             workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                             //workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                             workSheet.View.FreezePanes(2, 1);

                                                             //sourceDataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.KeySpace);
                                                             //sourceDataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.Table);
                                                             //sourceDataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.DataCenter);
                                                             //sourceDataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.NodeIPAddress);
                                                             //sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.CQLType);

                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WeightedFactorMax)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WeightedFactorAvg)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);

                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadMax)
                                                                .SetNumericFormat("#,###,###,##0.000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonReadLatencyMax);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadMin)
                                                                .SetNumericFormat("#,###,###,##0.000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonReadLatencyMin);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadAvg)
                                                                .SetNumericFormat("#,###,###,##0.000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonWriteLatencyAvg);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadStdDev)
                                                                .SetNumericFormat("#,###,###,##0.000");
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadFactor)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);

                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadPercent)
                                                                 .SetNumericFormat("##0.00%");
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.KeysPercent)
                                                                 .SetNumericFormat("##0.00%");

                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WriteMax)
                                                                .SetNumericFormat("#,###,###,##0.000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonWriteLatencyMax);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WriteMin)
                                                                .SetNumericFormat("#,###,###,##0.000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonWriteLatencyMin);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WriteAvg)
                                                                .SetNumericFormat("#,###,###,##0.000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonWriteLatencyAvg);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WriteStdDev)
                                                                .SetNumericFormat("#,###,###,##0.000");
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WriteFactor)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);

                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WritePercent)
                                                                 .SetNumericFormat("##0.00%");

                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablesMax)
                                                                .SetNumericFormat("#,###,###,##0")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonSSTablesTable);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablesMin)
                                                                .SetNumericFormat("#,###,###,##0")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonSSTablesTable);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablesAvg)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonSSTablesTable);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablesStdDev)
                                                                .SetNumericFormat("#,###,###,##0.00");
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablesFactor)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);

                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablePercent)
                                                                 .SetNumericFormat("##0.00%");
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.StoragePercent)
                                                                 .SetNumericFormat("##0.00%");

                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.TombstoneRatioMax)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonTombstoneLiveRatio);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.TombstoneRatioMin)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonTombstoneLiveRatio);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.TombstoneRatioAvg)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonTombstoneLiveRatio);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.TombstoneRatioStdDev)
                                                                .SetNumericFormat("#,###,###,##0.00");
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.TombstonesRead)
                                                                .SetNumericFormat("#,###,###,##0.00");
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.LiveRead)
                                                                .SetNumericFormat("#,###,###,##0.00");

                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.PartitionSizeMax)
                                                                .SetNumericFormat("#,###,###,##0.0000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonPartitionSize);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.PartitionSizeMin)
                                                                .SetNumericFormat("#,###,###,##0.0000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonPartitionSize);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.PartitionSizeAvg)
                                                                .SetNumericFormat("#,###,###,##0.0000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonPartitionSize);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.PartitionSizeStdDev)
                                                                .SetNumericFormat("#,###,###,##0.0000");
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.PartitionSizeFactor)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);

                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.FlushSizeMax)
                                                                .SetNumericFormat("#,###,###,##0.0000");
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.FlushSizeMin)
                                                                .SetNumericFormat("#,###,###,##0.0000");
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.FlushSizeAvg)
                                                                .SetNumericFormat("#,###,###,##0.0000");
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.FlushSizeStdDev)
                                                                .SetNumericFormat("#,###,###,##0.0000");
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.FlushSizeFactor)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.FlushPending)
                                                                .SetNumericFormat("#,###,###,##0");

                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SecondaryIndexes)
                                                                  .SetNumericFormat("#,###,###,##0");
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.MaterializedViews)
                                                                  .SetNumericFormat("#,###,###,##0");
                                                             //sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.CommonKey);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.CommonKeyFactor)
                                                                 .SetNumericFormat("#,###,###,##0.00")
                                                                 .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);

                                                             //sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.BaseTable);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.BaseTableWeightedFactorAvg)
                                                                    .SetNumericFormat("#,###,###,##0.00")
                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);
                                                             sourceDataTable.GetColumn(DT.TaggedItemsDataTable.Columns.BaseTableWeightedFactorMax)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);

                                                             //sourceDataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.ReconciliationRef);

                                                             workSheet.UpdateWorksheet(sourceDataTable, 1);

                                                             workSheet.AutoFitColumn(sourceDataTable);

                                                             var tableName = wsName + "Table";
                                                             var table = workSheet.Tables.FirstOrDefault(t => t.Name == tableName);
                                                             var rowCnt = sourceDataTable.Rows.Count;

                                                             if (rowCnt == 0)
                                                             {
                                                                 rowCnt = 1;
                                                             }
                                                             else
                                                             {
                                                                 rowCnt = workSheet.Dimension.End.Row;
                                                             }

                                                             if (table == null)
                                                             {
                                                                 using (var tblRange = workSheet.TranslaateToColumnRange(sourceDataTable,
                                                                                                                            DSEDiagnosticToDataTable.ColumnNames.KeySpace,
                                                                                                                            DSEDiagnosticToDataTable.ColumnNames.ReconciliationRef,
                                                                                                                            1,
                                                                                                                            rowCnt + 2))
                                                                 {
                                                                     table = workSheet.Tables.Add(tblRange, tableName);

                                                                     table.ShowFilter = true;
                                                                     table.ShowHeader = true;
                                                                     //table.TableStyle = OfficeOpenXml.Table.TableStyles.Light21;
                                                                 }
                                                             }
                                                             else
                                                             {
                                                                 var oldaddy = table.Address;
                                                                 var newaddy = new ExcelAddressBase(oldaddy.Start.Row, oldaddy.Start.Column, rowCnt + 2, oldaddy.End.Column);

                                                                 //Edit the raw XML by searching for all references to the old address
                                                                 table.TableXml.InnerXml = table.TableXml.InnerXml.Replace(oldaddy.ToString(), newaddy.ToString());
                                                             }
                                                         },
                                                         -1,
                                                        -1,
                                                        "A1",
                                                        useDefaultDataView,
                                                        appendToWorkSheet: this.AppendToWorkSheet,
                                                       cachePackage: LibrarySettings.ExcelPackageCache,
                                                       saveWorkSheet: LibrarySettings.ExcelSaveWorkSheet,
                                                       excelTemplateFile: this.ExcelTemplateWorkbook);
        }

        int GenerateTaggedTypeWorkSheet(DataTable sourceDataTable, bool useDefaultDataView, int nStartRow = 2)
        {
            return DataTableToExcel.Helpers.WorkBook(this.ExcelTargetWorkbook.PathResolved, sourceDataTable.TableName, sourceDataTable,
                                                        (stage, orgFilePath, targetFilePath, workSheetName, excelPackage, excelDataTable, rowCount, loadRange) =>
                                                        {
                                                            switch (stage)
                                                            {
                                                                case WorkBookProcessingStage.PreProcess:
                                                                case WorkBookProcessingStage.PrepareFileName:
                                                                case WorkBookProcessingStage.PreProcessDataTable:
                                                                    break;
                                                                case WorkBookProcessingStage.PreLoad:
                                                                    this.CallActionEvent("Begin Loading");
                                                                    break;
                                                                case WorkBookProcessingStage.PreSave:
                                                                    {
                                                                        var workSheet = excelPackage.Workbook.Worksheets[workSheetName];

                                                                        if(workSheet != null && rowCount > 0)
                                                                            workSheet.AltFileFillRow(nStartRow + 1,
                                                                                                       excelDataTable.GetColumn(Columns.KSCSType),
                                                                                                       null,
                                                                                                       excelDataTable.GetColumn(DT.ColumnNames.Table),
                                                                                                       excelDataTable.GetColumn(Columns.KSCSType),
                                                                                                       excelDataTable.GetColumn(DT.ColumnNames.Table));
                                                                    }
                                                                    this.CallActionEvent("Loaded");
                                                                    break;
                                                                case WorkBookProcessingStage.Saved:
                                                                    this.CallActionEvent("Workbook Saved");
                                                                    break;
                                                                case WorkBookProcessingStage.PostProcess:
                                                                    break;
                                                                default:
                                                                    break;
                                                            }
                                                        },
                                                         workSheet =>
                                                         {
                                                             workSheet.Cells[string.Format("1:{0}", nStartRow - 1)].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                             workSheet.Cells[string.Format("1:{0}", nStartRow)].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                             //workSheet.Cells["1:2"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                             
                                                             workSheet.UpdateWorksheet(sourceDataTable, nStartRow);

                                                             workSheet.View.FreezePanes(nStartRow + 1, 1);
                                                            
                                                             workSheet.ExcelRange(nStartRow,
                                                                                      sourceDataTable.GetColumn(DT.ColumnNames.Table),
                                                                                      sourceDataTable.GetColumn(Columns.BaseTableFactor))
                                                                            .First().AutoFilter = true;                                                             
                                                             workSheet.AutoFitColumn();
                                                         },
                                                         -1,
                                                        -1,
                                                        string.Format("A{0}", nStartRow),
                                                        useDefaultDataView,
                                                        appendToWorkSheet: this.AppendToWorkSheet,
                                                       cachePackage: LibrarySettings.ExcelPackageCache,
                                                       saveWorkSheet: LibrarySettings.ExcelSaveWorkSheet,
                                                       excelTemplateFile: this.ExcelTemplateWorkbook);
        }
    }
}
