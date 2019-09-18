using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;
using Common;
using OfficeOpenXml;
using DataTableToExcel;
using DT = DSEDiagnosticToDataTable;

namespace DSEDiagtnosticToExcel
{
    public sealed class CQLDLLExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.CQLDLL;

        public CQLDLLExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook,
                                string worksheetName,
                                bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        {
            this.AppendToWorkSheet = false;
        }

        public CQLDLLExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook = null)
            : this(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, null, true)
        { }

        public override Tuple<IFilePath, string, int> Load()
        {
           var nbrRows = DataTableToExcel.Helpers.WorkBook(this.ExcelTargetWorkbook.PathResolved, this.WorkSheetName, this.DataTable,
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
                                                                            var workSheet = excelPackage.Workbook.Worksheets[WorkSheetName];

                                                                            workSheet.AltFileFillRow(3,
                                                                                                       this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.KeySpace));

                                                                            this.CallActionEvent("Loaded");
                                                                        }
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
                                                            (workSheet, splitNbr) =>
                                                            {
                                                                workSheet.Cells["1:2"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                workSheet.Cells["1:2"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                workSheet.View.FreezePanes(3, 5);

                                                                this.DataTable.GetColumn("Active")
                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonFalseRed);
                                                                //this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.KeySpace);
                                                                //this.DataTable.GetColumn("Name");
                                                                //this.DataTable.GetColumn("Type");
                                                                //this.DataTable.GetColumn("Partition Key");
                                                                //this.DataTable.GetColumn("Cluster Key");
                                                                //this.DataTable.GetColumn("Compaction Strategy");
                                                                //this.DataTable.GetColumn("Compression");

                                                                this.DataTable.SetGroupHeader("Read-Repair", -2, true,
                                                                    this.DataTable.GetColumn("Chance")
                                                                        .SetNumericFormat("0%")
                                                                        .SetComment(Properties.Settings.Default.CmtReadRepairChance),
                                                                    this.DataTable.GetColumn("DC Chance")
                                                                        .SetNumericFormat("0%")
                                                                        .SetComment(Properties.Settings.Default.CmtReadRepairDCChance),
                                                                    this.DataTable.GetColumn("Policy")
                                                                    .SetComment(Properties.Settings.Default.CmtReadRepairPolicy)
                                                                );

                                                                this.DataTable.SetGroupHeader("Tombstone", -2, true,
                                                                    this.DataTable.GetColumn("GC Grace Period")
                                                                        .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat)
                                                                        .SetComment(Properties.Settings.Default.CmtGCGrace),
                                                                    this.DataTable.GetColumn("TTL")
                                                                        .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat)
                                                                );

                                                                this.DataTable.GetColumn("Memtable Flush Period")
                                                                    .SetNumericFormat("#,###,###,###")
                                                                    .SetComment("milliseconds");

                                                                this.DataTable.SetGroupHeader("Column Counts", -2, true,
                                                                    this.DataTable.GetColumn("Collections")
                                                                        .SetNumericFormat("#,###")
                                                                        .TotalColumn(),
                                                                    this.DataTable.GetColumn("Counters")
                                                                         .SetNumericFormat("#,###")
                                                                         .TotalColumn(),
                                                                    this.DataTable.GetColumn("Blobs")
                                                                         .SetNumericFormat("#,###")
                                                                         .TotalColumn(),
                                                                    this.DataTable.GetColumn("Static")
                                                                         .SetNumericFormat("#,###")
                                                                         .TotalColumn(),
                                                                    this.DataTable.GetColumn("Frozen")
                                                                         .SetNumericFormat("#,###")
                                                                         .TotalColumn(),
                                                                    this.DataTable.GetColumn("Tuple")
                                                                         .SetNumericFormat("#,###")
                                                                         .TotalColumn(),
                                                                    this.DataTable.GetColumn("UDT")
                                                                         .SetNumericFormat("#,###")
                                                                         .TotalColumn(),
                                                                    this.DataTable.GetColumn("Total")
                                                                         .SetNumericFormat("#,###")
                                                                         .TotalColumn()
                                                                );

                                                                this.DataTable.GetColumn("HasOrderBy")
                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonTrueYellow)
                                                                    .CountNonBlankColumn(includeCondFmt: "#,###,###");
                                                                this.DataTable.GetColumn("Compact Storage")
                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonTrueYellow)
                                                                    .CountNonBlankColumn(includeCondFmt: "#,###,###");
                                                                this.DataTable.GetColumn("Node Sync")
                                                                    .CountNonBlankColumn(true, "#,###,###");
                                                                
                                                                this.DataTable.GetColumn("NbrIndexes")
                                                                        .SetNumericFormat("#,###")
                                                                        .TotalColumn();
                                                                this.DataTable.GetColumn("NbrMVs")
                                                                        .SetNumericFormat("#,###")
                                                                        .TotalColumn();
                                                                this.DataTable.GetColumn("NbrTriggers")
                                                                        .SetNumericFormat("#,###")
                                                                        .TotalColumn();

                                                                this.DataTable.SetGroupHeader("Storage", -1, true,
                                                                   this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.Storage)
                                                                        .SetNumericFormat("###,###,###,###.0000")
                                                                        .TotalColumn()
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.StorageUtilized)
                                                                        .SetNumericFormat("##0%")
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue)
                                                                        );
                                                                this.DataTable.SetGroupHeader("Cells", -1, true,
                                                                   this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.ReadPercent)
                                                                       .SetNumericFormat("##0%")
                                                                       .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                   this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.WritePercent)
                                                                       .SetNumericFormat("##0%")
                                                                       .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue)
                                                                       );
                                                                this.DataTable.SetGroupHeader("Partitions", -1, true,
                                                                   this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.PartitionKeys)
                                                                       .SetNumericFormat("###,###,###,##0")
                                                                       .TotalColumn()
                                                                       .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                   this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.PartitionKeyPercent)
                                                                       .SetNumericFormat("##0%")
                                                                       .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue)
                                                                       );
                                                                this.DataTable.SetGroupHeader("SSTables", -1, true,
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.SSTables)
                                                                        .SetNumericFormat("###,###,###,##0")
                                                                        .TotalColumn()
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.SSTablePercent)
                                                                        .SetNumericFormat("##0%")
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue)
                                                                        );
                                                                this.DataTable.SetGroupHeader("Operations", -1, true,
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.FlushPercent)
                                                                        .SetNumericFormat("##0%")
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.CompactionPercent)
                                                                        .SetNumericFormat("##0%")
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue),
                                                                    this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.RepairPercent)
                                                                        .SetNumericFormat("##0%")
                                                                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue)
                                                                   );

                                                                workSheet.UpdateWorksheet(this.DataTable, 2);

                                                                workSheet.ExcelRange(2,
                                                                                      this.DataTable.GetColumn("Active"),
                                                                                      this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.RepairPercent))
                                                                            .First().AutoFilter = true;

                                                                workSheet.AutoFitColumn(workSheet.ExcelRange(this.DataTable.GetColumn("Active"),
                                                                                                                this.DataTable.GetColumn(DT.KeyspaceDataTable.Columns.RepairPercent)));
                                                            },
                                                            -1,
                                                           -1,
                                                           "A2",
                                                           this.UseDataTableDefaultView,
                                                           appendToWorkSheet: this.AppendToWorkSheet,
                                                           cachePackage: LibrarySettings.ExcelPackageCache,
                                                           saveWorkSheet: LibrarySettings.ExcelSaveWorkSheet,
                                                           excelTemplateFile: this.ExcelTemplateWorkbook);

            return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, nbrRows);
        }
    }
}
