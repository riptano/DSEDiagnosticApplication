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
    public class TaggedItemsExcel : LoadToExcel
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
        
        public override Tuple<IFilePath, string, int> Load()
        {

            this.CallActionEvent("PreLoading");

            if (this.DataTable.TableName == DataTableName)
                return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, 0);

            int nbrRows = DataTableToExcel.Helpers.WorkBook(this.ExcelTargetWorkbook.PathResolved, this.DataTable.TableName, this.DataTable,
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

                                                                        if (workSheet != null && rowCount > 0)
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
                                                         (workSheet, splitNbr) =>
                                                         {
                                                             //workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                             workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                             //workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                             workSheet.View.FreezePanes(2, 1);

                                                             //this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.KeySpace);
                                                             //this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.Table);
                                                             //this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.DataCenter);
                                                             //this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.NodeIPAddress);
                                                             //this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.CQLType);

                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WeightedFactorMax)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WeightedFactorAvg)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);

                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadMax)
                                                                .SetNumericFormat("#,###,###,##0.000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonReadLatencyMax);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadMin)
                                                                .SetNumericFormat("#,###,###,##0.000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonReadLatencyMin);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadAvg)
                                                                .SetNumericFormat("#,###,###,##0.000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonWriteLatencyAvg);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadStdDev)
                                                                .SetNumericFormat("#,###,###,##0.000");
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadFactor)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);

                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadPercent)
                                                                 .SetNumericFormat("##0.00%");
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.KeysPercent)
                                                                 .SetNumericFormat("##0.00%");

                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WriteMax)
                                                                .SetNumericFormat("#,###,###,##0.000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonWriteLatencyMax);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WriteMin)
                                                                .SetNumericFormat("#,###,###,##0.000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonWriteLatencyMin);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WriteAvg)
                                                                .SetNumericFormat("#,###,###,##0.000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonWriteLatencyAvg);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WriteStdDev)
                                                                .SetNumericFormat("#,###,###,##0.000");
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WriteFactor)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);

                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WritePercent)
                                                                 .SetNumericFormat("##0.00%");

                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablesMax)
                                                                .SetNumericFormat("#,###,###,##0")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonSSTablesTable);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablesMin)
                                                                .SetNumericFormat("#,###,###,##0")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonSSTablesTable);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablesAvg)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonSSTablesTable);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablesStdDev)
                                                                .SetNumericFormat("#,###,###,##0.00");
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablesFactor)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);

                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablePercent)
                                                                 .SetNumericFormat("##0.00%");
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.StoragePercent)
                                                                 .SetNumericFormat("##0.00%");

                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.TombstoneRatioMax)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonTombstoneLiveRatio);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.TombstoneRatioMin)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonTombstoneLiveRatio);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.TombstoneRatioAvg)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonTombstoneLiveRatio);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.TombstoneRatioStdDev)
                                                                .SetNumericFormat("#,###,###,##0.00");
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.TombstoneRatioFactor)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.TombstonesRead)
                                                                .SetNumericFormat("#,###,###,##0.00");
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.LiveRead)
                                                                .SetNumericFormat("#,###,###,##0.00");

                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.PartitionSizeMax)
                                                                .SetNumericFormat("#,###,###,##0.0000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonPartitionSize);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.PartitionSizeMin)
                                                                .SetNumericFormat("#,###,###,##0.0000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonPartitionSize);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.PartitionSizeAvg)
                                                                .SetNumericFormat("#,###,###,##0.0000")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonPartitionSize);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.PartitionSizeStdDev)
                                                                .SetNumericFormat("#,###,###,##0.0000");
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.PartitionSizeFactor)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);

                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.FlushSizeMax)
                                                                .SetNumericFormat("#,###,###,##0.0000");
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.FlushSizeMin)
                                                                .SetNumericFormat("#,###,###,##0.0000");
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.FlushSizeAvg)
                                                                .SetNumericFormat("#,###,###,##0.0000");
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.FlushSizeStdDev)
                                                                .SetNumericFormat("#,###,###,##0.0000");
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.FlushSizeFactor)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.FlushPending)
                                                                .SetNumericFormat("#,###,###,##0");

                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SecondaryIndexes)
                                                                  .SetNumericFormat("#,###,###,##0");
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.MaterializedViews)
                                                                  .SetNumericFormat("#,###,###,##0");
                                                             //this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.CommonKey);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.CommonKeyFactor)
                                                                 .SetNumericFormat("#,###,###,##0.00")
                                                                 .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);

                                                             //this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.BaseTable);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.BaseTableWeightedFactorAvg)
                                                                    .SetNumericFormat("#,###,###,##0.00")
                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);
                                                             this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.BaseTableWeightedFactorMax)
                                                                .SetNumericFormat("#,###,###,##0.00")
                                                                .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);

                                                             workSheet.UpdateWorksheet(this.DataTable, 1);

                                                             workSheet.AutoFitColumn(this.DataTable);

                                                             var newWSName = this.DataTable.TableName + "Table";

                                                             if (splitNbr.HasValue && splitNbr.Value > 1)
                                                             {
                                                                 newWSName += splitNbr.Value.ToString("000");
                                                             }

                                                             workSheet.CreateExcelTable(this.DataTable, newWSName);
                                                         },
                                                         -1,
                                                        -1,
                                                        "A1",
                                                        this.UseDataTableDefaultView,
                                                        appendToWorkSheet: this.AppendToWorkSheet,
                                                       cachePackage: LibrarySettings.ExcelPackageCache,
                                                       saveWorkSheet: LibrarySettings.ExcelSaveWorkSheet,
                                                       excelTemplateFile: this.ExcelTemplateWorkbook);

            return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, nbrRows);
        }        
    }
}
