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

        public override Tuple<IFilePath, string, int> Load()
        {
            int nbrRows = 0;
            {
                
                var linqDetailView = this.DataTable.AsEnumerable()
                                        .Where(dataRow => !dataRow.IsNull(DT.ColumnNames.NodeIPAddress))
                                        .OrderBy(dataRow => dataRow.Field<string>(DT.ColumnNames.KeySpace))
                                        .ThenBy(dataRow => dataRow.Field<string>(DT.ColumnNames.Table))
                                        .ThenBy(dataRow => dataRow.Field<string>(DT.ColumnNames.DataCenter))
                                        .ThenBy(dataRow => dataRow.Field<string>(DT.ColumnNames.NodeIPAddress))
                                        .CopyToDataTable();
                linqDetailView.TableName = "TaggedTablesDetail";
                
                nbrRows += this.GenerateWorkSheet("TaggedTablesDetail", linqDetailView, this.UseDataTableDefaultView);
            }

            {
                var linqDCView = this.DataTable.AsEnumerable()
                                        .Where(dataRow => dataRow.IsNull(DT.ColumnNames.NodeIPAddress))
                                        .OrderBy(dataRow => dataRow.Field<string>(DT.ColumnNames.KeySpace))
                                        .ThenBy(dataRow => dataRow.Field<string>(DT.ColumnNames.Table))
                                        .ThenBy(dataRow => dataRow.Field<string>(DT.ColumnNames.DataCenter))
                                        .CopyToDataTable();
                linqDCView.TableName = "TaggedTables";

                linqDCView.Columns.Remove(DT.ColumnNames.NodeIPAddress);

                nbrRows += this.GenerateWorkSheet("TaggedTables", linqDCView, this.UseDataTableDefaultView);
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

                                                                        workSheet.AltFileFillRow(2,
                                                                                                   this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.KeySpace),
                                                                                                   sourceDataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.Table));

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
    }
}
