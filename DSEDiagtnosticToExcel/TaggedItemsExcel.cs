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
                                                                 workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                 workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                 //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                 workSheet.View.FreezePanes(2, 1);

                                                                 //this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.KeySpace);
                                                                 //this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.Table);
                                                                 //this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.DataCenter);
                                                                 //this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.NodeIPAddress);
                                                                 //this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.CQLType);

                                                                 this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadMax)
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadMin)
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.ReadAvg)
                                                                    .SetNumericFormat("#,###,###,##0.000");
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
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WriteMin)
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WriteAvg)
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WriteStdDev)
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WriteFactor)
                                                                    .SetNumericFormat("#,###,###,##0.00")
                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);

                                                                 this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.WritePercent)
                                                                     .SetNumericFormat("##0.00%");

                                                                 this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablesMax)
                                                                    .SetNumericFormat("#,###,###,##0");
                                                                 this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablesMin)
                                                                    .SetNumericFormat("#,###,###,##0");
                                                                 this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.SSTablesAvg)
                                                                    .SetNumericFormat("#,###,###,##0.00");
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
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                 this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.TombstoneRatioMin)
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                 this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.TombstoneRatioAvg)
                                                                    .SetNumericFormat("#,###,###,##0.00");
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
                                                                    .SetNumericFormat("#,###,###,##0.0000");
                                                                 this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.PartitionSizeMin)
                                                                    .SetNumericFormat("#,###,###,##0.0000");
                                                                 this.DataTable.GetColumn(DT.TaggedItemsDataTable.Columns.PartitionSizeAvg)
                                                                    .SetNumericFormat("#,###,###,##0.0000");
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

                                                                 //this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.ReconciliationRef);

                                                                 workSheet.UpdateWorksheet(this.DataTable, 1);

                                                                 workSheet.AutoFitColumn(this.DataTable);

                                                                 var table = workSheet.Tables.FirstOrDefault(t => t.Name == "TaggedItmesTable");
                                                                 var rowCnt = this.DataTable.Rows.Count;

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
                                                                     using (var tblRange = workSheet.TranslaateToColumnRange(this.DataTable,
                                                                                                                                DSEDiagnosticToDataTable.ColumnNames.KeySpace,
                                                                                                                                DSEDiagnosticToDataTable.ColumnNames.ReconciliationRef,
                                                                                                                                1,
                                                                                                                                rowCnt + 2))
                                                                     {
                                                                         table = workSheet.Tables.Add(tblRange, workSheet.Name == this.WorkSheetName ? "TaggedItmesTable" : workSheet.Name + "Table");

                                                                         table.ShowFilter = true;
                                                                         table.ShowHeader = true;
                                                                         table.TableStyle = OfficeOpenXml.Table.TableStyles.Light21;
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
                                                            this.UseDataTableDefaultView,
                                                            appendToWorkSheet: this.AppendToWorkSheet,
                                                           cachePackage: LibrarySettings.ExcelPackageCache,
                                                           saveWorkSheet: LibrarySettings.ExcelSaveWorkSheet,
                                                           excelTemplateFile: this.ExcelTemplateWorkbook);

            return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, nbrRows);
        }
    }
}
