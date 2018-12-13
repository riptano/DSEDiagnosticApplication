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
                                                                 //this.DataTable.GetColumn("CQL Type");

                                                                 this.DataTable.GetColumn("Read Max")
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn("Read Min")
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn("Read Avg")
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn("Read StdDev")
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn("Read Factor")
                                                                    .SetNumericFormat("#,###,###,##0.00");

                                                                 this.DataTable.GetColumn("Read Percent")
                                                                     .SetNumericFormat("##0.00%");
                                                                 this.DataTable.GetColumn("Keys Percent")
                                                                     .SetNumericFormat("##0.00%");
                                                                 
                                                                 this.DataTable.GetColumn("Write Max")
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn("Write Min")
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn("Write Avg")
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn("Write StdDev")
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn("Write Factor")
                                                                    .SetNumericFormat("#,###,###,##0.00");

                                                                 this.DataTable.GetColumn("Write Percent")
                                                                     .SetNumericFormat("##0.00%");

                                                                 this.DataTable.GetColumn("SSTables Max")
                                                                    .SetNumericFormat("#,###,###,##0");
                                                                 this.DataTable.GetColumn("SSTables Min")
                                                                    .SetNumericFormat("#,###,###,##0");
                                                                 this.DataTable.GetColumn("SSTables Avg")
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                 this.DataTable.GetColumn("SSTables StdDev")
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                 this.DataTable.GetColumn("SSTables Factor")
                                                                    .SetNumericFormat("#,###,###,##0.00");

                                                                 this.DataTable.GetColumn("SSTable Percent")
                                                                     .SetNumericFormat("##0.00%");
                                                                 this.DataTable.GetColumn("Storage Percent")
                                                                     .SetNumericFormat("##0.00%");

                                                                 this.DataTable.GetColumn("Tombstone Ratio Max")
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                 this.DataTable.GetColumn("Tombstone Ratio Min")
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                 this.DataTable.GetColumn("Tombstone Ratio Avg")
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                 this.DataTable.GetColumn("Tombstone Ratio StdDev")
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                 this.DataTable.GetColumn("Tombstone Ratio Factor")
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                 this.DataTable.GetColumn("Tombstones Read")
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                 this.DataTable.GetColumn("Live Read")
                                                                    .SetNumericFormat("#,###,###,##0.00");

                                                                 this.DataTable.GetColumn("Partition Size Max")
                                                                    .SetNumericFormat("#,###,###,##0.0000");
                                                                 this.DataTable.GetColumn("Partition Size Min")
                                                                    .SetNumericFormat("#,###,###,##0.0000");
                                                                 this.DataTable.GetColumn("Partition Size Avg")
                                                                    .SetNumericFormat("#,###,###,##0.0000");
                                                                 this.DataTable.GetColumn("Partition Size StdDev")
                                                                    .SetNumericFormat("#,###,###,##0.0000");
                                                                 this.DataTable.GetColumn("Partition Size Factor")
                                                                    .SetNumericFormat("#,###,###,##0.00");

                                                                 this.DataTable.GetColumn("Flush Size Max")
                                                                    .SetNumericFormat("#,###,###,##0.0000");
                                                                 this.DataTable.GetColumn("Flush Size Min")
                                                                    .SetNumericFormat("#,###,###,##0.0000");
                                                                 this.DataTable.GetColumn("Flush Size Avg")
                                                                    .SetNumericFormat("#,###,###,##0.0000");
                                                                 this.DataTable.GetColumn("Flush Size StdDev")
                                                                    .SetNumericFormat("#,###,###,##0.0000");
                                                                 this.DataTable.GetColumn("Flush Size Factor")
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                 this.DataTable.GetColumn("Flush Pending")
                                                                    .SetNumericFormat("#,###,###,##0");
                                                                 
                                                                this.DataTable.GetColumn("Secondary Indexes")
                                                                     .SetNumericFormat("#,###,###,##0");
                                                                this.DataTable.GetColumn("Materialized Views")
                                                                     .SetNumericFormat("#,###,###,##0");
                                                                 //this.DataTable.GetColumn("Common Key");

                                                                 //this.DataTable.GetColumn("Base Table");
                                                                 this.DataTable.GetColumn("Base Table Weighted Factor Max")
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                 this.DataTable.GetColumn("Base Table Weighted Factor Avg")
                                                                    .SetNumericFormat("#,###,###,##0.00");

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
