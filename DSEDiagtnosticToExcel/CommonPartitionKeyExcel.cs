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
    public sealed class CommonPartitionKeyExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.CommonPartitionKey;

        public CommonPartitionKeyExcel(DataTable keyspaceDataTable,
                                        IFilePath excelTargetWorkbook,
                                        IFilePath excelTemplateWorkbook,
                                        string worksheetName,
                                        bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        {
            this.AppendToWorkSheet = false;
        }

        public CommonPartitionKeyExcel(DataTable keyspaceDataTable,
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

                                                                            workSheet.AltFileFillRow(2,
                                                                                                       this.DataTable.GetColumn("Partition Key"));
                                                                            
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
                                                                 workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                 workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                 //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                 workSheet.View.FreezePanes(2, 1);

                                                                 this.DataTable.GetColumn(DT.CommonPartitionKeyDataTable.Columns.PartitionKey)
                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartVar);

                                                                 this.DataTable.GetColumn(DT.CommonPartitionKeyDataTable.Columns.ReadCount)
                                                                   .SetNumericFormat("#,###,###,##0")
                                                                   .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightGreen);
                                                                 this.DataTable.GetColumn(DT.CommonPartitionKeyDataTable.Columns.ReadStdDev)
                                                                   .SetNumericFormat("#,###,###,##0.00")
                                                                   .SetCaption("Read Dev");
                                                                 this.DataTable.GetColumn(DT.CommonPartitionKeyDataTable.Columns.ReadFactor)
                                                                   .SetNumericFormat("#,###,###,##0.00")
                                                                   .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);
                                                                 this.DataTable.GetColumn(DT.CommonPartitionKeyDataTable.Columns.WriteCount)
                                                                   .SetNumericFormat("#,###,###,##0")
                                                                   .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightGreen);
                                                                 this.DataTable.GetColumn(DT.CommonPartitionKeyDataTable.Columns.WriteStdDev)
                                                                   .SetNumericFormat("#,###,###,##0.00")
                                                                   .SetCaption("Writes Dev");
                                                                 this.DataTable.GetColumn(DT.CommonPartitionKeyDataTable.Columns.WriteFactor)
                                                                   .SetNumericFormat("#,###,###,##0.00")
                                                                   .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);
                                                                 this.DataTable.GetColumn(DT.CommonPartitionKeyDataTable.Columns.PartitionKeysCount)
                                                                   .SetNumericFormat("#,###,###,##0")
                                                                   .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightGreen);
                                                                 this.DataTable.GetColumn(DT.CommonPartitionKeyDataTable.Columns.PartitionKeysStdDev)
                                                                   .SetNumericFormat("#,###,###,##0.00")
                                                                   .SetCaption("Partition Keys Dev");
                                                                 this.DataTable.GetColumn(DT.CommonPartitionKeyDataTable.Columns.PartitionKeysFactor)
                                                                   .SetNumericFormat("#,###,###,##0.00")
                                                                   .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);

                                                                 this.DataTable.GetColumn(DT.CommonPartitionKeyDataTable.Columns.Storage)
                                                                   .SetNumericFormat("#,###,###,##0.00##")
                                                                   .SetCaption("Storage (MB)")
                                                                   .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonDataBarLightBlue);
                                                                 this.DataTable.GetColumn(DT.CommonPartitionKeyDataTable.Columns.Tables)
                                                                   .SetNumericFormat("#,###,###,##0");
                                                                 this.DataTable.GetColumn(DT.CommonPartitionKeyDataTable.Columns.Factor)
                                                                   .SetNumericFormat("#,###,###,##0.00")
                                                                   .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);
                                                                 
                                                                 workSheet.UpdateWorksheet(this.DataTable, 1);

                                                                 workSheet.AutoFitColumn(this.DataTable);

                                                                 workSheet.TranslaateToColumnRange(this.DataTable,
                                                                                                    DT.CommonPartitionKeyDataTable.Columns.PartitionKey,
                                                                                                    DT.CommonPartitionKeyDataTable.Columns.Factor,
                                                                                                    1, 1).AutoFilter = true;                                                               
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
