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
    public abstract class PFExcel : LoadToExcel
    {
        public PFExcel(DataTable keyspaceDataTable,
                                    IFilePath excelTargetWorkbook,
                                    IFilePath excelTemplateWorkbook,
                                    string worksheetName,
                                    bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        { }

        public PFExcel(DataTable keyspaceDataTable,
                                    IFilePath excelTargetWorkbook,
                                    IFilePath excelTemplateWorkbook = null)
            : this(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, null, true)
        { }

        public void AddDCColumns()
        {
            var dcColumns = new List<DataColumn>();
            
            foreach (DataColumn dataColumn in this.DataTable.Columns)
            {
                if(dataColumn.ExtendedProperties.ContainsKey("SubTableTag"))
                {
                    dataColumn
                        .SetComment(dataColumn.ColumnName)
                        .SetNumericFormat("#,###,###,##0.00")
                        .SetConditionalFormat(Properties.Settings.Default.CondFmtJsonCommonKeyPartFactor);
                    dcColumns.Add(dataColumn);
                }
            }
            
            this.DataTable.SetGroupHeader("DataCenters", -1, true,
                dcColumns.ToArray()
            );

        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns>Returns the Row starting Position</returns>
        public abstract int FormatColumns();

        public override Tuple<IFilePath, string, int> Load()
        {
            this.CallActionEvent("PreLoading");

            this.AddDCColumns();
            var nStartRow = this.FormatColumns();

            var nbrRows = DataTableToExcel.Helpers.WorkBook(this.ExcelTargetWorkbook.PathResolved, this.DataTable.TableName, this.DataTable,
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
                                                                            workSheet.AltFileFillRow(nStartRow + 1,
                                                                                                       excelDataTable.GetColumn(DT.TaggedDCDataTable.Columns.KSCSType),
                                                                                                       null,
                                                                                                       excelDataTable.GetColumn(DT.ColumnNames.Table),
                                                                                                       excelDataTable.GetColumn(DT.TaggedDCDataTable.Columns.KSCSType),
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
                                                         (workSheet, splitNbr) =>
                                                         {
                                                             workSheet.Cells[string.Format("1:{0}", nStartRow - 1)].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                             workSheet.Cells[string.Format("1:{0}", nStartRow)].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                             //workSheet.Cells["1:2"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);

                                                             workSheet.UpdateWorksheet(this.DataTable, nStartRow);

                                                             workSheet.View.FreezePanes(nStartRow + 1, 1);

                                                             workSheet.ExcelRange(nStartRow,
                                                                                      this.DataTable.GetColumn(DT.ColumnNames.Table),
                                                                                      this.DataTable.GetColumn(DT.TaggedDCDataTable.Columns.BaseTableFactor))
                                                                            .First().AutoFilter = true;
                                                             workSheet.AutoFitColumn();
                                                         },
                                                         -1,
                                                        -1,
                                                        string.Format("A{0}", nStartRow),
                                                        this.UseDataTableDefaultView,
                                                        appendToWorkSheet: this.AppendToWorkSheet,
                                                       cachePackage: LibrarySettings.ExcelPackageCache,
                                                       saveWorkSheet: LibrarySettings.ExcelSaveWorkSheet,
                                                       excelTemplateFile: this.ExcelTemplateWorkbook);

            return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, nbrRows);
        }
    }
}
