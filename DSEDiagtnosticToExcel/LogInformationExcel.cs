using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;
using Common;
using OfficeOpenXml;
using DataTableToExcel;

namespace DSEDiagtnosticToExcel
{
    public sealed class LogInformationExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.LogInfo;

        public LogInformationExcel(DataTable loginfoDataTable,
                                    IFilePath excelTargetWorkbook,
                                    string worksheetName,
                                    bool useDataTableDefaultView)
            : base(loginfoDataTable, excelTargetWorkbook, worksheetName, useDataTableDefaultView)
        { }

        public LogInformationExcel(DataTable loginfoDataTable,
                                    IFilePath excelTargetWorkbook)
            : this(loginfoDataTable, excelTargetWorkbook, null, true)
        { }

        public override Tuple<IFilePath, string, int> Load()
        {
            var nbrRows = DataTableToExcel.Helpers.WorkBook(this.ExcelTargetWorkbook.PathResolved, this.WorkSheetName, this.DataTable,
                                                            (stage, orgFilePath, targetFilePath, workSheetName, excelPackage, excelDataTable, rowCount) =>
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
                                                                 workSheet.Cells["K:K"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Right;
                                                                 workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                 workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                 //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                 workSheet.View.FreezePanes(2, 1);

                                                                 workSheet.Cells["E:E"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss";
                                                                 workSheet.Cells["F:F"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss";
                                                                 workSheet.Cells["G:G"].Style.Numberformat.Format = "d hh:mm";
                                                                 workSheet.Cells["G:G"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                 workSheet.Cells["H:H"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss";
                                                                 workSheet.Cells["I:I"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss";
                                                                 workSheet.Cells["J:J"].Style.Numberformat.Format = "d hh:mm";
                                                                 workSheet.Cells["J:J"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                 workSheet.Cells["L:L"].Style.Numberformat.Format = "#,###,###,##0";
                                                                 workSheet.Cells["M:M"].Style.Numberformat.Format = "#,###,###,##0.00";

                                                                 workSheet.Cells["A1:N1"].AutoFilter = true;
                                                                 workSheet.AutoFitColumn(workSheet.Cells["A:J"], workSheet.Cells["L:N"]);
                                                                 workSheet.Column(11).Width = 27; //k
                                                             },
                                                             -1,
                                                            -1,
                                                            "A1",
                                                            this.UseDataTableDefaultView);

            return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, nbrRows);
        }
    }
}
