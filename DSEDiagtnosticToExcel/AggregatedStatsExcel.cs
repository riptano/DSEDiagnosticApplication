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
    public sealed class AggregatedStatsExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.AggregatedStats;

        public AggregatedStatsExcel(DataTable keyspaceDataTable,
                                    IFilePath excelTargetWorkbook,
                                    IFilePath excelTemplateWorkbook,
                                    string worksheetName,
                                    bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        { }

        public AggregatedStatsExcel(DataTable keyspaceDataTable,
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
                                                                 //workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                 //workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                 //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                 //workSheet.Cells["K:K"].Style.Numberformat.Format = "#,###,###,##0.00";

                                                                 //workSheet.Cells["I1"].Value = workSheet.Cells["I1"].Text + "(Formatted)";
                                                                 //workSheet.View.FreezePanes(2, 1);
                                                                 //workSheet.Cells["A1:M1"].AutoFilter = true;
                                                                 //workSheet.Column(10).Hidden = true;

                                                                 //int nbrLoaded = 0;

                                                                 //if(!this.AppendToWorkSheet)
                                                                 //{
                                                                 //    nbrloaded = this.LoadDefaultAttributes(workSheet);
                                                                 //}

                                                                 workSheet.AutoFitColumn(workSheet.Cells["A:N"]);

                                                                 var table = workSheet.Tables.FirstOrDefault(t => t.Name == "AggregatedStatsTable");
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
                                                                     using (var tblRange = workSheet.Cells[string.Format("A1:N{0}",rowCnt + 2)])
                                                                     {
                                                                         table = workSheet.Tables.Add(tblRange, workSheet.Name == this.WorkSheetName ? "AggregatedStatsTable" : workSheet.Name + "Table");

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
