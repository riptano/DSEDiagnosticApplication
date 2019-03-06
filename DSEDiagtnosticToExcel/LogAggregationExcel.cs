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
    public sealed class LogAggregationExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.LogAggregation;

        public LogAggregationExcel(DataTable loginfoDataTable,
                                    IFilePath excelTargetWorkbook,
                                    IFilePath excelTemplateWorkbook,
                                    string worksheetName,
                                    bool useDataTableDefaultView)
            : base(loginfoDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        { }

        public LogAggregationExcel(DataTable loginfoDataTable,
                                    IFilePath excelTargetWorkbook,
                                    IFilePath excelTemplateWorkbook = null)
            : this(loginfoDataTable, excelTargetWorkbook, excelTemplateWorkbook, null, true)
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
                                                                 
                                                                 this.DataTable.GetColumn(DT.ColumnNames.UTCTimeStamp)
                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelDateTimeFormat);
                                                                 this.DataTable.GetColumn(DT.ColumnNames.LogLocalTimeStamp)
                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelDateTimeFormat);
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.AggregationPeriod)
                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelTimeSpanFormat);
                                                                 //this.DataTable.GetColumn(DT.ColumnNames.DataCenter);
                                                                 //this.DataTable.GetColumn(DT.ColumnNames.NodeIPAddress);
                                                                 //this.DataTable.GetColumn(DT.ColumnNames.KeySpace);
                                                                 //this.DataTable.GetColumn(DT.ColumnNames.Table);
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.Class)
                                                                    .SetWidth(35);
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.RelatedInfo)
                                                                    .SetWidth(20);
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.HasOrphanedEvents);
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.Path)
                                                                    .SetWidth(20);
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.Exception)
                                                                    .SetWidth(25);
                                                                 
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.LastOccurrenceUTC)
                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelDateTimeFormat);
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.LastOccurrenceLocal)
                                                                    .SetNumericFormat(Properties.Settings.Default.ExcelDateTimeFormat);

                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.Occurrences)
                                                                    .SetNumericFormat("#,###,###,##0");

                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.UOM)
                                                                    .SetWidth(20);
                                                                 
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.DurationMax)
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.DurationMin)
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.DurationMean)
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.DurationStdDevp)
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.DurationTotal)
                                                                    .SetNumericFormat("#,###,###,##0.000");

                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.RateMax)
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.RateMin)
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.RateMean)
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.RateStdDevp)
                                                                    .SetNumericFormat("#,###,###,##0.000");
                                                                 
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.PendingTombstone)
                                                                    .SetNumericFormat("#,###,###,##0");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.CompletedLive)
                                                                    .SetNumericFormat("#,###,###,##0");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.Blocked)
                                                                    .SetNumericFormat("#,###,###,##0");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.AllTimeBlocked)
                                                                    .SetNumericFormat("#,###,###,##0");
                                                                 
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.EdenThresholdReadRateMax)
                                                                    .SetNumericFormat("#,###,###,##0.00####")
                                                                    .SetCaption("Eden Max|Threshold Max|Read Rate Max");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.EdenThresholdReadRateMin)
                                                                    .SetNumericFormat("#,###,###,##0.00####")
                                                                    .SetCaption("Eden Dif|Threshold Min|Read Rate Min");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.OldThresholdReadRateMean)
                                                                    .SetNumericFormat("#,###,###,##0.00####")
                                                                    .SetCaption("Old Max|Threshold Mean|Read Rate Mean");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.OldThresholdReadRateStdDevp)
                                                                    .SetNumericFormat("#,###,###,##0.00####")
                                                                    .SetCaption("Old Dif|Threshold StdDevp|Read Rate StdDevp");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.SurvivorMax)
                                                                    .SetNumericFormat("#,###,###,##0.00####");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.SurvivorDif)
                                                                    .SetNumericFormat("#,###,###,##0.00####");
                                                                
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.SizeMax)
                                                                    .SetNumericFormat("#,###,###,##0.00####");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.SizeMin)
                                                                    .SetNumericFormat("#,###,###,##0.00####");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.SizeMean)
                                                                    .SetNumericFormat("#,###,###,##0.00####");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.SizeStdDevp)
                                                                    .SetNumericFormat("#,###,###,##0.00####");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.SizeTotal)
                                                                    .SetNumericFormat("#,###,###,##0.00####");

                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.CountMax)
                                                                    .SetNumericFormat("#,###,###,##0");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.CountMin)
                                                                    .SetNumericFormat("#,###,###,##0");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.CountMean)
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.CountStdDevp)
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.CountTotal)
                                                                    .SetNumericFormat("#,###,###,##0");

                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.OPSMax)
                                                                    .SetNumericFormat("#,###,###,##0");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.OPSMin)
                                                                    .SetNumericFormat("#,###,###,##0");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.OPSMean)
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.OPSStdDevp)
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.SerializedMax)
                                                                    .SetNumericFormat("#,###,###,##0.00####");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.SerializedMin)
                                                                    .SetNumericFormat("#,###,###,##0.00####");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.SerializedMean)
                                                                    .SetNumericFormat("#,###,###,##0.00####");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.SerializedStdDevp)
                                                                    .SetNumericFormat("#,###,###,##0.00####");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.SerializedTotal)
                                                                    .SetNumericFormat("#,###,###,##0.00####");

                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.StorageMax)
                                                                    .SetNumericFormat("#,###,###,##0.00####");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.StorageMin)
                                                                    .SetNumericFormat("#,###,###,##0.00####");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.StorageMean)
                                                                    .SetNumericFormat("#,###,###,##0.00####");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.StorageStdDevp)
                                                                    .SetNumericFormat("#,###,###,##0.00####");
                                                                 this.DataTable.GetColumn(DT.LogAggregationDataTable.Columns.StorageTotal)
                                                                    .SetNumericFormat("#,###,###,##0.00####");

                                                                 workSheet.UpdateWorksheet(this.DataTable, 1);

                                                                 workSheet.AutoFitColumn(this.DataTable);

                                                                 workSheet.CreateExcelTable(this.DataTable, "AggregatedLogTable");                                                                 
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

