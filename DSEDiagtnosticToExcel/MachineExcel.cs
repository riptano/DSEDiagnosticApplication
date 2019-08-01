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
    public sealed class MachineExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.Machine;

        public MachineExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook,
                                string worksheetName,
                                bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        {
            this.AppendToWorkSheet = false;
        }

        public MachineExcel(DataTable keyspaceDataTable,
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
                                                                                                       this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.DataCenter));
                                                                            
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
                                                                workSheet.View.FreezePanes(3, 2);

                                                                //this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.NodeIPAddress);
                                                                this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.DataCenter)
                                                                    .SetConditionalFormat(Properties.Settings.Default.CondFmtMachineDCUnknown);

                                                                //this.DataTable.GetColumn("Cloud-VM Type");
                                                                //this.DataTable.GetColumn("CPU Architecture");
                                                                this.DataTable.GetColumn("Cores")
                                                                    .SetNumericFormat("#0");
                                                                this.DataTable.GetColumn("Physical Memory (MB)")
                                                                    .SetNumericFormat("#,###,###,##0.00");
                                                                //this.DataTable.GetColumn("OS");
                                                                //this.DataTable.GetColumn("OS Version");
                                                                //this.DataTable.GetColumn("Kernel");
                                                                //this.DataTable.GetColumn("TimeZone");

                                                                //CPU Load
                                                                this.DataTable.SetGroupHeader("CPU Load(Percent)", -2, true,
                                                                   this.DataTable.GetColumn("Average")
                                                                        .SetNumericFormat("##0.00%"),
                                                                    this.DataTable.GetColumn("Idle")
                                                                        .SetNumericFormat("##0.00%"),
                                                                    this.DataTable.GetColumn("System")
                                                                        .SetNumericFormat("##0.00%"),
                                                                    this.DataTable.GetColumn("User")
                                                                        .SetNumericFormat("##0.00%"),
                                                                    this.DataTable.GetColumn("IOWait")
                                                                        .SetNumericFormat("##0.00%"),
                                                                    this.DataTable.GetColumn("StealTime")
                                                                        .SetNumericFormat("##0.00%")
                                                                    );

                                                                //Memory
                                                                this.DataTable.SetGroupHeader("Memory (MB)", -2, true,
                                                                    this.DataTable.GetColumn("Available")
                                                                        .SetNumericFormat("#,###,###,##0.00"),
                                                                    this.DataTable.GetColumn("Cache")
                                                                        .SetNumericFormat("#,###,###,##0.00"),
                                                                    this.DataTable.GetColumn("Buffers")
                                                                        .SetNumericFormat("#,###,###,##0.00"),
                                                                    this.DataTable.GetColumn("Shared")
                                                                        .SetNumericFormat("#,###,###,##0.00"),
                                                                    this.DataTable.GetColumn("Free")
                                                                        .SetNumericFormat("#,###,###,##0.00"),
                                                                    this.DataTable.GetColumn("Used")
                                                                        .SetNumericFormat("#,###,###,##0.00")
                                                                    );

                                                                //Java
                                                                this.DataTable.SetGroupHeader("Java", -2, true,
                                                                    this.DataTable.GetColumn("Vendor"),
                                                                    this.DataTable.GetColumn("Model"),
                                                                    this.DataTable.GetColumn("Runtime Name"),
                                                                    this.DataTable.GetColumn("Runtime Version"),
                                                                    this.DataTable.GetColumn("GC")
                                                                );

                                                                //Java NonHeapMemoryUsage
                                                                this.DataTable.SetGroupHeader("Java Non-Heap (MB)", -2, true,
                                                                    this.DataTable.GetColumn("Non-Heap Committed")
                                                                        .SetCaption("Committed")
                                                                        .SetNumericFormat("#,###,###,##0.00"),
                                                                    this.DataTable.GetColumn("Non-Heap Init")
                                                                        .SetCaption("Init")
                                                                        .SetNumericFormat("#,###,###,##0.00"),
                                                                    this.DataTable.GetColumn("Non-Heap Max")
                                                                        .SetCaption("Max")
                                                                        .SetNumericFormat("#,###,###,##0.00"),
                                                                    this.DataTable.GetColumn("Non-Heap Used")
                                                                        .SetCaption("Used")
                                                                        .SetNumericFormat("#,###,###,##0.00")
                                                                );

                                                                //Java HeapMemoryUsage
                                                                this.DataTable.SetGroupHeader("Java Heap (MB)", -2, true,
                                                                    this.DataTable.GetColumn("Heap Committed")
                                                                        .SetCaption("Committed")
                                                                        .SetNumericFormat("#,###,###,##0.00"),
                                                                    this.DataTable.GetColumn("Heap Init")
                                                                        .SetCaption("Init")
                                                                        .SetNumericFormat("#,###,###,##0.00"),
                                                                    this.DataTable.GetColumn("Heap Max")
                                                                        .SetCaption("Max")
                                                                        .SetNumericFormat("#,###,###,##0.00"),
                                                                    this.DataTable.GetColumn("Heap Used")
                                                                        .SetCaption("Used")
                                                                        .SetNumericFormat("#,###,###,##0.00")
                                                                    );

                                                                //NTP
                                                                this.DataTable.SetGroupHeader("NTP", -2, true,
                                                                    this.DataTable.GetColumn("Correction (ms)")
                                                                        .SetNumericFormat("#,###,###,##0"),
                                                                    this.DataTable.GetColumn("Polling (secs)")
                                                                        .SetNumericFormat("#,###,###,##0"),
                                                                    this.DataTable.GetColumn("Maximum Error (us)")
                                                                        .SetNumericFormat("#,###,###,##0"),
                                                                    this.DataTable.GetColumn("Estimated Error (us)")
                                                                        .SetNumericFormat("#,###,###,##0"),
                                                                    this.DataTable.GetColumn("Time Constant")
                                                                        .SetNumericFormat("#,###,###,##0"),
                                                                    this.DataTable.GetColumn("Precision (us)")
                                                                        .SetNumericFormat("#,###,###,##0"),
                                                                    this.DataTable.GetColumn("Frequency (ppm)")
                                                                            .SetNumericFormat("#,###,###,##0"),
                                                                    this.DataTable.GetColumn("Tolerance (ppm)")
                                                                        .SetNumericFormat("#,###,###,##0")
                                                                    );
                                                            
                                                                //this.DataTable.GetColumn("Host Names");

                                                                
                                                                workSheet.UpdateWorksheet(this.DataTable, 2);

                                                                workSheet.ExcelRange(2,
                                                                                      this.DataTable.GetColumn(DSEDiagnosticToDataTable.ColumnNames.NodeIPAddress),
                                                                                      this.DataTable.GetColumn("Host Names"))
                                                                            .First().AutoFilter = true;

                                                                workSheet.AutoFitColumn();
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
