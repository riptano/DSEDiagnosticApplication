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
    public sealed class DataCenterExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.DataCenters;

        public DataCenterExcel(DataTable dcDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook,
                                string worksheetName,
                                bool useDataTableDefaultView)
            : base(dcDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        {
            this.AppendToWorkSheet = false;
        }

        public DataCenterExcel(DataTable keyspaceDataTable,
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
                                                                            var rangeAddress = loadRange == null ? null : workSheet?.Cells[loadRange];

                                                                            if (rangeAddress != null)
                                                                            {
                                                                                var startRow = rangeAddress.Start.Row;
                                                                                var endRow = rangeAddress.End.Row;
                                                                                string lastValue = string.Empty;
                                                                                string currentValue;
                                                                                bool formatOn = false;

                                                                                for (int nRow = startRow; nRow <= endRow; ++nRow)
                                                                                {
                                                                                    if (workSheet.Cells[nRow, 1] != null)
                                                                                    {
                                                                                        currentValue = workSheet.Cells[nRow, 1].Value as string;
                                                                                        if (currentValue != null)
                                                                                        {
                                                                                            if (lastValue == null)
                                                                                            {
                                                                                                lastValue = currentValue;
                                                                                                formatOn = false;
                                                                                            }
                                                                                            else if (lastValue != currentValue)
                                                                                            {
                                                                                                lastValue = currentValue;
                                                                                                formatOn = !formatOn;
                                                                                            }

                                                                                            if (formatOn)
                                                                                            {
                                                                                                workSheet.Row(nRow).Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.Solid;
                                                                                                workSheet.Row(nRow).Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                                            }
                                                                                            else
                                                                                            {
                                                                                                workSheet.Row(nRow).Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.None;
                                                                                            }
                                                                                        }
                                                                                    }
                                                                                }
                                                                            }

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
                                                            workSheet =>
                                                            {
                                                                workSheet.Cells["1:3"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                workSheet.Cells["1:3"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                //workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);

                                                                this.DataTable.GetColumn("Data Center")
                                                                                .SetCaption("Name");
                                                                
                                                                this.DataTable.SetGroupHeader("Total", -1, false,
                                                                   this.DataTable.GetColumn("Total Nodes")
                                                                                    .SetCaption("Nodes")
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .TotalColumn());

                                                                this.DataTable.SetGroupHeader("Workload Types (Nodes)", -2, true,
                                                                    this.DataTable.GetColumn("Cassandra")                                                                                   
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .TotalColumn(),
                                                                    this.DataTable.GetColumn("Search")                                                                                    
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .TotalColumn(),
                                                                    this.DataTable.GetColumn("Analytics")
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .TotalColumn(),
                                                                    this.DataTable.GetColumn("Graph")
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .TotalColumn(),
                                                                    this.DataTable.GetColumn("Multi-Instance")
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .TotalColumn(),
                                                                    this.DataTable.GetColumn("Other")
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .TotalColumn()
                                                                );

                                                                this.DataTable.SetGroupHeader("Status (Nodes)", -2, true,
                                                                    this.DataTable.GetColumn("Status Up")
                                                                                    .SetCaption("Up")
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .TotalColumn(),
                                                                    this.DataTable.GetColumn("Status Down")
                                                                                    .SetCaption("Down")
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .TotalColumn(),
                                                                    this.DataTable.GetColumn("Status Unknown")
                                                                                    .SetCaption("Unknown")
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .TotalColumn()
                                                                    );

                                                                //Device Utilization 
                                                                this.DataTable.SetGroupHeader("Device Utilization", -2, true,
                                                                    this.DataTable.GetColumn("DU Max")
                                                                                    .SetCaption("Max")
                                                                                    .SetNumericFormat("##0.00%"),
                                                                    this.DataTable.GetColumn("DU Max-Node")
                                                                                    .SetCaption("Max-Node"),
                                                                    this.DataTable.GetColumn("DU Min")
                                                                                    .SetCaption("Min")
                                                                                    .SetNumericFormat("##0.00%"),
                                                                    this.DataTable.GetColumn("DU Min-Node")
                                                                                    .SetCaption("Min-Node"),
                                                                    this.DataTable.GetColumn("DU Avg")
                                                                                    .SetCaption("Avg")
                                                                                    .SetNumericFormat("##0.00%")
                                                                    );

                                                                //Storage
                                                                this.DataTable.SetGroupHeader("Storage (MB)", -2, true,
                                                                    this.DataTable.GetColumn("Storage Max")
                                                                                    .SetCaption("Max")
                                                                                    .SetNumericFormat("#,###,###,##0.0000"),
                                                                    this.DataTable.GetColumn("Storage Max-Node")
                                                                                    .SetCaption("Max-Node"),
                                                                    this.DataTable.GetColumn("Storage Min")
                                                                                    .SetCaption("Min")
                                                                                    .SetNumericFormat("#,###,###,##0.0000"), 
                                                                    this.DataTable.GetColumn("Storage Min-Node")
                                                                                    .SetCaption("Min-Node"),
                                                                    this.DataTable.GetColumn("Storage Avg")
                                                                                    .SetCaption("Avg")
                                                                                    .SetNumericFormat("#,###,###,##0.0000"),
                                                                    this.DataTable.GetColumn("Storage Total")
                                                                                    .SetCaption("Total")
                                                                                    .SetNumericFormat("#,###,###,##0.0000")
                                                                                    .TotalColumn(),//u
                                                                    this.DataTable.GetColumn("Storage Total (User)")
                                                                                    .SetCaption("Total (User)")
                                                                                    .SetNumericFormat("#,###,###,##0.0000")
                                                                                    .TotalColumn(),//v
                                                                    this.DataTable.GetColumn("Storage Percent")
                                                                                    .SetCaption("Percent-Cluster")
                                                                                    .SetNumericFormat("##0.00%")
                                                                                );

                                                                //SSTables
                                                                this.DataTable.SetGroupHeader("SSSTables", -2, true,
                                                                   this.DataTable.GetColumn("SSTables Max")
                                                                                    .SetCaption("Max")
                                                                                    .SetNumericFormat("#,###,###,##0"),
                                                                   this.DataTable.GetColumn("SSTables Max-Node")
                                                                                    .SetCaption("Max-Node"),
                                                                   this.DataTable.GetColumn("SSTables Min")
                                                                                    .SetCaption("Min")
                                                                                    .SetNumericFormat("#,###,###,##0"),
                                                                   this.DataTable.GetColumn("SSTables Min-Node")
                                                                                    .SetCaption("Min-Node"),
                                                                   this.DataTable.GetColumn("SSTables Avg")
                                                                                    .SetCaption("Avg")
                                                                                    .SetNumericFormat("#,###,###,##0.00"),
                                                                   this.DataTable.GetColumn("SSTables Total")
                                                                                    .SetCaption("Total")
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .TotalColumn(),//Ac
                                                                   this.DataTable.GetColumn("SSTables Total (User)")
                                                                                    .SetCaption("Total (User)")
                                                                                    .SetNumericFormat("#,###,###,##0")
                                                                                    .TotalColumn(),//Ad
                                                                   this.DataTable.GetColumn("SSTables Percent")
                                                                                    .SetCaption("Percent-Cluster")
                                                                                    .SetNumericFormat("##0.00%")
                                                                                    );
                                                                //Distribution
                                                                this.DataTable.SetGroupHeader("Distribution", -2, true,
                                                                    this.DataTable.SetGroupHeader("Storage", -1, true,
                                                                       this.DataTable.GetColumn("Distribution Storage From")
                                                                                        .SetCaption("From")
                                                                                        .SetNumericFormat("##0.00%"),
                                                                       this.DataTable.GetColumn("Distribution Storage To")
                                                                                        .SetCaption("To")
                                                                                        .SetNumericFormat("##0.00%") 
                                                                                        ),
                                                                    this.DataTable.SetGroupHeader("Keys", -1, true,
                                                                       this.DataTable.GetColumn("Distribution Keys From")
                                                                                        .SetCaption("From")
                                                                                        .SetNumericFormat("##0.00%"),
                                                                       this.DataTable.GetColumn("Distribution Keys To")
                                                                                        .SetCaption("To")
                                                                                        .SetNumericFormat("##0.00%")
                                                                                        ),
                                                                    this.DataTable.SetGroupHeader("SSTables", -1, true,
                                                                       this.DataTable.GetColumn("Distribution SSTables From")
                                                                                        .SetCaption("From")
                                                                                        .SetNumericFormat("##0.00%"),//aj
                                                                       this.DataTable.GetColumn("Distribution SSTables To")
                                                                                        .SetCaption("To")
                                                                                        .SetNumericFormat("##0.00%")
                                                                ));

                                                                //Counts
                                                                this.DataTable.SetGroupHeader("Cell Counts", -2, true,
                                                                    this.DataTable.SetGroupHeader("Read", -1, true,
                                                                       this.DataTable.GetColumn("Reads Max")
                                                                                        .SetCaption("Max")
                                                                                        .SetNumericFormat("#,###,###,##0"),
                                                                       this.DataTable.GetColumn("Reads Max-Node")
                                                                                        .SetCaption("Max-Node"),//am
                                                                       this.DataTable.GetColumn("Reads Min")
                                                                                        .SetCaption("Min")
                                                                                        .SetNumericFormat("#,###,###,##0"),
                                                                       this.DataTable.GetColumn("Reads Min-Node")
                                                                                        .SetCaption("Min-Node"),
                                                                       this.DataTable.GetColumn("Reads Avg")
                                                                                        .SetCaption("Avg")
                                                                                        .SetNumericFormat("#,###,###,##0.00"),
                                                                       this.DataTable.GetColumn("Reads Total")
                                                                                        .SetCaption("Total")
                                                                                        .SetNumericFormat("#,###,###,##0")
                                                                                        .TotalColumn(),
                                                                       this.DataTable.GetColumn("Reads Total (User)")
                                                                                        .SetCaption("Total (User)")
                                                                                        .SetNumericFormat("#,###,###,##0")
                                                                                        .TotalColumn(),
                                                                       this.DataTable.GetColumn("Reads Percent")
                                                                                        .SetCaption("Percent-Cluster")
                                                                                        .SetNumericFormat("##0.00%")
                                                                                        ),
                                                                    this.DataTable.SetGroupHeader("Write", -1, true,
                                                                       this.DataTable.GetColumn("Writes Max")
                                                                                        .SetCaption("Max")
                                                                                        .SetNumericFormat("#,###,###,##0"),
                                                                       this.DataTable.GetColumn("Writes Max-Node")
                                                                                        .SetCaption("Max-Node"),
                                                                       this.DataTable.GetColumn("Writes Min")
                                                                                        .SetCaption("Min")
                                                                                        .SetNumericFormat("#,###,###,##0"),
                                                                       this.DataTable.GetColumn("Writes Min-Node")
                                                                                        .SetCaption("Min-Node"),
                                                                       this.DataTable.GetColumn("Writes Avg")
                                                                                        .SetCaption("Avg")
                                                                                        .SetNumericFormat("##0.00%"),
                                                                       this.DataTable.GetColumn("Writes Total")
                                                                                        .SetCaption("Total")
                                                                                        .SetNumericFormat("#,###,###,##0")
                                                                                        .TotalColumn(),
                                                                       this.DataTable.GetColumn("Writes Total (User)")
                                                                                        .SetCaption("Total (User)")
                                                                                        .SetNumericFormat("#,###,###,##0")
                                                                                        .TotalColumn(),
                                                                       this.DataTable.GetColumn("Writes Percent")
                                                                                        .SetCaption("Percent-Cluster")
                                                                                        .SetNumericFormat("##0.00%")
                                                                 ));
                                                                
                                                                workSheet.UpdateWorksheet(this.DataTable, 3);

                                                                workSheet.View.FreezePanes(4, 2);
                                                                workSheet.Cells["A3:BB3"].AutoFilter = true;   
                                                                                                                                
                                                                workSheet.AutoFitColumn();
                                                            },
                                                            -1,
                                                           -1,
                                                           "A3",
                                                           this.UseDataTableDefaultView,
                                                           appendToWorkSheet: this.AppendToWorkSheet,
                                                           cachePackage: LibrarySettings.ExcelPackageCache,
                                                           saveWorkSheet: LibrarySettings.ExcelSaveWorkSheet,
                                                           excelTemplateFile: this.ExcelTemplateWorkbook);

            return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, nbrRows);
        }
    }
}
