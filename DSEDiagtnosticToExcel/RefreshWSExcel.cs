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
    public sealed class RefreshWSExcel : LoadToExcel
    {
        public const string DataTableName = "Refresh";

        public RefreshWSExcel(IFilePath excelTargetWorkbook,
                                        IFilePath excelTemplateWorkbook,
                                        string worksheetName)
            : base(null,
                    excelTargetWorkbook,
                    excelTemplateWorkbook,
                    worksheetName,
                    true)
        { }

        public RefreshWSExcel(IFilePath excelTargetWorkbook, IFilePath excelTemplateWorkbook = null)
            : this(excelTargetWorkbook,
                    excelTemplateWorkbook,
                    DataTableName)
        { }

        public override Tuple<IFilePath, string, int> Load()
        {
            
            using (var excelPkgCache = ExcelPkgCache.GetAddExcelPackageCache(this.ExcelTargetWorkbook, LibrarySettings.ExcelPackageCache, this.ExcelTemplateWorkbook))
            {
                var excelPkg = excelPkgCache.ExcelPackage;

                var wsInstance = excelPkg.Workbook.Worksheets[this.WorkSheetName];
                {
                    int row = Properties.Settings.Default.RefreshWSAttrRow;
                    var symbols = DSEDiagnosticLibrary.LibrarySettings.GetAttributeSymbols().ToArray();
                    wsInstance.Cells[row++, Properties.Settings.Default.RefreshWSAttrColumn].Value = "Symbol Legend";
                    foreach(var item in symbols)
                    {
                        wsInstance.Cells[row++, Properties.Settings.Default.RefreshWSAttrColumn].Value = string.Format("{0} -- {1}", item.Item1, item.Item2);
                    }
                    row++;
                    wsInstance.Cells[row++, Properties.Settings.Default.RefreshWSAttrColumn].Value = "Nodes can have symbols appended to the end of the name (IP-Address) where each position is a DC attribute for the following positions: 1) Node Uptime indicator, 2) System Log indicator, 3) Debug log indicator, 4) If Node Uptime matches System duration indicator, 5) Read Count indicator, 6) Write Count indicator, and 7) DSE work load type (optional, if not present Cassandra workload)";
                    wsInstance.Cells[row++, Properties.Settings.Default.RefreshWSAttrColumn].Value = string.Format("Node Example: 10.0.0.1 {0}{1}{2}{3}{4}{5}{6} -- 1) Evaluated Uptime for DC, 2) Low System Log Durations for DC, 3) Debug Log Duration typical for DC, 4) Node Uptime and Log duration do not match, 5) Read Counts are typical for DC, 6) Write Counts are below DC Threshold, 7) Search Node",
                                                                                                                    symbols[0].Item1,
                                                                                                                    symbols[1].Item1,
                                                                                                                    symbols[2].Item1,
                                                                                                                    symbols[11].Item1,
                                                                                                                    symbols[2].Item1,
                                                                                                                    symbols[0].Item1,
                                                                                                                    symbols[4].Item1);
                    wsInstance.Cells[row++, Properties.Settings.Default.RefreshWSAttrColumn].Value = string.Format("Node Example: 10.0.0.2 {0}{1}{2}{3}{4}{5}{6} -- 1) Uptime within Range for DC, 2) System Log Durations witin Range for DC, 3) No Debug Logs, 4) Node Uptime and System Log duration similar 4) Read Counts typical for DC, 5) Write Counts typical for DC, 6) Analytics Node",
                                                                                                                   symbols[2].Item1,
                                                                                                                   symbols[2].Item1,
                                                                                                                   symbols[3].Item1,
                                                                                                                   symbols[2].Item1,
                                                                                                                   symbols[2].Item1,
                                                                                                                   symbols[2].Item1,
                                                                                                                   symbols[5].Item1);
                    wsInstance.Cells[row++, Properties.Settings.Default.RefreshWSAttrColumn].Value = string.Format("Table Example: TableName-1 {0}{1} -- 1) Has solr index, 2) Has a Materalized View",
                                                                                                                   symbols[4].Item1,
                                                                                                                   symbols[9].Item1);
                    wsInstance.Cells[row++, Properties.Settings.Default.RefreshWSAttrColumn].Value = string.Format("Materialized View Example: MVName-1 {0} -- 1) Is a Materalized View",
                                                                                                                   symbols[8].Item1);
                }                
            }
            return new Tuple<IFilePath, string, int>(this.ExcelTargetWorkbook, this.WorkSheetName, 1);
        }
    }
}
