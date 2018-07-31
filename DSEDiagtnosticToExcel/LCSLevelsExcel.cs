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
    public class LCSLevelsExcel : LoadToExcel
    {
        public const string DataTableName = DSEDiagnosticToDataTable.TableNames.AggregatedStats;
        public const string ExcelDataTableName = "LCS Levels";

        public LCSLevelsExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook,
                                string worksheetName,
                                bool useDataTableDefaultView)
            : base(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, worksheetName, useDataTableDefaultView)
        { }

        public LCSLevelsExcel(DataTable keyspaceDataTable,
                                IFilePath excelTargetWorkbook,
                                IFilePath excelTemplateWorkbook = null)
            : this(keyspaceDataTable, excelTargetWorkbook, excelTemplateWorkbook, ExcelDataTableName, true)
        { }

        public DataTable CreateInitializationTable()
        {
            var dtStats = new DataTable(ExcelDataTableName, DSEDiagnosticToDataTable.TableNames.Namespace);

            if (this.DataTable.Columns.Contains(DSEDiagnosticToDataTable.ColumnNames.SessionId)) dtStats.Columns.Add(DSEDiagnosticToDataTable.ColumnNames.SessionId, typeof(Guid));

            dtStats.Columns.Add(DSEDiagnosticToDataTable.ColumnNames.KeySpace, typeof(string)); //A
            dtStats.Columns.Add(DSEDiagnosticToDataTable.ColumnNames.Table, typeof(string));
            dtStats.Columns.Add(DSEDiagnosticToDataTable.ColumnNames.DataCenter, typeof(string));
            dtStats.Columns.Add(DSEDiagnosticToDataTable.ColumnNames.NodeIPAddress, typeof(string)); //D

            dtStats.Columns.Add("LCS Levels", typeof(string)); //E
            dtStats.Columns.Add("Split Level", typeof(bool)).AllowDBNull = true; //F
            dtStats.Columns.Add("Max Level", typeof(int)).AllowDBNull = true; //G
            dtStats.Columns.Add("Min Split Level", typeof(int)).AllowDBNull = true; //H
            dtStats.Columns.Add("Nbr Split Level", typeof(int)).AllowDBNull = true; //I
            dtStats.Columns.Add("OutlineLevel", typeof(int)); //J

            /*
            dtStats.DefaultView.ApplyDefaultSort = false;
            dtStats.DefaultView.AllowDelete = false;
            dtStats.DefaultView.AllowEdit = false;
            dtStats.DefaultView.AllowNew = false;
            dtStats.DefaultView.Sort = string.Format("[{0}] ASC, [{1)] ASC, [{2)] ASC, [{3)] ASC",
                                                        DSEDiagnosticToDataTable.ColumnNames.KeySpace,
                                                        DSEDiagnosticToDataTable.ColumnNames.Table,
                                                        DSEDiagnosticToDataTable.ColumnNames.DataCenter,
                                                        DSEDiagnosticToDataTable.ColumnNames.NodeIPAddress);
            */
            return dtStats;
        }

        static int MaxLevel(string level)
        {
            var levels = level.Trim('[',']').Split(',').Select(l => l.Trim());
            var maxLevel = levels.Skip(1).TakeWhile(l => l != "0").Count();

            return maxLevel == 0 ? (levels.First() == "0" ? -1 : 0) : maxLevel;
        }

        static int MinSplitLevel(string level)
        {
            var levels = level.Trim('[', ']').Split(',').Select(l => l.Trim());
            var minLevel = levels.TakeWhile(l => !l.Contains('/')).Count();

            return minLevel == 0 ? (levels.First() == "0" ? -1 : 0) : (minLevel == levels.Count() ? -1 : minLevel);
        }

        public override Tuple<IFilePath, string, int> Load()
        {
            var dtLCSLevels = this.CreateInitializationTable();

            var lcsLevelsRows = from row in this.DataTable.AsEnumerable()
                                where row.Field<string>("Attribute") == Properties.Settings.Default.CFStatsLCSLevels && row.Field<string>("Properties") == Properties.Settings.Default.LCL
                                group row by new
                                {
                                    Keyspace = row.Field<string>(DSEDiagnosticToDataTable.ColumnNames.KeySpace),
                                    Table = row.Field<string>(DSEDiagnosticToDataTable.ColumnNames.Table)
                                } into g
                                orderby g.Key.Keyspace ascending, g.Key.Table ascending
                                select new
                                {
                                    Key = g.Key,
                                    NodeLevels = g.Select(n =>
                                                    {
                                                        var lvlValue = n.Field<string>("Value");
                                                        return new
                                                                {
                                                                    DC = n.Field<string>(DSEDiagnosticToDataTable.ColumnNames.DataCenter),
                                                                    Node = n.Field<string>(DSEDiagnosticToDataTable.ColumnNames.NodeIPAddress),
                                                                    Levels = lvlValue,
                                                                    SplitLevels = lvlValue?.Count(i => i == '/'),
                                                                    MaxLevel = MaxLevel(lvlValue),
                                                                    MinSplitLevel = MinSplitLevel(lvlValue)
                                                                };
                                                        })
                                                    .OrderBy(n => n.DC)
                                                    .ThenBy(n => n.Node)
                                };

            int outLineNbr = 0;

            foreach(var lcsLevel in lcsLevelsRows)
            {
                ++outLineNbr;
                foreach (var nodeLvl in lcsLevel.NodeLevels)
                {
                    dtLCSLevels.Rows.Add(lcsLevel.Key.Keyspace,
                                            lcsLevel.Key.Table,
                                            nodeLvl.DC,
                                            nodeLvl.Node,
                                            nodeLvl.Levels,
                                            nodeLvl.SplitLevels > 0,
                                            nodeLvl.MaxLevel == -1 ? (int?) null : nodeLvl.MaxLevel,
                                            nodeLvl.MinSplitLevel == -1 ? (int?) null : nodeLvl.MinSplitLevel,
                                            nodeLvl.SplitLevels > 0 ? nodeLvl.SplitLevels : (int?) null,
                                            outLineNbr);
                }
            }

            var nbrRows = DataTableToExcel.Helpers.WorkBook(this.ExcelTargetWorkbook.PathResolved, this.WorkSheetName, dtLCSLevels,
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
                                                                        var workSheet = excelPackage.Workbook.Worksheets[WorkSheetName];
                                                                        var rangeAddress = loadRange == null ? null : workSheet?.Cells[loadRange];

                                                                        if (rangeAddress != null)
                                                                        {
                                                                            var startRow = rangeAddress.Start.Row;
                                                                            var endRow = rangeAddress.End.Row;
                                                                            int lastValue = 0;
                                                                            int currentValue;
                                                                            bool formatOn = false;

                                                                            for (int nRow = startRow; nRow <= endRow; ++nRow)
                                                                            {
                                                                                if (nRow == 1) continue;

                                                                                if (workSheet.Cells[nRow, 10] != null)
                                                                                {
                                                                                    currentValue = (int) workSheet.Cells[nRow, 10].Value;

                                                                                    if (lastValue == 0)
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
                                                                                int? splitLevel = workSheet.Cells[nRow, 8] == null
                                                                                                        ? (int?)null : (int?)workSheet.Cells[nRow, 8].Value;
                                                                                if (splitLevel.HasValue)
                                                                                {                                                                                    
                                                                                    if (splitLevel.Value == 0)
                                                                                    {
                                                                                        workSheet.Cells[nRow, 1].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.Solid;
                                                                                        workSheet.Cells[nRow, 1].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightPink);
                                                                                    }
                                                                                    else                                                                                    
                                                                                    {
                                                                                        workSheet.Cells[nRow, 1].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.Solid;
                                                                                        workSheet.Cells[nRow, 1].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.Orange);
                                                                                    }
                                                                                }
                                                                            }
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
                                                             workSheet =>
                                                             {
                                                                 workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                                 workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                                 //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                                 
                                                                 workSheet.View.FreezePanes(2, 1);
                                                                 workSheet.Cells["A1:I1"].AutoFilter = true;
                                                                 workSheet.Column(10).Hidden = true;
                                                                 workSheet.AutoFitColumn(workSheet.Cells["A:I"]);                                                           
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
