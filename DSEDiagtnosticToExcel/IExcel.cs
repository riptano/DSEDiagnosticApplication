using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagtnosticToExcel
{

    public enum LoadToTypes
    {
        WorkSheet,
        WorkBook
    }

    public delegate void OnActionEventHandler(IExcel sender, string action);

    public interface IExcel
    {
        LoadToTypes LoadTo { get; }
        bool UseDataTableDefaultView { get; }
        System.Data.DataTable DataTable { get; }
        Common.IFilePath ExcelTargetWorkbook { get; }
        Common.IDirectoryPath ExcelTargetFolder { get; }
        string WorkSheetName { get; }
        string WorkBookName { get; }

        /// <summary>
        ///
        /// </summary>
        /// <returns>
        /// Item1 is the Excel Workbook
        /// Item2 is the Excel worksheet name, only if the data table is loaded into a worksheet. Otherwise it will be null.
        /// </returns>
        Tuple<Common.IFilePath,string,int> Load();

        event OnActionEventHandler OnAction;
    }
}
