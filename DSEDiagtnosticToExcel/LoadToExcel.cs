using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;
using Common;

namespace DSEDiagtnosticToExcel
{
    public abstract class LoadToExcel : IExcel
    {


        internal LoadToExcel(DataTable dataTableToLoad,
                                IFilePath excelTargetWorkbook,
                                string worksheetName,
                                bool useDataTableDefaultView)
        {
            this.LoadTo = LoadToTypes.WorkSheet;
            this.UseDataTableDefaultView = useDataTableDefaultView;
            this.DataTable = dataTableToLoad;
            this.ExcelTargetWorkbook = excelTargetWorkbook;
            this.ExcelTargetFolder = excelTargetWorkbook.ParentDirectoryPath;
            this.WorkSheetName = string.IsNullOrEmpty(worksheetName) ? dataTableToLoad.TableName : worksheetName;
        }

        internal LoadToExcel(DataTable dataTableToLoad,
                                IDirectoryPath excelTargeFolder,
                                string workBookName,
                                bool useDataTableDefaultView,
                                string excelFileExtension = null)
        {
            this.LoadTo = LoadToTypes.WorkSheet;
            this.UseDataTableDefaultView = useDataTableDefaultView;
            this.DataTable = dataTableToLoad;
            this.ExcelTargetFolder = ExcelTargetFolder;
            this.WorkBookName = string.IsNullOrEmpty(workBookName) ? dataTableToLoad.TableName : workBookName;

            IFilePath excelWorkbook;
            if (this.ExcelTargetFolder.MakeFile(this.WorkBookName,
                                                    string.IsNullOrEmpty(excelFileExtension) ? Properties.Settings.Default.ExcelFileExtension : excelFileExtension,
                                                    out excelWorkbook))
            {
                this.ExcelTargetWorkbook = excelWorkbook;
            }
        }

        public LoadToTypes LoadTo { get; }
        public bool UseDataTableDefaultView { get; }
        public DataTable DataTable { get; }
        public IFilePath ExcelTargetWorkbook { get; }
        public IDirectoryPath ExcelTargetFolder { get; }
        public string WorkSheetName { get; }
        public string WorkBookName { get; }

        /// <summary>
        ///
        /// </summary>
        /// <returns>
        /// Item1 is the Excel Workbook
        /// Item2 is the Excel worksheet name, only if the data table is loaded into a worksheet. Otherwise it will be null.
        /// </returns>
        public abstract Tuple<IFilePath, string, int> Load();

        public event OnActionEventHandler OnAction;

        internal void CallActionEvent(string actionItem)
        {
            var action = this.OnAction;

            if(action != null)
            {
                action(this, actionItem);
            }
        }
    }
}
