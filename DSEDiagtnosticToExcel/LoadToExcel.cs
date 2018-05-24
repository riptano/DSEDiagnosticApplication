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
                                IFilePath excelTemplateWorkbook,
                                string worksheetName,
                                bool useDataTableDefaultView)
        {
            this.LoadTo = LoadToTypes.WorkSheet;
            this.UseDataTableDefaultView = useDataTableDefaultView;
            this.DataTable = dataTableToLoad;
            this.ExcelTargetWorkbook = excelTargetWorkbook;
            this.ExcelTemplateWorkbook = excelTemplateWorkbook;
            this.ExcelTargetFolder = excelTargetWorkbook.ParentDirectoryPath;
            this.WorkSheetName = string.IsNullOrEmpty(worksheetName) ? dataTableToLoad.TableName : worksheetName;
            this.AppendToWorkSheet = LibrarySettings.AppendToWorkSheet;
        }

        internal LoadToExcel(DataTable dataTableToLoad,
                                IDirectoryPath excelTargeFolder,
                                string workBookName,
                                bool useDataTableDefaultView,
                                string excelFileExtension = null,
                                IFilePath excelTemplateWorkbook = null)
        {
            this.LoadTo = LoadToTypes.WorkSheet;
            this.UseDataTableDefaultView = useDataTableDefaultView;
            this.DataTable = dataTableToLoad;
            this.ExcelTargetFolder = ExcelTargetFolder;
            this.ExcelTemplateWorkbook = excelTemplateWorkbook;
            this.WorkBookName = string.IsNullOrEmpty(workBookName) ? dataTableToLoad.TableName : workBookName;
            this.AppendToWorkSheet = LibrarySettings.AppendToWorkSheet;

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
        public IFilePath ExcelTemplateWorkbook { get; }
        public IDirectoryPath ExcelTargetFolder { get; }
        public string WorkSheetName { get; }
        public string WorkBookName { get; }
        /// <summary>
        /// The default of this property is set by the library setting property. Node that some instances will need to set this property explicitly.
        /// </summary>
        public bool AppendToWorkSheet { get; set; }

        /// <summary>
        ///
        /// </summary>
        /// <returns>
        /// Item1 is the Excel Workbook
        /// Item2 is the Excel worksheet name, only if the data table is loaded into a worksheet. Otherwise it will be null.
        /// </returns>
        public abstract Tuple<IFilePath, string, int> Load();

        public event OnActionEventHandler OnAction;

        internal bool LoadDefaultAttributes(OfficeOpenXml.ExcelWorksheet excelWorkSheet)
        {
            bool bResult = false;

            if(!string.IsNullOrEmpty(this.WorkSheetName))
            {
                DataTableToExcel.WorkSheetColAttrDefaults[] defaultAttrs;

                if(LibrarySettings.WorkSheetDefaultAttrs.TryGetValue(this.WorkSheetName, out defaultAttrs))
                {
                    var startRow = excelWorkSheet.Dimension.End.Row + 1;

                    foreach (var defaultAttr in defaultAttrs)
                    {
                        bResult = DataTableToExcel.Helpers.WorkSheetLoadColumnDefaults(excelWorkSheet, defaultAttr, startRow);
                    }
                }
            }

            return bResult;
        }

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
