using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Data;
using System.Threading.Tasks;
using OfficeOpenXml;
using Common;

namespace DataTableToExcel
{
    public enum WorkBookProcessingStage
    {
        /// <summary>
        /// Action called before any data table or excel processing. Called only once.
        /// </summary>
        PreProcess,
        /// <summary>
        /// Action called before workbook is open so that the file name can be modified. (one time per workbook)
        /// </summary>
        PrepareFileName,
        /// <summary>
        /// Only called if the data table needs to be split because it exceeds maximum number rows in a workbook. Called before actual table split. Called only once.
        /// </summary>
        PreProcessDataTable,
        /// <summary>
        /// Called after a workbook is open but before the worksheet is processed. Can be called multiple times (one time per workbook)
        /// </summary>
        PreLoad,
        /// <summary>
        /// Called after a worksheet is loaded but before the workbook is closed and saved. Can be called multiple times (one time per workbook)
        /// </summary>
        PreSave,
        /// <summary>
        /// Called after the workbook is saved but still open. Can be called multiple times (one time per workbook)
        /// </summary>
        Saved,
        /// <summary>
        /// Called after workbook processing (workbook is closed). Called only once.
        /// </summary>
        PostProcess
    }

    public sealed class ExcelPkgCache : IDisposable
    {
        public ExcelPkgCache(IFilePath excelFilePath, bool isCached, IFilePath excelTemplateFilePath = null)
        {
            this.ExcelFilePath = excelFilePath;
            this.ExcelPackage = excelTemplateFilePath == null ? new ExcelPackage(excelFilePath.FileInfo()) : new ExcelPackage(excelFilePath.FileInfo(), excelTemplateFilePath.FileInfo());
            this._refCnt = isCached ? 2 : 1;
            this.CachedValue = isCached;
        }

        #region Public Members

        public ExcelPackage ExcelPackage { get; }
        public IFilePath ExcelFilePath { get; }
        public bool CachedValue { get; }

        private int _refCnt;
        public int RefCnt { get { return _refCnt; } }

        public void Close()
        {
            if (this._refCnt > 0 && !this.Disposed) this.ExcelPackage.Dispose();
            this._refCnt = 0;
        }

        public void Save()
        {
            if (this._refCnt > 0 && !this.Disposed)
            {                
                this.ExcelPackage.Save();
            }
        }

        public void Save(IFilePath newFilePath)
        {
            if (this._refCnt > 0 && !this.Disposed) this.ExcelPackage.SaveAs(newFilePath.FileInfo());
        }

        public void SaveAndClose(IFilePath newFilePath = null)
        {
            if (newFilePath == null)
                this.Save();
            else
                this.Save(newFilePath);

            this.Close();
        }

        public int IncrementRefCnt()
        {
            if (this.Disposed) return 0;
            return System.Threading.Interlocked.Increment(ref this._refCnt);
        }

        #endregion

        #region Static Members

        static private System.Collections.Concurrent.ConcurrentDictionary<IFilePath, ExcelPkgCache> _Caches = new System.Collections.Concurrent.ConcurrentDictionary<IFilePath, ExcelPkgCache>();

        static public IReadOnlyList<KeyValuePair<IFilePath, ExcelPkgCache>> Caches { get { return _Caches.ToList(); } }

        static public ExcelPackage GetExcelPackage(IFilePath excelPath)
        {
            ExcelPkgCache cache;

            if(_Caches.TryGetValue(excelPath, out cache))
            {
                return cache.ExcelPackage;
            }

            return null;
        }

        static public ExcelPkgCache GetExcelPackageCache(IFilePath excelPath)
        {
            ExcelPkgCache cache;

            if (_Caches.TryGetValue(excelPath, out cache))
            {
                return cache;
            }

            return null;
        }

        static public ExcelPkgCache GetAddExcelPackageCache(IFilePath excelPath, bool cacheValue = true, IFilePath excelTemplateFilePath = null)
        {
            ExcelPkgCache newInstance = null;
            var cache = _Caches.GetOrAdd(excelPath, excelFile => newInstance = new ExcelPkgCache(excelPath, cacheValue, excelTemplateFilePath));

            if (newInstance == null) cache.IncrementRefCnt();

            return cache;
        }

        static public ExcelPkgCache RemoveExcelPackageCache(IFilePath excelPath, bool saveFile = false, bool disposePackage = true)
        {
            ExcelPkgCache cache = null;

            if(_Caches.TryRemove(excelPath, out cache))
            {
                if (saveFile) cache.Save();
                if (disposePackage) cache.DisposeForce();
            }

            return cache;
        }

        static public int SaveAllExcelFiles(bool closePackage = false)
        {
            int nSaved = 0;

            foreach(var cache in Caches)
            {                
                if (closePackage)
                {
                    RemoveExcelPackageCache(cache.Key, true, true);
                    Logger.Instance.InfoFormat("Excel WorkBooks saved and closed to \"{0}\"", cache.Key.PathResolved);
                }
                else
                {
                    cache.Value.Save();
                    Logger.Instance.InfoFormat("Excel WorkBooks saved to \"{0}\"", cache.Key.PathResolved);
                }
                nSaved++;
            }

            return nSaved;
        }
        #endregion

        #region Dispose Methods

        public bool Disposed { get; private set; }

        public void DisposeForce()
        {
            Dispose(false);
            GC.SuppressFinalize(this);            
        }

        // Implement IDisposable.
        // Do not make this method virtual.
        // A derived class should not be able to override this method.
        public void Dispose()
        {
            Dispose(true);

            if (this.Disposed)
            {
                // This object will be cleaned up by the Dispose method.
                // Therefore, you should call GC.SupressFinalize to
                // take this object off the finalization queue
                // and prevent finalization code for this object
                // from executing a second time.
                GC.SuppressFinalize(this);
            }
        }

        // Dispose(bool disposing) executes in two distinct scenarios.
        // If disposing equals true, the method has been called directly
        // or indirectly by a user's code. Managed and unmanaged resources
        // can be disposed.
        // If disposing equals false, the method has been called by the
        // runtime from inside the finalizer and you should not reference
        // other objects. Only unmanaged resources can be disposed.
        private void Dispose(bool disposing)
        {
            // Check to see if Dispose has already been called.
            if (!this.Disposed)
            {
                if (disposing)
                {                    
                    if (this._refCnt > 0)
                    {
                        if (System.Threading.Interlocked.Decrement(ref this._refCnt) <= 0 || !this.CachedValue)
                        {
                            this._refCnt = 0;
                            this.ExcelPackage.Dispose();
                            this.Disposed = true;
                        }                                               
                    }
                }
                else
                {
                    this._refCnt = 0;
                    this.ExcelPackage.Dispose();
                    this.Disposed = true;
                }
            }
        }

        #endregion //end of Dispose Methods

    }

    public static class Helpers
    {
        static public void AutoFitColumn(this ExcelWorksheet workSheet, params ExcelRange[] autoFitRanges)
        {
            try
            {
                if (autoFitRanges == null || autoFitRanges.Length == 0)
                {
                    workSheet.Cells.AutoFitColumns();
                }
                else
                {
                    foreach (var range in autoFitRanges)
                    {
                        try
                        {
                            range.AutoFitColumns();
                        }
                        catch (OverflowException)
                        {}                        
                    }
                }
            }
            catch (System.Exception ex)
            {
                Logger.Instance.Error(string.Format("Excel Cell Range AutoFitColumns Exception Occurred in Worksheet \"{0}\". Worksheet was loaded/updated but NOT auto-formatted...", workSheet.Name), ex);
            }
        }

        static public void AutoFitColumn(this ExcelWorksheet workSheet)
        {
            try
            {
                workSheet.Cells.AutoFitColumns();
            }
            catch (System.Exception ex)
            {
                Logger.Instance.Error(string.Format("Excel AutoFitColumns Exception Occurred in Worksheet \"{0}\". Worksheet was loaded/updated but NOT auto-formatted...", workSheet.Name), ex);
            }
        }

        /// <summary>
        /// Loads data table into a workbook&apos;s worksheet. If the worksheet exists it is cleared otherwise it is created.
        /// </summary>
        /// <param name="excelPkg">
        /// Open workbook instance.
        /// </param>
        /// <param name="workSheetName">
        /// Worksheet name. If null the data table name is used.
        /// </param>
        /// <param name="dtExcel">
        /// data table
        /// </param>
        /// <param name="worksheetAction">
        /// Called after the worksheet is loaded.
        /// </param>
        /// <param name="enableMaxRowLimitPerWorkSheet">
        /// if true (default) and the row count exceeds the maximum EPPlus row limit, an additional worksheets are created to accommodate all the rows. worksheetAction is called for all additional worksheets.
        /// </param>
        /// <param name="maxRowInExcelWorkSheet"></param>
        /// The maximum number of rows within a worksheet before another worksheet is created.
        /// <param name="startingWSCell"></param>
        /// <param name="useDefaultView">
        /// if useDefaultView is true the datatable&apos;s default view is used to filter and/or sort the table.
        /// Default is false.
        /// </param>
        /// <param name="renameFirstWorksheetIfDivided">
        /// renames the first worksheet (default) in cases where the maxRowInExcelWorkSheet is exceed. Otherwise the original workSheetName is used.
        /// </param>
        /// <param name="appendToWorkSheet">
        /// if true, worksheet is appended (not overwritten). The default is false.
        /// </param>
        /// <param name="clearWorkSheet">
        /// If true (default), the worksheet is first cleared (ignored if appendToWorksheet is true)
        /// </param>
        /// <param name="printHeaders">
        /// </param>
        /// <returns></returns>
        static public ExcelRangeBase WorkSheet(this ExcelPackage excelPkg,
                                                string workSheetName,
                                                System.Data.DataTable dtExcel,
                                                Action<ExcelWorksheet> worksheetAction = null,
                                                bool enableMaxRowLimitPerWorkSheet = true,
                                                int maxRowInExcelWorkSheet = -1,
                                                string startingWSCell = "A1",
                                                bool useDefaultView = false,
                                                bool renameFirstWorksheetIfDivided = true,
                                                bool appendToWorkSheet = false,
                                                bool printHeaders = true,
                                                bool clearWorkSheet = true)
        {
            if (string.IsNullOrEmpty(workSheetName)) workSheetName = dtExcel.TableName;

            dtExcel.AcceptChanges();

            var dtErrors = dtExcel.GetErrors();
            if (dtErrors.Length > 0)
            {
                Logger.Instance.Dump(dtErrors, Logger.DumpType.Error, "Table \"{0}\" Has Error", dtExcel.TableName);
            }

            if (dtExcel.Rows.Count == 0)
            {
                Logger.Instance.InfoFormat("No row in DataTable \"{0}\" for Excel WorkSheet \"{1}\" in Workbook \"{2}\".{3}",
                                            dtExcel.TableName,
                                            workSheetName,
                                            excelPkg.File?.Name,
                                            clearWorkSheet ? " Worksheet Cleared" : string.Empty);
                if (!appendToWorkSheet && clearWorkSheet)
                {
                    var clrWorkSheet = excelPkg.Workbook.Worksheets[workSheetName];

                    if (clrWorkSheet != null)
                    {
                        clrWorkSheet.Cells.Clear();
                        foreach (ExcelComment comment in clrWorkSheet.Comments.Cast<ExcelComment>().ToArray())
                        {
                            try
                            {
                                clrWorkSheet.Comments.Remove(comment);
                            }
                            catch { }
                        }
                    }
                }
                return null;
            }

            if(useDefaultView)
            {
                if (string.IsNullOrEmpty(dtExcel.DefaultView.Sort)
                        && string.IsNullOrEmpty(dtExcel.DefaultView.RowFilter)
                        && dtExcel.DefaultView.RowStateFilter == DataViewRowState.CurrentRows)
                {
                    useDefaultView = false;
                }
                else
                {
                    dtExcel = dtExcel.DefaultView.ToTable();
                }
            }

            if (dtExcel.Rows.Count == 0)
            {
                if (useDefaultView)
                {
                    Logger.Instance.InfoFormat("Using Default DataView (\"{2}\", \"{3}\", \"{4}\") within Excel Worksheet Loading for DataTable \"{0}\" into WorkSheet \"{1}\" for Workbook \"{5}\"",
                                                dtExcel.TableName,
                                                workSheetName,
                                                dtExcel.DefaultView.Sort,
                                                dtExcel.DefaultView.RowFilter,
                                                dtExcel.DefaultView.RowStateFilter,
                                                excelPkg.File?.Name);
                }
                Logger.Instance.InfoFormat("No row in DataTable \"{0}\" for Excel WorkSheet \"{1}\" in Workbook \"{2}\".{3}",
                                            dtExcel.TableName,
                                            workSheetName,
                                            excelPkg.File?.Name,
                                            clearWorkSheet ? " Worksheet Cleared" : string.Empty);
                if (!appendToWorkSheet && clearWorkSheet)
                {
                    var clrWorkSheet = excelPkg.Workbook.Worksheets[workSheetName];

                    if (clrWorkSheet != null)
                    {
                        clrWorkSheet.Cells.Clear();
                        foreach (ExcelComment comment in clrWorkSheet.Comments.Cast<ExcelComment>().ToArray())
                        {
                            try
                            {
                                clrWorkSheet.Comments.Remove(comment);
                            }
                            catch { }
                        }
                    }
                }
                return null;
            }

            if (maxRowInExcelWorkSheet > 0
                   && dtExcel.Rows.Count > maxRowInExcelWorkSheet)
            {
                var dtSplits = dtExcel.SplitTable(maxRowInExcelWorkSheet, useDefaultView);
                ExcelRangeBase excelRange = null;
                int splitCnt = 1;

                foreach (var dtSplit in dtSplits)
                {
                    excelRange = WorkSheet(excelPkg,
                                            renameFirstWorksheetIfDivided || splitCnt > 1
                                                ? string.Format("{0}-{1:000}", workSheetName, splitCnt)
                                                : workSheetName,
                                            dtSplit,
                                            worksheetAction,
                                            true,
                                            -1,
                                            startingWSCell,
                                            false,
                                            false,
                                            appendToWorkSheet,
                                            printHeaders,
                                            clearWorkSheet);
                    ++splitCnt;
                }

                return excelRange;
            }

            var workSheet = excelPkg.Workbook.Worksheets[workSheetName];
            if (workSheet == null)
            {
                try
                {
                    workSheet = excelPkg.Workbook.Worksheets.Add(workSheetName);
                }
                catch (InvalidOperationException)
                {
                    workSheet = excelPkg.Workbook.Worksheets[workSheetName];

                    if (workSheet == null)
                    {
                        throw;
                    }
                }
                catch
                {
                    throw;
                }
            }
            else
            {
                if (appendToWorkSheet)
                {
                    var row = workSheet.Dimension.End.Row;
                    var startingRange = workSheet.Cells[startingWSCell];
                    var endingColumn = workSheet.Dimension.End.Column;

                    if (row > startingRange.Start.Row)
                    {
                        while (row >= 1)
                        {
                            var range = workSheet.Cells[row, 1, row, endingColumn];

                            if (range.Any(c => !string.IsNullOrEmpty(c.Text)))
                            {
                                break;
                            }
                            row--;
                        }

                        startingWSCell = workSheet.Cells[row + 1, startingRange.End.Column].Address;
                        printHeaders = false;
                        clearWorkSheet = false;
                    }
                }

                if(clearWorkSheet)
                {
                    workSheet.Cells.Clear();
                    foreach (ExcelComment comment in workSheet.Comments.Cast<ExcelComment>().ToArray())
                    {
                        try
                        {
                            workSheet.Comments.Remove(comment);
                        }
                        catch { }
                    }
                }
            }

            Logger.Instance.InfoFormat("Loading DataTable \"{0}\" into Excel WorkSheet \"{1}\" at Cell \"{4}\" for Workbook \"{3}\". Rows: {2:###,###,##0}",
                                        dtExcel.TableName,
                                        workSheet.Name,
                                        dtExcel.Rows.Count,
                                        excelPkg.File?.Name,
                                        startingWSCell);

            if (useDefaultView)
            {
                Logger.Instance.InfoFormat("Using Default DataView (\"{2}\", \"{3}\", \"{4}\") within Excel Worksheet Loading for DataTable \"{0}\" into WorkSheet \"{1}\" for Workbook \"{5}\"",
                                            dtExcel.TableName,
                                            workSheetName,
                                            dtExcel.DefaultView.Sort,
                                            dtExcel.DefaultView.RowFilter,
                                            dtExcel.DefaultView.RowStateFilter,
                                            excelPkg.File?.Name);
            }

            if (dtExcel.Rows.Count > ExcelPackage.MaxRows)
            {
                if (enableMaxRowLimitPerWorkSheet
                        && Properties.Settings.Default.ExcelPackageMaxRowsLimit > 0)
                {
                    Logger.Instance.ErrorFormat("Table \"{0}\" with {1:###,###,##0} rows exceed maximum number of rows allowed by EPPlus (Max. Limit of {2:###,###,##0}) for worksheet \"{3}\" in workbook \"{4}\". The worksheet will be divided, creating multiple worksheets with a {5:###,###,##0} rows each. This may cause some workbook features to not work correctly!",
                                                dtExcel.TableName,
                                                dtExcel.Rows.Count,
                                                ExcelPackage.MaxRows,
                                                workSheetName,
                                                excelPkg.File?.Name,
                                                Properties.Settings.Default.ExcelPackageMaxRowsLimit);

                    return WorkSheet(excelPkg,
                                        workSheetName,
                                        dtExcel,
                                        worksheetAction,
                                        false,
                                        Properties.Settings.Default.ExcelPackageMaxRowsLimit,
                                        startingWSCell,
                                        false,
                                        false,
                                        appendToWorkSheet,
                                        printHeaders,
                                        clearWorkSheet);
                }
                else
                {
                    throw new ArgumentOutOfRangeException("dtExcel.Rows.Count",
                                                            string.Format("Table \"{0}\" with {1:###,###,##0} rows exceed maximum number of rows allowed by EPPlus (Max. Limit of {2:###,###,##0}) for worksheet \"{3}\" in workbook \"{4}\". Maybe enable DivideWorksheetIfExceedMaxRows parameter, or divide worksheet creation into smaller chucks.",
                                                                            dtExcel.TableName,
                                                                            dtExcel.Rows.Count,
                                                                            ExcelPackage.MaxRows,
                                                                            workSheetName,
                                                                            excelPkg.File?.Name));
                }
            }

            var loadRange = workSheet.Cells[startingWSCell].LoadFromDataTable(dtExcel, printHeaders);

            if (loadRange != null && worksheetAction != null)
            {
                try
                {
                    worksheetAction(workSheet);
                }
                catch (System.Exception ex)
                {
                    Logger.Instance.Error(string.Format("Exception Occurred in Worksheet \"{0}\" for Workbook \"{1}\"", workSheetName, excelPkg.File?.Name), ex);
                }
            }

            Logger.Instance.InfoFormat("Loaded DataTable \"{0}\" into Excel WorkSheet \"{1}\" at \"{4}\" for Workbook \"{3}\". Range: {2}",
                                        dtExcel.TableName,
                                        workSheet.Name,
                                        loadRange,
                                        excelPkg.File?.Name,
                                        startingWSCell);

            return loadRange;
        }

        static public void WorkBookWorkSheet(IFilePath orgTargetFile,
                                                IFilePath excelFile,
                                                string workSheetName,
                                                System.Data.DataTable dtExcel,
                                                ref int nResult,
                                                Action<WorkBookProcessingStage, IFilePath, IFilePath, string, ExcelPackage, DataTable, int, string> workBookActions = null,
                                                Action<ExcelWorksheet> worksheetAction = null,
                                                bool enableMaxRowLimitPerWorkSheet = true,
                                                int maxRowInExcelWorkSheet = -1,
                                                string startingWSCell = "A1",
                                                bool useDefaultView = false,
                                                bool renameFirstWorksheetIfDivided = true,
                                                bool appendToWorkSheet = false,
                                                bool printHeaders = true,
                                                bool clearWorkSheet = true,
                                                bool cachePackage = false,
                                                bool saveWorkSheet = true,
                                                IFilePath excelTemplateFile = null)
        {
            using (var excelPkgCache = ExcelPkgCache.GetAddExcelPackageCache(excelFile, cachePackage, excelTemplateFile))
            {
                var excelPkg = excelPkgCache.ExcelPackage;

                workBookActions?.Invoke(WorkBookProcessingStage.PreLoad, orgTargetFile, excelFile, workSheetName, excelPkg, dtExcel, -1, null);

                var loadRange = WorkSheet(excelPkg,
                                            workSheetName,
                                            dtExcel,
                                            worksheetAction,
                                            enableMaxRowLimitPerWorkSheet,
                                            maxRowInExcelWorkSheet,
                                            startingWSCell,
                                            useDefaultView,
                                            renameFirstWorksheetIfDivided,
                                            appendToWorkSheet,
                                            printHeaders,
                                            clearWorkSheet);

                System.Threading.Interlocked.Add(ref nResult, dtExcel.Rows.Count);

                workBookActions?.Invoke(WorkBookProcessingStage.PreSave, orgTargetFile, excelFile, workSheetName, excelPkg, dtExcel, dtExcel.Rows.Count, loadRange?.Address);

                if (saveWorkSheet)
                {
                    excelPkg.Save();
                    workBookActions?.Invoke(WorkBookProcessingStage.Saved, orgTargetFile, excelFile, workSheetName, excelPkg, dtExcel, dtExcel.Rows.Count, loadRange?.Address);
                    Logger.Instance.InfoFormat("Excel WorkBooks saved to \"{0}\"", excelFile.PathResolved);
                }
            }
        }



        /// <summary>
        /// Loads the data table into a new or existing Excel workbook based on the associated worksheet name (if worksheet exists it is cleared).
        /// </summary>
        /// <param name="excelFilePath"></param>
        /// <param name="workSheetName">
        /// if null the data table name is used.
        /// </param>
        /// <param name="dtExcel"></param>
        /// <param name="workBookActions">
        /// Called based on the workbook action
        /// The action has the following parameters:
        ///     Processing Stage <seealso cref="WorkBookProcessingStage"/>
        ///     Original workbook file
        ///     Actual workbook file that will be or is opened (null on PreProcess, PreProcessDataTable, and PostProcess)
        ///         Note on PrepareFileName action, the file path instance can be changed to reflect another actual file.
        ///     Worksheet Name
        ///     Excel Package Instance (workbook instance), null if workbook is not open
        ///         Additional EPPlus operations can be conducted against this instance.
        ///     Data Table
        ///     Total Number of rows loaded (-1 on PreProcess, PrepareFileName, PreLoad, PreProcessDataTable, and PostProcess).
        /// </param>
        /// <param name="worksheetAction">
        /// Called on each worksheet action
        /// </param>
        /// <param name="maxRowInExcelWorkBook">
        /// maximum number of rows that a workbook can contain. If rows exceed this number, multiple workbooks are created (if already exists that workbook is used).
        /// </param>
        /// <param name="maxRowInExcelWorkSheet">
        /// maximum number of rows in an individual worksheet. If rows exceed this number, multiple worksheets are created in this workbook. if maxRowInExcelWorkBook is exceeded a new workbook is created.
        /// </param>
        /// <param name="startingWSCell"></param>
        /// <param name="useDefaultView">
        /// if useDefaultView is true the datatable&apos;s default view is used to filter and/or sort the table.
        /// Default is false.
        /// </param>
        /// <param name="appendToWorkSheet">
        /// if true, worksheet is appended (not overwritten). The default is false.
        /// </param>
        /// <param name="clearWorkSheet">
        /// If true (default), the worksheet is first cleared (ignored if appendToWorksheetis true)
        /// </param>
        /// <param name="generateNewFileName">
        /// If true, the worksheet name is appended to the file name. Default is false.
        /// </param>
        /// <param name="processWorkSheetOnEmptyDataTable">
        /// If true (default), empty data tables are still processed. If false this method just returns (calls PostProcess event).
        /// </param>
        /// <returns></returns>
        static public int WorkBook(string excelFilePath,
                                                    string workSheetName,
                                                    DataTable dtExcel,
                                                    Action<WorkBookProcessingStage, IFilePath, IFilePath, string, ExcelPackage, DataTable, int, string> workBookActions = null,
                                                    Action<ExcelWorksheet> worksheetAction = null,
                                                    int maxRowInExcelWorkBook = -1,
                                                    int maxRowInExcelWorkSheet = -1,
                                                    string startingWSCell = "A1",
                                                    bool useDefaultView = false,
                                                    bool generateNewFileName = false,
                                                    bool appendToWorkSheet = false,
                                                    bool clearWorkSheet = true,
                                                    bool processWorkSheetOnEmptyDataTable = true,
                                                    bool cachePackage = false,
                                                    bool saveWorkSheet = true,
                                                    IFilePath excelTemplateFile = null)
        {
            var excelTargetFile = Common.Path.PathUtils.BuildFilePath(excelFilePath);
            var orgTargetFile = (IFilePath)excelTargetFile.Clone();
            int nResult = 0;

            if (string.IsNullOrEmpty(workSheetName)) workSheetName = dtExcel.TableName;

            workBookActions?.Invoke(WorkBookProcessingStage.PreProcess,
                                        orgTargetFile,
                                        null,
                                        workSheetName,
                                        null,
                                        dtExcel,
                                        -1,
                                        null);

            if (dtExcel.Rows.Count == 0 && !processWorkSheetOnEmptyDataTable)
            {
                workBookActions?.Invoke(WorkBookProcessingStage.PostProcess, orgTargetFile, null, null, null, dtExcel, 0, null);
                return 0;
            }

            if (maxRowInExcelWorkBook <= 0)
            {
                excelTargetFile.FileNameFormat = string.Format("{0}-{{0}}{1}",
                                                                        excelTargetFile.Name,
                                                                        string.IsNullOrEmpty(Properties.Settings.Default.ExcelWorkBookFileExtension)
                                                                            ? excelTargetFile.FileExtension
                                                                            : Properties.Settings.Default.ExcelWorkBookFileExtension);

                var excelFile = generateNewFileName
                                    ? ((IFilePath)excelTargetFile.Clone()).ApplyFileNameFormat(new object[] { workSheetName })
                                    : excelTargetFile;

                workBookActions?.Invoke(WorkBookProcessingStage.PrepareFileName, orgTargetFile, excelFile, workSheetName, null, dtExcel, -1, null);

                WorkBookWorkSheet(orgTargetFile,
                                    excelFile,
                                    workSheetName,                                                
                                    dtExcel,
                                    ref nResult,
                                    workBookActions: workBookActions,
                                    worksheetAction: worksheetAction,
                                    maxRowInExcelWorkSheet: maxRowInExcelWorkSheet,
                                    startingWSCell: startingWSCell,
                                    useDefaultView: useDefaultView,
                                    appendToWorkSheet: appendToWorkSheet,
                                    clearWorkSheet: clearWorkSheet,
                                    cachePackage: cachePackage,
                                    saveWorkSheet: saveWorkSheet,
                                    excelTemplateFile: excelTemplateFile);                

                workBookActions?.Invoke(WorkBookProcessingStage.PostProcess, orgTargetFile, null, workSheetName, null, dtExcel, dtExcel.Rows.Count, null);
                return dtExcel.Rows.Count;
            }

            workBookActions?.Invoke(WorkBookProcessingStage.PreProcessDataTable, orgTargetFile, null, workSheetName, null, dtExcel, -1, null);

            var dtSplits = dtExcel.SplitTable(maxRowInExcelWorkBook, useDefaultView);            
            long totalRows = 0;

            if (dtSplits.Count() == 1)
            {
                excelTargetFile.FileNameFormat = string.Format("{0}-{{0}}{1}",
                                                                        excelTargetFile.Name,
                                                                        string.IsNullOrEmpty(Properties.Settings.Default.ExcelWorkBookFileExtension)
                                                                            ? excelTargetFile.FileExtension
                                                                            : Properties.Settings.Default.ExcelWorkBookFileExtension);
            }
            else
            {
                excelTargetFile.FileNameFormat = string.Format("{0}-{{0}}-{{1:000}}{1}",
                                                                excelTargetFile.Name,
                                                                string.IsNullOrEmpty(Properties.Settings.Default.ExcelWorkBookFileExtension)
                                                                            ? excelTargetFile.FileExtension
                                                                            : Properties.Settings.Default.ExcelWorkBookFileExtension);
                cachePackage = false;
                saveWorkSheet = true;
            }

            Parallel.ForEach(dtSplits, dtSplit =>
            //foreach (var dtSplit in dtSplits)
            {
                var excelFile = generateNewFileName
                                        ? ((IFilePath)excelTargetFile.Clone())
                                                .ApplyFileNameFormat(new object[] { workSheetName, System.Threading.Interlocked.Increment(ref totalRows) })
                                         : excelTargetFile;

                workBookActions?.Invoke(WorkBookProcessingStage.PrepareFileName, orgTargetFile, excelFile, workSheetName, null, dtSplit, -1, null);

                WorkBookWorkSheet(orgTargetFile,
                                    excelFile,
                                    workSheetName,
                                    dtSplit,
                                    ref nResult,
                                    workBookActions: workBookActions,
                                    worksheetAction: worksheetAction,
                                    maxRowInExcelWorkSheet: maxRowInExcelWorkSheet,
                                    startingWSCell: startingWSCell,
                                    useDefaultView: false,
                                    appendToWorkSheet: appendToWorkSheet,
                                    clearWorkSheet: clearWorkSheet,
                                    cachePackage: cachePackage,
                                    saveWorkSheet: saveWorkSheet,
                                    excelTemplateFile: excelTemplateFile);        
            });

            workBookActions?.Invoke(WorkBookProcessingStage.PostProcess, orgTargetFile, null, workSheetName, null, null, nResult, null);

            return nResult;
        }

        static public int SaveCloseAllWorkBooks()
        {
            return ExcelPkgCache.SaveAllExcelFiles(true);
        }

        static public void WorkSheetLoadColumnDefaults(this ExcelWorksheet workSheet,
                                                        string column,
                                                        string[] defaultValues)
        {
            for (int emptyRow = workSheet.Dimension.End.Row + 1, posValue = 0; posValue < defaultValues.Length; ++emptyRow, ++posValue)
            {
                workSheet.Cells[string.Format("{0}{1}", column, emptyRow)].Value = defaultValues[posValue];
            }

        }

        static public void WorkSheetLoadColumnDefaults(this ExcelWorksheet workSheet,
                                                        string column,
                                                        int startRow,
                                                        string[] defaultValues)
        {
            for (int emptyRow = startRow, posValue = 0; posValue < defaultValues.Length; ++emptyRow, ++posValue)
            {
                workSheet.Cells[string.Format("{0}{1}", column, emptyRow)].Value = defaultValues[posValue];
            }

        }

        static public void WorkSheetLoadColumnDefaults(this ExcelPackage excelPkg,
                                                        string workSheetName,
                                                        string column,
                                                        int startRow,
                                                        string[] defaultValues)
        {
            var workSheet = excelPkg.Workbook.Worksheets[workSheetName];
            if (workSheet == null)
            {
                workSheet = excelPkg.Workbook.Worksheets.Add(workSheetName);
            }
            else
            {
                workSheet.Cells[string.Format("{0}:{1}", startRow, workSheet.Dimension.End.Row)].Clear();
            }

            for (int emptyRow = startRow, posValue = 0; posValue < defaultValues.Length; ++emptyRow, ++posValue)
            {
                workSheet.Cells[string.Format("{0}{1}", column, emptyRow)].Value = defaultValues[posValue];
            }

        }

        static public bool WorkSheetLoadColumnDefaults(this ExcelWorksheet workSheet, WorkSheetColAttrDefaults defaultAttrs)
        {
            if (string.IsNullOrEmpty(defaultAttrs.Column) || defaultAttrs.Attrs == null || defaultAttrs.Attrs.Length == 0)
            {
                return false;
            }

            WorkSheetLoadColumnDefaults(workSheet, defaultAttrs.Column, defaultAttrs.Attrs);
            return true;
        }

        static public bool WorkSheetLoadColumnDefaults(this ExcelWorksheet workSheet, WorkSheetColAttrDefaults defaultAttrs, int startRow)
        {
            if (string.IsNullOrEmpty(defaultAttrs.Column) || defaultAttrs.Attrs == null || defaultAttrs.Attrs.Length == 0)
            {
                return false;
            }

            WorkSheetLoadColumnDefaults(workSheet, defaultAttrs.Column, startRow, defaultAttrs.Attrs);
            return true;
        }

        static public ExcelRange TranslaateToExcelRange(this ExcelWorksheet excelWorksheet, DataTable dataTable, string dtColumnName, int excelEndRow = -1, int excelStartRow = 1)
        {
            var dtCol = dataTable.Columns[dtColumnName];

            if(excelEndRow <= 0)
            {
                excelEndRow = dataTable.Rows.Count;
            }

            return excelWorksheet.Cells[excelStartRow, dtCol.Ordinal, excelEndRow, dtCol.Ordinal];
        }

    }
}
