using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Common;

namespace DSEDiagtnosticToExcel
{
    public sealed class LoadDataSet
    {
        public LoadDataSet(DataSet loadExcelFromDataSet,
                            IFilePath excelTargetWorkbook,
                            IFilePath excelTemplateWorkbook = null,
                            bool? explicitlyAppendToTargetExcelFile = null)
        {
            this.ExcelTargetWorkBook = excelTargetWorkbook;
            this.DataSet = loadExcelFromDataSet;
            this.DataSetTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask(loadExcelFromDataSet);
            this.ExplicitlyAppendToTargetExcelFile = explicitlyAppendToTargetExcelFile;
        }

        public LoadDataSet(Task<DataSet> loadExcelFromDataSetTask,
                            IFilePath excelTargetWorkbook,
                            IFilePath excelTemplateWorkbook = null,
                            CancellationTokenSource cancellationSource = null,
                            bool? explictlyAppendToTargetExcelFile = null)
        {
            this.DataSetTask = loadExcelFromDataSetTask;
            this.ExcelTargetWorkBook = excelTargetWorkbook;
            this.ExcelTemplateWorkbook = excelTemplateWorkbook;
            this.ExplicitlyAppendToTargetExcelFile = explictlyAppendToTargetExcelFile;

            if (cancellationSource == null)
            {
                this.CancellationToken = new CancellationToken();
            }
            else
            {
                this.CancellationToken = cancellationSource.Token;
            }
        }

        /// <summary>
        /// This will explicitly set the AppendToWorkSheet property for all Excel load instances overriding their default value.
        /// </summary>
        /// <seealso cref="IExcel.AppendToWorkSheet"/>
        public bool? ExplicitlyAppendToTargetExcelFile { get; }
        public IFilePath ExcelTemplateWorkbook { get; }
        public IFilePath ExcelTargetWorkBook { get; }
        public Task<DataSet> DataSetTask { get; }
        public CancellationToken CancellationToken { get; }
        public DataSet DataSet { get; }

        public event OnActionEventHandler OnAction;

        static IEnumerable<System.Reflection.FieldInfo> DataTableNsmeFieldInfo = System.Reflection.Assembly.GetAssembly(typeof(LoadToExcel))
                                                                                    .GetTypes()
                                                                                    .Where(t => t.IsClass
                                                                                                && t != typeof(LoadToExcel)
                                                                                                && typeof(LoadToExcel).IsAssignableFrom(t))
                                                                                    .SelectMany(t => t.GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.FlattenHierarchy)
                                                                                                        .Where(fi => fi.IsLiteral && !fi.IsInitOnly && fi.Name == "DataTableName"));
        public Task<IFilePath> Load()
        {
            var excelTargetFileFound = this.ExcelTargetWorkBook.Exist();
            var excelTargetFileAttrs = excelTargetFileFound ? this.ExcelTargetWorkBook.GetAttributes() : System.IO.FileAttributes.Normal;

            if (this.ExcelTemplateWorkbook != null && !excelTargetFileFound)
            {
                this.CancellationToken.ThrowIfCancellationRequested();
                try
                {
                    if(this.ExcelTargetWorkBook.FileExtension != this.ExcelTemplateWorkbook.FileExtension)
                    {
                        this.ExcelTargetWorkBook.ReplaceFileExtension(this.ExcelTemplateWorkbook.FileExtension);
                    }

                    if (this.ExcelTemplateWorkbook.Copy(this.ExcelTargetWorkBook))
                    {
                        Logger.Instance.InfoFormat("Created Workbook \"{0}\" from Template \"{1}\"", this.ExcelTargetWorkBook.Path, this.ExcelTemplateWorkbook.Path);
                        excelTargetFileFound = true;
                        excelTargetFileAttrs = this.ExcelTargetWorkBook.GetAttributes();
                    }
                    else
                    {
                        var msg = string.Format("Creation of Workbook \"{0}\" from Template \"{1}\" Failed", this.ExcelTargetWorkBook.Path, this.ExcelTemplateWorkbook.Path);
                        Logger.Instance.Error(msg);
                        throw new System.IO.IOException(msg);
                    }
                }
                catch (System.Exception ex)
                {
                    var msg = string.Format("Creation of Workbook \"{0}\" from Template \"{1}\" Failed", this.ExcelTargetWorkBook.Path, this.ExcelTemplateWorkbook.Path);
                    Logger.Instance.Error(msg, ex);
                    throw;
                }
            }

            return this.DataSetTask.ContinueWith(task =>
            {
                var dataSet = task.Result;
                List<IExcel> loadInstances = new List<IExcel>();
                var onAction = this.OnAction;

                this.CancellationToken.ThrowIfCancellationRequested();

                try
                {
                    this.ExcelTargetWorkBook.SetAttributes(excelTargetFileAttrs | System.IO.FileAttributes.Hidden);

                    foreach (DataTable dataTable in dataSet.Tables)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        foreach (var fieldInfo in DataTableNsmeFieldInfo)
                        {
                            if (fieldInfo.GetValue(null) as string == dataTable.TableName)
                            {
                                this.CancellationToken.ThrowIfCancellationRequested();

                                var instance = (IExcel)Activator.CreateInstance(fieldInfo.DeclaringType, dataTable, this.ExcelTargetWorkBook);

                                if(this.ExplicitlyAppendToTargetExcelFile.HasValue)
                                {
                                    instance.AppendToWorkSheet = this.ExplicitlyAppendToTargetExcelFile.Value;
                                }

                                loadInstances.Add(instance);
                                if (onAction != null)
                                {
                                    instance.OnAction += onAction;
                                }
                            }
                        }
                    }
                    foreach (var instance in loadInstances)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        instance.Load();
                    }
                }
                finally
                {
                    this.ExcelTargetWorkBook.SetAttributes(excelTargetFileAttrs);
                }

                return this.ExcelTargetWorkBook;
            },
            this.CancellationToken,
            TaskContinuationOptions.OnlyOnRanToCompletion,
            TaskScheduler.Current);
        }
    }
}
