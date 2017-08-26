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
                            IFilePath excelTargetWorkbook)
        {
            this.ExcelTargetWorkBook = excelTargetWorkbook;
            this.DataSet = loadExcelFromDataSet;
            this.DataSetTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask(loadExcelFromDataSet);
        }

        public LoadDataSet(Task<DataSet> loadExcelFromDataSetTask,
                            IFilePath excelTargetWorkbook,
                            CancellationTokenSource cancellationSource = null)
        {
            this.DataSetTask = loadExcelFromDataSetTask;
            this.ExcelTargetWorkBook = excelTargetWorkbook;

            if (cancellationSource == null)
            {
                this.CancellationToken = new CancellationToken();
            }
            else
            {
                this.CancellationToken = cancellationSource.Token;
            }
        }

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
        public Task Load()
        {
            return this.DataSetTask.ContinueWith(task =>
            {
                var dataSet = task.Result;
                List<IExcel> loadInstances = new List<IExcel>();
                var onAction = this.OnAction;

                foreach(DataTable dataTable in dataSet.Tables)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    foreach(var fieldInfo in DataTableNsmeFieldInfo)
                    {
                        if(fieldInfo.GetValue(null) as string == dataTable.TableName)
                        {
                            var instance = (IExcel)Activator.CreateInstance(fieldInfo.DeclaringType, dataTable, this.ExcelTargetWorkBook);

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
                    instance.Load();
                }
            },
            this.CancellationToken,
            TaskContinuationOptions.OnlyOnRanToCompletion,
            TaskScheduler.Current);
        }
    }
}
