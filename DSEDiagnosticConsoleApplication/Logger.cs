using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using System.Data;
using log4net;

[assembly: log4net.Config.XmlConfigurator(Watch = true)]

namespace DSEDiagnosticConsoleApplication
{
    public static class Logger
    {

        static public readonly DSEDiagnosticLogger.Logger Instance = null;

        static Logger()
        {
            //log4net.Config.XmlConfigurator.Configure();
            var logRepository = LogManager.GetRepository(System.Reflection.Assembly.GetEntryAssembly());
            log4net.Config.XmlConfigurator.Configure(logRepository, new System.IO.FileInfo("log4net.config"));
            Instance = DSEDiagnosticLogger.Logger.Instance;
            Instance.Log4NetInstance = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        }

        public enum DumpType
        {
            Info,
            Warning,
            Error,
            Debug
        }
        public static DataRow[] Dump(this DataRow[] dataRows, DumpType dumpType, string comments = null, params object[] args)
        {
            string strComment = comments == null ? string.Empty : (string.Format(comments, args) + ":");
            var strRows = new StringBuilder();

            foreach (DataRow dataRow in dataRows)
            {
                strRows.AppendFormat("\tTable: \"{0}\"", dataRow.Table.TableName);
                strRows.AppendLine();

                if (!string.IsNullOrEmpty(dataRow.RowError))
                {
                    strRows.AppendFormat("\t\tRow Error: \"{0}\"", dataRow.RowError);
                    strRows.AppendLine();
                }

                foreach (DataColumn column in dataRow.Table.Columns)
                {
                    var columnError = dataRow.GetColumnError(column.Ordinal);

                    strRows.AppendFormat("\t\tColumn: \"{0}\"\t\tValue: \"{1}\"", column.ColumnName, dataRow[column.Ordinal]);
                    strRows.AppendLine();

                    if (!string.IsNullOrEmpty(columnError))
                    {
                        strRows.AppendFormat("\t\t\tColumn Error: \"{0}\"", columnError);
                        strRows.AppendLine();
                    }
                }
            }

            switch (dumpType)
            {
                case DumpType.Info:
                    Instance.Info(strComment + strRows.ToString());
                    break;
                case DumpType.Warning:
                    Instance.Warn(strComment + strRows.ToString());
                    break;
                case DumpType.Error:
                    Instance.Error(strComment + strRows.ToString());
                    break;
                case DumpType.Debug:
                    Instance.Debug(strComment + strRows.ToString());
                    break;
                default:
                    break;
            }

            return dataRows;
        }

        public static string[] Dump(this string[] strValues, DumpType dumpType, string comments = null, params object[] args)
        {
            string strComment = comments == null ? string.Empty : (string.Format(comments, args) + ":");
            var strValue = string.Join(", ", strValues);

            switch (dumpType)
            {
                case DumpType.Info:
                    Instance.Info(strComment + strValue);
                    break;
                case DumpType.Warning:
                    Instance.Warn(strComment + strValue);
                    break;
                case DumpType.Error:
                    Instance.Error(strComment + strValue);
                    break;
                case DumpType.Debug:
                    Instance.Debug(strComment + strValue);
                    break;
                default:
                    break;
            }

            return strValues;
        }

        public static string Dump(this string strItem, DumpType dumpType, string comments = null, params object[] args)
        {
            string strComment = comments == null ? string.Empty : (string.Format(comments, args) + ": ");

            switch (dumpType)
            {
                case DumpType.Info:
                    Instance.Info(strComment + strItem);
                    break;
                case DumpType.Warning:
                    Instance.Warn(strComment + strItem);
                    break;
                case DumpType.Error:
                    Instance.Error(strComment + strItem);
                    break;
                case DumpType.Debug:
                    Instance.Debug(strComment + strItem);
                    break;
                default:
                    break;
            }

            return strItem;
        }

        public static bool Flush()
        {
            bool bResult = true;

            try
            {
                var rep = Instance.Log4NetInstance.Logger.Repository;

                rep.GetAppenders().Cast<log4net.Appender.IFlushable>()
                    .ForEach(appender =>
                    {
                        appender.Flush(2000);
                    });
                bResult = true;
            }
            catch
            {
                Logger.Instance.Error("Log4Net Appender Flush Failed!");
            }

            return bResult;
        }
    }
}
