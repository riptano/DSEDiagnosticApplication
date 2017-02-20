using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

[assembly: log4net.Config.XmlConfigurator(Watch = true)]

namespace DSEDiagnosticApplication
{
    public static class Logger
    {
        static public readonly log4net.ILog Instance = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        static Logger()
        {
            DSEDiagnosticFileParser.Logger.Instance = Instance;
            log4net.Config.XmlConfigurator.Configure();
        }

        public enum DumpType
        {
            Info,
            Warning,
            Error,
            Debug
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

    }
}
