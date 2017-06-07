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
        static public readonly DSEDiagnosticLogger.Logger Instance = null;

        static Logger()
        {
            log4net.Config.XmlConfigurator.Configure();
            Instance = DSEDiagnosticLogger.Logger.Instance;
            Instance.Log4NetInstance = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        }

    }
}
