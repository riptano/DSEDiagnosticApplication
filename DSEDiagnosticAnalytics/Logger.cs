using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticAnalytics
{
    public static class Logger
    {
        public static readonly DSEDiagnosticLogger.Logger Instance = DSEDiagnosticLogger.Logger.Instance;
    }
}
