using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticInsightsES
{
    public static class LibrarySettings
    {
        static LibrarySettings() { }

#if DEBUG 
        public const bool ESEnableDebuggingDefault = true;
#else
        public const bool ESEnableDebuggingDefault = Properties.Settings.Default.ESEnableDebugging;
#endif

        public static bool ESEnableDebugging = ESEnableDebuggingDefault;

    }
}
