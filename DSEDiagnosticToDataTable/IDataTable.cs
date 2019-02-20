using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;

namespace DSEDiagnosticToDataTable
{
    public interface IDataTable
    {
        DSEDiagnosticLibrary.Cluster Cluster { get; }
        DataTable Table { get; }
        CancellationToken CancellationToken { get;}
        
        DataTable CreateInitializationTable();

        /// <summary>
        /// Loads data table with information from the cluster instance
        /// </summary>
        /// <returns>Loaded data table</returns>
        /// <exception cref="Exception">Should re-thrown any exception except for OperationCanceledException</exception>
        DataTable LoadTable();
    }

    public interface ILogFileStats
    {
        IEnumerable<DSEDiagnosticLibrary.IAggregatedStats> LogFileStats { get; set; }
    }
}
