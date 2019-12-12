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

        /// <summary>
        /// The creation/loading of this Data table is dependent on the data from the reference Data table.
        /// </summary>
        IDataTable ReferenceDataTable { get; set; }

        IAsyncResult ReferenceDataTableWaitHandler { get; set; }

        DataTable Table { get; }
        
        CancellationToken CancellationToken { get;}
        
        DataTable CreateInitializationTable();
        
        /// <summary>
        /// Loads data table with information from the cluster instance
        /// </summary>
        /// <remarks>
        /// Note that if a DataSet is actually returned, this will return null and LoadDataSet is required to be called! 
        /// </remarks>
        /// <returns>Loaded data table or null</returns>
        /// <exception cref="Exception">Should re-thrown any exception except for OperationCanceledException</exception>
        DataTable LoadTable();        
    }

    public interface ILogFileStats
    {
        IEnumerable<DSEDiagnosticLibrary.IAggregatedStats> LogFileStats { get; set; }
    }
}
