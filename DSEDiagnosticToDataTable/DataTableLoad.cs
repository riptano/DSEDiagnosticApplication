using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using Common;

namespace DSEDiagnosticToDataTable
{
    public abstract class DataTableLoad : IDataTable
    {
        public DataTableLoad(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource, Guid? sessionId = null)
        {
            this.Cluster = cluster;
            
            if (cancellationSource == null)
            {
                this.CancellationToken = new CancellationToken();
            }
            else
            {
                this.CancellationToken = cancellationSource.Token;
            }
            this.SessionId = sessionId;

            this.Table = this.CreateInitializationTable();
        }

        public DataTableLoad(DSEDiagnosticLibrary.Cluster cluster,
                                CancellationTokenSource cancellationSource,
                                IDataTable referenceDT,
                                IAsyncResult referenceWaitHandler = null,
                                Guid? sessionId = null)
        {
            this.Cluster = cluster;
            
            if (cancellationSource == null)
            {
                this.CancellationToken = new CancellationToken();
            }
            else
            {
                this.CancellationToken = cancellationSource.Token;
            }
            this.ReferenceDataTable = referenceDT;
            this.ReferenceDataTableWaitHandler = referenceWaitHandler;
            this.SessionId = sessionId;

            this.Table = this.CreateInitializationTable();
        }

        public DataTableLoad(DSEDiagnosticLibrary.Cluster cluster,
                                CancellationTokenSource cancellationSource,
                                DataTable sourceTable,
                                Guid? sessionId = null)
        {
            this.Cluster = cluster;
            
            if (cancellationSource == null)
            {
                this.CancellationToken = new CancellationToken();
            }
            else
            {
                this.CancellationToken = cancellationSource.Token;
            }
            this.SessionId = sessionId;
            this.SourceTable = sourceTable;
            this.Table = this.CreateInitializationTable();
        }

        public Guid? SessionId { get; }
        public DSEDiagnosticLibrary.Cluster Cluster { get; }

        public IDataTable ReferenceDataTable { get; set; }

        public IAsyncResult ReferenceDataTableWaitHandler { get; set; }

        public DataTable Table { get; }
        public DataTable SourceTable { get; }
        
        public CancellationToken CancellationToken { get; }

        public override string ToString()
        {
            return string.Format("{0}<Rows={1}>", this.Table?.TableName ?? "null", this.Table?.Rows.Count ?? 0);
        }

        abstract public DataTable CreateInitializationTable();       
        
        /// <summary>
        /// Loads data table with information from the cluster instance
        /// </summary>
        /// <remarks>
        /// Note that if a DataSet is actually returned, this will return null and LoadDataSet is required to be called! 
        /// </remarks>
        /// <returns>Loaded data table or null</returns>
        /// <exception cref="Exception">Should re-thrown any exception except for OperationCanceledException</exception>
        abstract public DataTable LoadTable();        
    }
}
