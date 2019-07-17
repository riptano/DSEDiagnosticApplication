using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using DSEDiagnosticLogger;
using Common;

namespace DSEDiagnosticToDataTable
{
    public sealed class MachineDataTable : DataTableLoad
    {
        public MachineDataTable(DSEDiagnosticLibrary.Cluster cluster, CancellationTokenSource cancellationSource = null, Guid? sessionId = null)
            : base(cluster, cancellationSource, sessionId)
        {}

        public override DataTable CreateInitializationTable()
        {
            var dtOSMachineInfo = new DataTable(TableNames.Machine, TableNames.Namespace);

            if (this.SessionId.HasValue) dtOSMachineInfo.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtOSMachineInfo.Columns.Add(ColumnNames.NodeIPAddress, typeof(string)).Unique = true;
            dtOSMachineInfo.Columns.Add(ColumnNames.DataCenter, typeof(string));
            dtOSMachineInfo.PrimaryKey = new System.Data.DataColumn[] { dtOSMachineInfo.Columns[ColumnNames.NodeIPAddress] };

            dtOSMachineInfo.Columns.Add("Cloud-VM Type", typeof(string)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("CPU Architecture", typeof(string)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Cores", typeof(int)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Physical Memory (MB)", typeof(int)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("OS", typeof(string)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("OS Version", typeof(string)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Kernel", typeof(string)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("TimeZone", typeof(string)).AllowDBNull = true;
            //CPU Load
            dtOSMachineInfo.Columns.Add("Average", typeof(decimal)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Idle", typeof(decimal)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("System", typeof(decimal)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("User", typeof(decimal)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("IOWait", typeof(decimal)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("StealTime", typeof(decimal)).AllowDBNull = true;
            //Memory
            dtOSMachineInfo.Columns.Add("Available", typeof(int)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Cache", typeof(int)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Buffers", typeof(int)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Shared", typeof(int)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Free", typeof(int)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Used", typeof(int)).AllowDBNull = true;
            //Java
            dtOSMachineInfo.Columns.Add("Vendor", typeof(string)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Model", typeof(string)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Runtime Name", typeof(string)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Runtime Version", typeof(string)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("GC", typeof(string)).AllowDBNull = true;
            //Java NonHeapMemoryUsage
            dtOSMachineInfo.Columns.Add("Non-Heap Committed", typeof(decimal)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Non-Heap Init", typeof(decimal)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Non-Heap Max", typeof(decimal)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Non-Heap Used", typeof(decimal)).AllowDBNull = true;
            //Javaa HeapMemoryUsage
            dtOSMachineInfo.Columns.Add("Heap Committed", typeof(decimal)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Heap Init", typeof(decimal)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Heap Max", typeof(decimal)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Heap Used", typeof(decimal)).AllowDBNull = true;

            //NTP
            dtOSMachineInfo.Columns.Add("Correction (ms)", typeof(int)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Polling (secs)", typeof(int)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Maximum Error (us)", typeof(int)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Estimated Error (us)", typeof(int)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Time Constant", typeof(int)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Precision (us)", typeof(decimal)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Frequency (ppm)", typeof(decimal)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Tolerance (ppm)", typeof(decimal)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Host Names", typeof(string)).AllowDBNull = true;

            dtOSMachineInfo.DefaultView.ApplyDefaultSort = false;
            dtOSMachineInfo.DefaultView.AllowDelete = false;
            dtOSMachineInfo.DefaultView.AllowEdit = false;
            dtOSMachineInfo.DefaultView.AllowNew = false;
            dtOSMachineInfo.DefaultView.Sort = string.Format("[{0}] ASC, [{1}] ASC", ColumnNames.DataCenter, ColumnNames.NodeIPAddress);

            return dtOSMachineInfo;
        }

        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        /// <exception cref="Exception">Should re-thrown any exception except for OperationCanceledException</exception>
        public override DataTable LoadTable()
        {
            this.Table.BeginLoadData();
            try
            {               
                int nbrItems = 0;
                var dcNodes = new List<DSEDiagnosticLibrary.INode>();

                foreach (var dataCenter in this.Cluster.DataCenters)
                {
                    nbrItems = 0;
                    this.CancellationToken.ThrowIfCancellationRequested();

                    Logger.Instance.InfoFormat("Loading Server Information for DC \"{0}\"", dataCenter.Name);

                    foreach (var node in dataCenter.Nodes)
                    {
                        this.ProcessNode(node, ref nbrItems);
                        dcNodes.Add(node);
                    }                    

                    Logger.Instance.InfoFormat("Loaded Server Information for DC \"{0}\", Total Nbr Items {1:###,###,##0}", dataCenter.Name, nbrItems);
                }

                foreach (var node in DSEDiagnosticLibrary.Cluster.GetUnAssocaitedNodes())
                {
                    if(dcNodes.Exists(n => n.Id.Equals(node.Id))) continue;

                    this.ProcessNode(node, ref nbrItems);
                    Logger.Instance.WarnFormat("Unkown Node Detected for Cluster \"{0}\", DC \"{1}\" Node \"{2}\"",
                                                node.Cluster?.Name,
                                                node.DataCenter?.Name,
                                                node.NodeName());
                }
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn("Loading Server Information Canceled");
            }
            finally
            {
                this.Table.AcceptChanges();
                this.Table.EndLoadData();
            }

            return this.Table;
        }

        private void ProcessNode(DSEDiagnosticLibrary.INode node, ref int nbrItems)
        {
            this.CancellationToken.ThrowIfCancellationRequested();

            var dataRow = this.Table.NewRow();

            if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

            dataRow.SetField(ColumnNames.NodeIPAddress, node.NodeName());
            dataRow.SetField(ColumnNames.DataCenter, node.DCName());

            dataRow.SetField("Cloud-VM Type", node.Machine.CloudVMType?.ToString());
            dataRow.SetField("CPU Architecture", node.Machine.CPU.Architecture);
            if (node.Machine.CPU.Cores.HasValue) dataRow.SetField("Cores", (int)node.Machine.CPU.Cores.Value);
            dataRow.SetFieldToInt("Physical Memory (MB)", node.Machine.Memory.PhysicalMemory, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);
            dataRow.SetField("OS", node.Machine.OS);
            dataRow.SetField("OS Version", node.Machine.OSVersion);
            dataRow.SetField("Kernel", node.Machine.Kernel);

            if (node.Machine.TimeZone == null)
            {
                dataRow.SetField("TimeZone", (node.Machine.TimeZoneName ?? string.Empty) + " (?)");
            }
            else if (node.Machine.UsesDefaultTZ)
            {
                dataRow.SetField("TimeZone", node.Machine.TimeZone.Name + " (default)");
            }
            else
            {
                dataRow.SetField("TimeZone", node.Machine.TimeZone.Name);
            }
            //CPU Load
            dataRow.SetFieldToDecimal("Average", node.Machine.CPULoad.Average, convertFromPercent: true)
                    .SetFieldToDecimal("Idle", node.Machine.CPULoad.Idle, convertFromPercent: true)
                    .SetFieldToDecimal("System", node.Machine.CPULoad.System, convertFromPercent: true)
                    .SetFieldToDecimal("User", node.Machine.CPULoad.User, convertFromPercent: true)
                    .SetFieldToDecimal("IOWait", node.Machine.CPULoad.User, convertFromPercent: true)
                    .SetFieldToDecimal("StealTime", node.Machine.CPULoad.User, convertFromPercent: true);
            //Memory
            dataRow.SetFieldToInt("Available", node.Machine.Memory.Available, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                    .SetFieldToInt("Cache", node.Machine.Memory.Cache, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                    .SetFieldToInt("Buffers", node.Machine.Memory.Buffers, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                    .SetFieldToInt("Shared", node.Machine.Memory.Shared, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                    .SetFieldToInt("Free", node.Machine.Memory.Free, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                    .SetFieldToInt("Used", node.Machine.Memory.Used, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);
            //Java
            dataRow.SetField("Vendor", node.Machine.Java.Vendor);
            if (node.Machine.Java.Model.HasValue) dataRow.SetField("Model", node.Machine.Java.Model.Value);
            dataRow.SetField("Runtime Name", node.Machine.Java.RuntimeName);
            dataRow.SetField("Runtime Version", node.Machine.Java.Version);
            dataRow.SetField("GC", node.Machine.Java.GCType);
            //Java NonHeapMemoryUsage
            dataRow.SetFieldToDecimal("Non-Heap Committed", node.Machine.Java.NonHeapMemory.Committed, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                    .SetFieldToDecimal("Non-Heap Init", node.Machine.Java.NonHeapMemory.Initial, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                    .SetFieldToDecimal("Non-Heap Max", node.Machine.Java.NonHeapMemory.Maximum, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                    .SetFieldToDecimal("Non-Heap Used", node.Machine.Java.NonHeapMemory.Used, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);
            //Javaa HeapMemoryUsage
            dataRow.SetFieldToDecimal("Heap Committed", node.Machine.Java.HeapMemory.Committed, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                    .SetFieldToDecimal("Heap Init", node.Machine.Java.HeapMemory.Initial, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                    .SetFieldToDecimal("Heap Max", node.Machine.Java.HeapMemory.Maximum, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                    .SetFieldToDecimal("Heap Used", node.Machine.Java.HeapMemory.Used, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);

            //NTP
            dataRow.SetFieldToInt("Correction (ms)", node.Machine.NTP.Correction, DSEDiagnosticLibrary.UnitOfMeasure.Types.MS)
                    .SetFieldToInt("Polling (secs)", node.Machine.NTP.Polling, DSEDiagnosticLibrary.UnitOfMeasure.Types.SEC)
                    .SetFieldToInt("Maximum Error (us)", node.Machine.NTP.MaximumError, DSEDiagnosticLibrary.UnitOfMeasure.Types.us)
                    .SetFieldToInt("Estimated Error (us)", node.Machine.NTP.EstimatedError, DSEDiagnosticLibrary.UnitOfMeasure.Types.us)
                    .SetFieldToDecimal("Precision (us)", node.Machine.NTP.Precision, DSEDiagnosticLibrary.UnitOfMeasure.Types.us)
                    .SetFieldToDecimal("Frequency (ppm)", node.Machine.NTP.Frequency)
                    .SetFieldToDecimal("Tolerance (ppm)", node.Machine.NTP.Tolerance);
            if (node.Machine.NTP.TimeConstant.HasValue) dataRow.SetField("Time Constant", node.Machine.NTP.TimeConstant.Value);

            if (node.Id.HostNames != null && node.Id.HostNames.HasAtLeastOneElement()) dataRow.SetField("Host Names", string.Join(", ", node.Id.HostNames));

            this.Table.Rows.Add(dataRow);
            ++nbrItems;
        }
    }
}
