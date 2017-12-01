using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using DSEDiagnosticLogger;

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

            dtOSMachineInfo.Columns.Add("Instance Type", typeof(string)).AllowDBNull = true;//c
            dtOSMachineInfo.Columns.Add("CPU Architecture", typeof(string));
            dtOSMachineInfo.Columns.Add("Cores", typeof(int)).AllowDBNull = true; //e
            dtOSMachineInfo.Columns.Add("Physical Memory (MB)", typeof(int)); //f
            dtOSMachineInfo.Columns.Add("OS", typeof(string));
            dtOSMachineInfo.Columns.Add("OS Version", typeof(string));
            dtOSMachineInfo.Columns.Add("TimeZone", typeof(string));
            //CPU Load
            dtOSMachineInfo.Columns.Add("Average", typeof(decimal)); //j
            dtOSMachineInfo.Columns.Add("Idle", typeof(decimal));
            dtOSMachineInfo.Columns.Add("System", typeof(decimal));
            dtOSMachineInfo.Columns.Add("User", typeof(decimal)); //m
                                                                  //Memory
            dtOSMachineInfo.Columns.Add("Available", typeof(int)); //n
            dtOSMachineInfo.Columns.Add("Cache", typeof(int));
            dtOSMachineInfo.Columns.Add("Buffers", typeof(int));
            dtOSMachineInfo.Columns.Add("Shared", typeof(int));
            dtOSMachineInfo.Columns.Add("Free", typeof(int));
            dtOSMachineInfo.Columns.Add("Used", typeof(int)); //s
                                                              //Java
            dtOSMachineInfo.Columns.Add("Vendor", typeof(string));//t
            dtOSMachineInfo.Columns.Add("Model", typeof(string));
            dtOSMachineInfo.Columns.Add("Runtime Name", typeof(string));
            dtOSMachineInfo.Columns.Add("Runtime Version", typeof(string));//w
            dtOSMachineInfo.Columns.Add("GC", typeof(string)).AllowDBNull = true;
            //Java NonHeapMemoryUsage
            dtOSMachineInfo.Columns.Add("Non-Heap Committed", typeof(decimal)); //y
            dtOSMachineInfo.Columns.Add("Non-Heap Init", typeof(decimal));
            dtOSMachineInfo.Columns.Add("Non-Heap Max", typeof(decimal));//aa
            dtOSMachineInfo.Columns.Add("Non-Heap Used", typeof(decimal));//ab
            //Javaa HeapMemoryUsage
            dtOSMachineInfo.Columns.Add("Heap Committed", typeof(decimal)); //ac
            dtOSMachineInfo.Columns.Add("Heap Init", typeof(decimal)); //ad
            dtOSMachineInfo.Columns.Add("Heap Max", typeof(decimal)); //ae
            dtOSMachineInfo.Columns.Add("Heap Used", typeof(decimal)); //af

            //DataStax Versions
            dtOSMachineInfo.Columns.Add("DSE", typeof(string)).AllowDBNull = true; //ag
            dtOSMachineInfo.Columns.Add("Cassandra", typeof(string)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Search", typeof(string)).AllowDBNull = true;
            dtOSMachineInfo.Columns.Add("Spark", typeof(string)).AllowDBNull = true;//aj
            dtOSMachineInfo.Columns.Add("Agent", typeof(string)).AllowDBNull = true; //ak
            dtOSMachineInfo.Columns.Add("VNodes", typeof(bool)).AllowDBNull = true; //al

            //NTP
            dtOSMachineInfo.Columns.Add("Correction (ms)", typeof(int)); //am
            dtOSMachineInfo.Columns.Add("Polling (secs)", typeof(int));
            dtOSMachineInfo.Columns.Add("Maximum Error (us)", typeof(int));
            dtOSMachineInfo.Columns.Add("Estimated Error (us)", typeof(int));
            dtOSMachineInfo.Columns.Add("Time Constant", typeof(int)); //aq
            dtOSMachineInfo.Columns.Add("Precision (us)", typeof(decimal)); //ar
            dtOSMachineInfo.Columns.Add("Frequency (ppm)", typeof(decimal));
            dtOSMachineInfo.Columns.Add("Tolerance (ppm)", typeof(decimal)); //at

            dtOSMachineInfo.DefaultView.ApplyDefaultSort = false;
            dtOSMachineInfo.DefaultView.AllowDelete = false;
            dtOSMachineInfo.DefaultView.AllowEdit = false;
            dtOSMachineInfo.DefaultView.AllowNew = false;
            dtOSMachineInfo.DefaultView.Sort = string.Format("[{0}] ASC, [{1}] ASC", ColumnNames.NodeIPAddress, ColumnNames.DataCenter);

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
                DataRow dataRow = null;
                int nbrItems = 0;

                foreach (var dataCenter in this.Cluster.DataCenters)
                {
                    nbrItems = 0;
                    this.CancellationToken.ThrowIfCancellationRequested();

                    Logger.Instance.InfoFormat("Loading Server Information for DC \"{0}\"", dataCenter.Name);

                    foreach (var node in dataCenter.Nodes)
                    {
                        this.CancellationToken.ThrowIfCancellationRequested();

                        dataRow = this.Table.NewRow();

                        if (this.SessionId.HasValue) dataRow.SetField(ColumnNames.SessionId, this.SessionId.Value);

                        dataRow.SetField(ColumnNames.NodeIPAddress, node.Id.NodeName());
                        dataRow.SetField(ColumnNames.DataCenter, dataCenter.Name);

                        dataRow.SetField("Instance Type", node.DSE.InstanceType.ToString());
                        dataRow.SetField("CPU Architecture", node.Machine.CPU.Architecture);
                        if(node.Machine.CPU.Cores.HasValue) dataRow.SetField("Cores", (int) node.Machine.CPU.Cores.Value);
                        dataRow.SetFieldToInt("Physical Memory (MB)", node.Machine.Memory.PhysicalMemory, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);
                        dataRow.SetField("OS", node.Machine.OS);
                        dataRow.SetField("OS Version", node.Machine.OSVersion);

                        if (node.Machine.TimeZone == null)
                        {
                            dataRow.SetField("TimeZone", (node.Machine.TimeZoneName ?? string.Empty) + " (?)");
                        }
                        else if(node.Machine.UsesDefaultTZ)                           
                        {
                            dataRow.SetField("TimeZone", node.Machine.TimeZone.Name + " (default)");
                        }
                        else
                        {
                            dataRow.SetField("TimeZone", node.Machine.TimeZone.Name);
                        }
                        //CPU Load
                        dataRow.SetFieldToDecimal("Average", node.Machine.CPULoad.Average)
                                .SetFieldToDecimal("Idle", node.Machine.CPULoad.Idle)
                                .SetFieldToDecimal("System", node.Machine.CPULoad.System)
                                .SetFieldToDecimal("User", node.Machine.CPULoad.User);
                        //Memory
                        dataRow.SetFieldToInt("Available", node.Machine.Memory.Available, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                                .SetFieldToInt("Cache", node.Machine.Memory.Cache, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                                .SetFieldToInt("Buffers", node.Machine.Memory.Buffers, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                                .SetFieldToInt("Shared", node.Machine.Memory.Shared, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                                .SetFieldToInt("Free", node.Machine.Memory.Free, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                                .SetFieldToInt("Used", node.Machine.Memory.Used, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);
                        //Java
                        dataRow.SetField("Vendor", node.Machine.Java.Vendor);
                        if(node.Machine.Java.Model.HasValue) dataRow.SetField("Model", node.Machine.Java.Model.Value);
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

                        //DataStax Versions
                        dataRow.SetField("DSE", node.DSE.Versions.DSE?.ToString());
                        dataRow.SetField("Cassandra", node.DSE.Versions.Cassandra?.ToString());
                        dataRow.SetField("Search", node.DSE.Versions.Search?.ToString());
                        dataRow.SetField("Spark", node.DSE.Versions.Analytics?.ToString());
                        dataRow.SetField("Agent", node.DSE.Versions.OpsCenterAgent?.ToString());
                        if(node.DSE.VNodesEnabled.HasValue) dataRow.SetField("VNodes", node.DSE.VNodesEnabled.Value);

                        //NTP
                        dataRow.SetFieldToInt("Correction (ms)", node.Machine.NTP.Correction, DSEDiagnosticLibrary.UnitOfMeasure.Types.MS)
                                .SetFieldToInt("Polling (secs)", node.Machine.NTP.Polling, DSEDiagnosticLibrary.UnitOfMeasure.Types.SEC)
                                .SetFieldToInt("Maximum Error (us)", node.Machine.NTP.MaximumError, DSEDiagnosticLibrary.UnitOfMeasure.Types.us)
                                .SetFieldToInt("Estimated Error (us)", node.Machine.NTP.EstimatedError, DSEDiagnosticLibrary.UnitOfMeasure.Types.us)
                                .SetFieldToDecimal("Precision (us)", node.Machine.NTP.Precision, DSEDiagnosticLibrary.UnitOfMeasure.Types.us)
                                .SetFieldToDecimal("Frequency (ppm)", node.Machine.NTP.Frequency)
                                .SetFieldToDecimal("Tolerance (ppm)", node.Machine.NTP.Tolerance);
                        if (node.Machine.NTP.TimeConstant.HasValue) dataRow.SetField("Time Constant", node.Machine.NTP.TimeConstant.Value);

                        this.Table.Rows.Add(dataRow);
                        ++nbrItems;
                    }

                    Logger.Instance.InfoFormat("Loaded Server Information for DC \"{0}\", Total Nbr Items {1:###,###,##0}", dataCenter.Name, nbrItems);
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
    }
}
