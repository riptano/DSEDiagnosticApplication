using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DSEDiagnosticFileParser;
using DSEDiagnosticLibrary;
using Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticFileParser.Tests
{
    [TestClass]
    public class LogParsingTest_GCStatusLogger_Graph
    {
        const bool LogEventsAreMemoryMapped = true;
        static readonly string ClusterName = "xnetdse-prod1";

        private DSEDiagnosticLibrary.Cluster _cluster;

        //[TestMethod]
        public void CreateClusterDCNodeDDL_Graph_Log()
        {
            DSEDiagnosticLibrary.LibrarySettings.LogEventsAreMemoryMapped = LogEventsAreMemoryMapped;

            this._cluster = DSEDiagnosticLibrary.Cluster.TryGetAddCluster(ClusterName);

            Assert.AreEqual(ClusterName, this._cluster?.Name);
            Assert.AreEqual(this._cluster, DSEDiagnosticLibrary.Cluster.TryGetCluster(ClusterName));

            var dseringPath = Common.Path.PathUtils.BuildFilePath(@".\Files\DSE5-Graph-dsering");
            var dseRingParsing = new file_dsetool_ring(DiagnosticFile.CatagoryTypes.CommandOutputFile,
                                                            dseringPath.ParentDirectoryPath,
                                                            dseringPath,
                                                            null, ClusterName, null, null);

            var nbrLinesParsed = dseRingParsing.ProcessFile();

            Assert.AreEqual((uint) 30, nbrLinesParsed);            
            Assert.AreEqual(4, this._cluster.DataCenters.Count());
            Assert.AreEqual(30, this._cluster.Nodes.Count());
            Assert.IsNotNull(this._cluster.TryGetDataCenter("Chicago"));
            Assert.AreEqual(12, this._cluster.TryGetDataCenter("Chicago").Nodes.Count());
            Assert.AreEqual(this._cluster, this._cluster.TryGetDataCenter("Chicago").Cluster);
            Assert.IsNotNull(this._cluster.TryGetNode("172.26.2.190"));
            Assert.AreEqual("Chicago1", this._cluster.TryGetNode("172.26.2.190").DataCenter.Name);
            Assert.AreEqual("00-3A-7D-6A-A6-84", this._cluster.TryGetNode("172.26.2.190").DSE.PhysicalServerId);
            Assert.AreEqual(DSEInfo.InstanceTypes.Cassandra | DSEInfo.InstanceTypes.Graph | DSEInfo.InstanceTypes.MultiInstance, this._cluster.TryGetNode("172.26.2.190").DSE.InstanceType);
            Assert.AreEqual((uint)32, this._cluster.TryGetNode("172.26.2.190").DSE.NbrTokens.Value);
            Assert.AreEqual(new UnitOfMeasure("910.22 KB"), this._cluster.TryGetNode("172.26.2.190").DSE.StorageUsed);

            var ddlPath = Common.Path.PathUtils.BuildFilePath(@".\DDL\graph_ddl.cql");
            var ddlParsing = new DSEDiagnosticFileParser.cql_ddl(ddlPath, ClusterName, null);

            nbrLinesParsed = ddlParsing.ProcessFile();

            Assert.AreEqual((uint) 669, nbrLinesParsed);
            Assert.AreNotEqual(0, this._cluster.Keyspaces.Count());

            var keyspace = "xnetgraph";
            Assert.IsNotNull(this._cluster.GetKeyspaces(keyspace).FirstOrDefault());
            Assert.AreEqual(1, this._cluster.GetKeyspaces(keyspace).Count());
            Assert.AreEqual(201, this._cluster.GetKeyspaces(keyspace).First().GetTables().Count());
            Assert.AreEqual(6, this._cluster.GetKeyspaces(keyspace).First().GetViews().Count());
            Assert.AreEqual(6, this._cluster.GetKeyspaces(keyspace).First().GetIndexes().Count());
            Assert.AreEqual(2, this._cluster.GetKeyspaces(keyspace).First().Replications.Count());

            keyspace = "xnetgraph1";
            
            Assert.IsNotNull(this._cluster.GetKeyspaces(keyspace).FirstOrDefault());
            Assert.AreEqual(1, this._cluster.GetKeyspaces(keyspace).Count());
            Assert.AreEqual(77, this._cluster.GetKeyspaces(keyspace).First().GetTables().Count());
            Assert.AreEqual(0, this._cluster.GetKeyspaces(keyspace).First().GetViews().Count());
            Assert.AreEqual(0, this._cluster.GetKeyspaces(keyspace).First().GetIndexes().Count());
            Assert.AreEqual(1, this._cluster.GetKeyspaces(keyspace).First().Replications.Count());
            Assert.AreEqual("Chicago", this._cluster.GetKeyspaces(keyspace).First().Replications.First().DataCenter.Name);
        }

        [TestMethod]
        public void ProcessFileTest_Graph_GCStatusLogger()
        {
            this.CreateClusterDCNodeDDL_Graph_Log();
            
            var testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\GCStatusLogger.log");

            var parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, 
                                                        testLogFile.ParentDirectoryPath,
                                                        testLogFile, this._cluster.TryGetNode("172.26.2.132"), ClusterName, null, null);
            parseFile.CheckOverlappingDateRange = false;
            parseFile.DebugLogProcessing = file_cassandra_log4net.DebugLogProcessingTypes.AllMsg;

            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)610, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(637, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);

            var result = parseFile.Result as file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(this._cluster, result.Cluster);
            Assert.AreEqual(this._cluster.TryGetNode("172.26.2.132"), result.Node);

            result.ResultsTask.Wait();

            Assert.AreEqual(610, result.ResultsTask.Result.Count());
            Assert.AreEqual(610, result.Results.Count());
            Assert.AreEqual(610, result.Node.LogEvents.Count());
            Assert.IsNotNull(result.Node.LogFiles);
            Assert.AreEqual(1, result.Node.LogFiles.Count());
            Assert.AreEqual(DateTimeOffset.Parse("3/2/2018 4:23:15.916 +00:00"), result.Node.LogFiles.First().LogDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("3/5/2018 18:43:22.740 +00:00"), result.Node.LogFiles.First().LogDateRange.Max);
            Assert.AreEqual(DateTimeOffset.Parse("3/2/2018 4:23:15.916 +00:00"), result.Node.LogFiles.First().LogFileDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("3/5/2018 18:43:22.740 +00:00"), result.Node.LogFiles.First().LogDateRange.Max);
            Assert.AreEqual(new UnitOfMeasure("75621 Byte"), result.Node.LogFiles.First().LogFileSize);
            Assert.AreEqual(610, result.Node.LogFiles.First().LogItems);
            Assert.AreEqual(0, result.Node.LogFiles.First().OrphanedEvents.Count);
            Assert.IsNotNull(result.Node.LogFiles.First().Restarts);
            Assert.AreEqual(DateTimeOffset.Parse("2018-03-05 17:43:30.050 +00:00"), result.Node.LogFiles.First().Restarts.First().Min);
            Assert.AreEqual(DateTimeOffset.Parse("2018-03-05 18:42:27.050 +00:00"), result.Node.LogFiles.First().Restarts.First().Max);
            
            
        }
    }
}
