using Microsoft.VisualStudio.TestTools.UnitTesting;
using DSEDiagnosticLibrary;
using DSEDiagnosticFileParser;
using Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticFileParser.Tests
{
    [TestClass()]
    public class file_yamlTests
    {

        static readonly string FilePathCassandra = @".\Files\cassandra.yaml";
        static readonly string FilePathDSE = @".\Files\dse.yaml";
        static readonly string ClusterName = "TstCluster";
        static readonly string DC1 = "DC1";
        static readonly string DC2 = "DC2";
        static readonly string NodeName1 = "10.146.117.93"; 
        static readonly string NodeName2 = "10.146.117.99";
        static readonly string NodeName3 = "10.0.0.1";

        private DSEDiagnosticLibrary.Cluster _cluster;
        private DSEDiagnosticLibrary.IDataCenter _datacenter1;
        private DSEDiagnosticLibrary.IDataCenter _datacenter2;
        private DSEDiagnosticLibrary.INode _node1; //DC1
        private DSEDiagnosticLibrary.INode _node2; //DC1
        private DSEDiagnosticLibrary.INode _node3; //DC1

        public struct ItemResult
        {
            public string type;
            public string tostring;
        }

        public void CreateClusterDCNodeDDL()
        {
            this._cluster = DSEDiagnosticLibrary.Cluster.TryGetAddCluster(ClusterName);

            Assert.AreEqual(ClusterName, this._cluster?.Name);
            Assert.AreEqual(this._cluster, DSEDiagnosticLibrary.Cluster.GetCurrentOrMaster());

            this._datacenter1 = DSEDiagnosticLibrary.Cluster.TryGetAddDataCenter(DC1, this._cluster);
            Assert.AreEqual(DC1, this._datacenter1?.Name);
            Assert.AreEqual(this._datacenter1, this._cluster.TryGetDataCenter(DC1));

            this._datacenter2 = DSEDiagnosticLibrary.Cluster.TryGetAddDataCenter(DC2, this._cluster);
            Assert.AreEqual(DC2, this._datacenter2?.Name);
            Assert.AreEqual(this._datacenter2, this._cluster.TryGetDataCenter(DC2));

            this._node1 = DSEDiagnosticLibrary.Cluster.TryGetAddNode(NodeName1, this._datacenter1);
            Assert.AreEqual(NodeName1, this._node1?.Id.NodeName());
            Assert.AreEqual(this._node1, this._datacenter1.TryGetNode(NodeName1));

            this._node2 = DSEDiagnosticLibrary.Cluster.TryGetAddNode(NodeName2, this._datacenter1);
            Assert.AreEqual(NodeName2, this._node2?.Id.NodeName());
            Assert.AreEqual(this._node2, this._datacenter1.TryGetNode(NodeName2));

            this._node3 = DSEDiagnosticLibrary.Cluster.TryGetAddNode(NodeName3, this._datacenter1);
            Assert.AreEqual(NodeName3, this._node3?.Id.NodeName());
            Assert.AreEqual(this._node3, this._datacenter1.TryGetNode(NodeName3));
        }

        [TestMethod()]
        public void file_yamlTest()
        {
            this.CreateClusterDCNodeDDL();

            Assert.AreEqual(this._cluster, DSEDiagnosticLibrary.Cluster.GetCurrentOrMaster());
            Assert.IsNotNull(this._node1);
            Assert.IsNotNull(this._node2);
            Assert.IsNotNull(this._node3);

            var testLogFile = Common.Path.PathUtils.BuildFilePath(FilePathCassandra);

            var parseFile = new file_yaml(DiagnosticFile.CatagoryTypes.ConfigurationFile,
                                            testLogFile.ParentDirectoryPath,
                                            testLogFile,
                                            this._node3, null, null, null);

            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint) 96, nbrLinesParsed);
            Assert.IsNull(parseFile.Exception);
            Assert.AreEqual(0, parseFile.NbrWarnings);
            Assert.AreEqual(973, parseFile.NbrItemsParsed);
            Assert.AreEqual(0, parseFile.NbrItemGenerated);
            Assert.AreEqual(this._node3, parseFile.Node);
            Assert.IsFalse(parseFile.Canceled);
            Assert.AreEqual(this._cluster, parseFile.Result.Cluster);
            Assert.AreEqual(this._node3, parseFile.Result.Node);
            Assert.AreEqual(this._datacenter1, parseFile.Result.DataCenter);
            Assert.AreEqual(96, parseFile.Result.NbrItems);
            Assert.AreEqual(testLogFile, parseFile.Result.Path);
            Assert.AreEqual(96, parseFile.Result.Results.Count());

            var testJsonFile = Common.Path.PathUtils.BuildFilePath(@".\ResultFiles\cassandrayaml.json");
            var definition = new { type = "", values = new ItemResult[0] };
            var testCompareResultsCassandra = Newtonsoft.Json.JsonConvert.DeserializeAnonymousType(testJsonFile.ReadAllText(), definition);

            Assert.AreEqual(96, testCompareResultsCassandra.values.Length);

            testCompareResultsCassandra.values.For(0, (idx, item) => Assert.AreEqual(item.tostring, parseFile.Result.Results.ElementAt(idx).ToString()));

            ///DSE yaml
            testLogFile = Common.Path.PathUtils.BuildFilePath(FilePathDSE);

            parseFile = new file_yaml(DiagnosticFile.CatagoryTypes.ConfigurationFile,
                                            testLogFile.ParentDirectoryPath,
                                            testLogFile,
                                            this._node3, null, null, null);

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)159, nbrLinesParsed);
            Assert.IsNull(parseFile.Exception);
            Assert.AreEqual(0, parseFile.NbrWarnings);
            Assert.AreEqual(220, parseFile.NbrItemsParsed);
            Assert.AreEqual(0, parseFile.NbrItemGenerated);
            Assert.AreEqual(this._node3, parseFile.Node);
            Assert.IsFalse(parseFile.Canceled);
            Assert.AreEqual(this._cluster, parseFile.Result.Cluster);
            Assert.AreEqual(this._node3, parseFile.Result.Node);
            Assert.AreEqual(this._datacenter1, parseFile.Result.DataCenter);
            Assert.AreEqual(159, parseFile.Result.NbrItems);
            Assert.AreEqual(testLogFile, parseFile.Result.Path);
            Assert.AreEqual(159, parseFile.Result.Results.Count());

            testJsonFile = Common.Path.PathUtils.BuildFilePath(@".\ResultFiles\dseyaml.json");

            var testCompareResultsDSE = Newtonsoft.Json.JsonConvert.DeserializeAnonymousType(testJsonFile.ReadAllText(), definition);

            Assert.AreEqual(159, testCompareResultsDSE.values.Length);

            testCompareResultsDSE.values.For(0, (idx, item) => Assert.AreEqual(item.tostring, parseFile.Result.Results.ElementAt(idx).ToString()));

            //Check node's config
            Assert.AreEqual(159 + 96, this._node3.Configurations.Count());

            var nodeConfigProps = new List<Tuple<string, string>>();
            var resultFilePos = 0;
            var cassandraYaml = true;

            this._node3.Configurations.ForEach(i =>
            {
                var iObj = i.ParsedValue();
                nodeConfigProps.Add(new Tuple<string, string>(i.Property, iObj?.ToString()));
                
                if(i.Type == ConfigTypes.Cassandra)
                {
                    Assert.AreEqual(testCompareResultsCassandra.values[resultFilePos++].tostring, i.ToString());
                }
                else
                {
                    if(cassandraYaml)
                    {
                        resultFilePos = 0;
                        cassandraYaml = false;
                    }
                    Assert.AreEqual(testCompareResultsDSE.values[resultFilePos++].tostring, i.ToString());
                }
            });

            testJsonFile = Common.Path.PathUtils.BuildFilePath(@".\ResultFiles\nodeConfigyaml.json");

            var testCompareResultsNode = Newtonsoft.Json.JsonConvert.DeserializeAnonymousType(testJsonFile.ReadAllText(), definition);

            Assert.AreEqual(159 + 96, testCompareResultsNode.values.Length);

            testCompareResultsNode.values.For(0, (idx, item) => Assert.AreEqual(item.tostring,
                                                                                    string.Format("({0}, {1})", nodeConfigProps[idx].Item1, nodeConfigProps[idx].Item2)));
        }


    }
}