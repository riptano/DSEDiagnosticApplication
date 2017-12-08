using Microsoft.VisualStudio.TestTools.UnitTesting;
using DSEDiagnosticFileParser;
using DSEDiagnosticLibrary;
using Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticFileParser.Tests
{
    [TestClass()]
    public class file_dsetool_ringTests
    {
        static readonly string FilePath = @".\Files\DSE5-ring";
        static readonly string ClusterName = "TstCluster";
        static readonly string DC1 = "ashburn";
        static readonly string DC2 = "hillsboro";
        static readonly string NodeName1 = "10.146.117.95";
        static readonly string NodeName2 = "172.27.205.229";
        
        [TestMethod()]
        public void file_dsetool_ringTest()
        {
            var testLogFile = Common.Path.PathUtils.BuildFilePath(FilePath);
            var readNode = Cluster.TryGetAddNode(NodeName1);

            Assert.IsNotNull(readNode);
            Assert.IsNull(readNode.DataCenter);
            Assert.IsTrue(readNode.Cluster.IsMaster);
            Assert.AreEqual(1, Cluster.GetUnAssocaitedNodes().Count());

            var parseFile = new file_dsetool_ring(DiagnosticFile.CatagoryTypes.CommandOutputFile,
                                                    testLogFile.ParentDirectoryPath,
                                                    testLogFile,
                                                    readNode, null, null, null);

            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(0, Cluster.GetUnAssocaitedNodes().Count());
            Assert.AreEqual((uint) 24, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrItemGenerated);
            Assert.AreEqual(2, Cluster.MasterCluster.DataCenters.Count());
            Assert.IsNull(parseFile.Exception);

            var nodes = Cluster.GetNodes(false);
            Assert.AreEqual(24, nodes.Count());

            foreach (var node in nodes)
            {
                Assert.IsNotNull(node.DataCenter);
                Assert.IsNotNull(node.Cluster);
                Assert.IsTrue(node.Cluster.IsMaster);
                Assert.IsTrue(node.DataCenter.Cluster.IsMaster);

                if (node.Id.NodeName().StartsWith("10."))
                {
                    Assert.AreEqual(DC1, node.DataCenter.Name);
                }
                else
                {
                    Assert.AreEqual(DC2, node.DataCenter.Name);
                }
            }

            var dc1Instance = Cluster.GetCurrentOrMaster().TryGetDataCenter(DC1);
            Assert.IsNotNull(dc1Instance);
            Assert.AreEqual(readNode, dc1Instance.TryGetNode(NodeName1));
            Assert.IsTrue(ReferenceEquals(readNode, Cluster.TryGetAddNode(NodeName1)));

            var cluster = Cluster.NameClusterAssociatewItem(readNode, ClusterName);
            var readNode2 = Cluster.TryGetAddNode(NodeName2);

            Assert.IsNotNull(readNode2.DataCenter);
            Assert.AreEqual(DC2, readNode2.DataCenter.Name);
            Assert.IsTrue(readNode2.Cluster.IsMaster);
            Assert.AreEqual(0, Cluster.GetUnAssocaitedNodes().Count());

            Assert.AreEqual(cluster, Cluster.NameClusterAssociatewItem(readNode2, ClusterName));

            Assert.AreEqual(2, Cluster.MasterCluster.DataCenters.Count());
            Assert.AreEqual(2, cluster.DataCenters.Count());

            foreach (var node in nodes)
            {
                Assert.IsNotNull(node.DataCenter);
                Assert.IsNotNull(node.Cluster);
                Assert.IsFalse(node.Cluster.IsMaster);
                Assert.IsFalse(node.DataCenter.Cluster.IsMaster);
                Assert.AreEqual(cluster, node.Cluster);
                Assert.AreEqual(cluster, node.DataCenter.Cluster);

                if (node.Id.NodeName().StartsWith("10."))
                {
                    Assert.AreEqual(DC1, node.DataCenter.Name);
                }
                else
                {
                    Assert.AreEqual(DC2, node.DataCenter.Name);
                }
            }

            nodes = cluster.Nodes;

            foreach (var node in nodes)
            {
                Assert.IsNotNull(node.DataCenter);
                Assert.IsNotNull(node.Cluster);
                Assert.IsFalse(node.Cluster.IsMaster);
                Assert.IsFalse(node.DataCenter.Cluster.IsMaster);
                Assert.AreEqual(cluster, node.Cluster);
                Assert.AreEqual(cluster, node.DataCenter.Cluster);

                if (node.Id.NodeName().StartsWith("10."))
                {
                    Assert.AreEqual(DC1, node.DataCenter.Name);
                }
                else
                {
                    Assert.AreEqual(DC2, node.DataCenter.Name);
                }
            }

        }

        
    }
}