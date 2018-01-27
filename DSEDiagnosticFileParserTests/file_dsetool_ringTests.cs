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
        static readonly string ClusterName = "TstClusterRing";
        static readonly string DC1 = "ashburn";
        static readonly string DC2 = "hillsboro";
        static readonly string NodeName1 = "10.146.117.95";
        static readonly string NodeName2 = "172.27.205.229";
        static readonly string ClusterName1 = "TstClusterRing1";
        static readonly string DC11 = "ashburn1";
        static readonly string DC21 = "hillsboro1";
        static readonly string NodeName11 = "11.146.117.95";
        static readonly string NodeName21 = "173.27.205.229";

        [TestMethod()]
        public void file_dsetool_ringTest()
        {
            var testLogFile = Common.Path.PathUtils.BuildFilePath(FilePath);
            var readNode = Cluster.TryGetAddNode(NodeName1, null, ClusterName);
            var cluster = Cluster.TryGetCluster(ClusterName);

            Assert.IsNotNull(readNode);
            Assert.IsNotNull(cluster);
            Assert.IsNull(readNode.DataCenter);
            Assert.IsFalse(readNode.Cluster.IsMaster);
            Assert.AreEqual(1, Cluster.GetUnAssocaitedNodes().Count());

            var parseFile = new file_dsetool_ring(DiagnosticFile.CatagoryTypes.CommandOutputFile,
                                                    testLogFile.ParentDirectoryPath,
                                                    testLogFile,
                                                    readNode, ClusterName, null, null);

            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(0, Cluster.GetUnAssocaitedNodes().Count());
            Assert.AreEqual((uint) 24, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrItemGenerated);
            Assert.AreEqual(2, cluster.DataCenters.Count());
            Assert.IsNull(parseFile.Exception);
            
            var nodes = cluster.Nodes;
            Assert.AreEqual(24, nodes.Count());

            foreach (var node in nodes)
            {
                Assert.IsNotNull(node.DataCenter);
                Assert.IsNotNull(node.Cluster);
                Assert.AreEqual(cluster, node.Cluster);
                Assert.IsFalse(node.DataCenter.Cluster.IsMaster);
               
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

            cluster = Cluster.NameClusterAssociatewItem(readNode, ClusterName);
            var readNode2 = Cluster.TryGetAddNode(NodeName2);

            Assert.IsNotNull(readNode2.DataCenter);
            Assert.AreEqual(DC2, readNode2.DataCenter.Name);
            Assert.IsFalse(readNode2.Cluster.IsMaster);
            Assert.AreEqual(0, Cluster.GetUnAssocaitedNodes().Count());

            Assert.AreEqual(cluster, Cluster.NameClusterAssociatewItem(readNode2, ClusterName));

            //Assert.AreEqual(2, Cluster.MasterCluster.DataCenters.Count());
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

        [TestMethod()]
        public void file_dsetool_ringTest_UnAssocCluster()
        {
            var testLogFile = Common.Path.PathUtils.BuildFilePath(@".\Files\DSE5-ring1");
            var readNode = Cluster.TryGetAddNode(NodeName11);
            var currentCluster = readNode.Cluster;
            var dcCnt = currentCluster.DataCenters.Count();
            var nodeCnt = currentCluster.Nodes.Count();

            Assert.IsNotNull(readNode);
            Assert.IsNull(readNode.DataCenter);            
            Assert.AreEqual(1, Cluster.GetUnAssocaitedNodes().Count());

            var parseFile = new file_dsetool_ring(DiagnosticFile.CatagoryTypes.CommandOutputFile,
                                                    testLogFile.ParentDirectoryPath,
                                                    testLogFile,
                                                    readNode, null, null, null);

            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(0, Cluster.GetUnAssocaitedNodes().Count());
            Assert.AreEqual((uint)24, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrItemGenerated);
            Assert.AreEqual(2 + dcCnt, currentCluster.DataCenters.Count());
            Assert.IsNull(parseFile.Exception);

            var nodes = currentCluster.Nodes;
            Assert.AreEqual(24 + nodeCnt, nodes.Count());
            int nbrNodes = 0;

            foreach (var node in nodes)
            {
                Assert.IsNotNull(node.DataCenter);
                Assert.IsNotNull(node.Cluster);
                Assert.AreEqual(currentCluster, node.Cluster);
                Assert.AreEqual(currentCluster, node.DataCenter.Cluster);

                if (node.Id.NodeName().StartsWith("11."))
                {
                    Assert.AreEqual(DC11, node.DataCenter.Name);
                    ++nbrNodes;
                }
                else if(node.Id.NodeName().StartsWith("173."))
                {
                    Assert.AreEqual(DC21, node.DataCenter.Name);
                    ++nbrNodes;
                }
            }

            Assert.AreEqual(24, nbrNodes);

            var dc1Instance = currentCluster.TryGetDataCenter(DC11);
            Assert.IsNotNull(dc1Instance);
            Assert.AreEqual(readNode, dc1Instance.TryGetNode(NodeName11));
            Assert.IsTrue(ReferenceEquals(readNode, Cluster.TryGetAddNode(NodeName11)));

            var cluster = Cluster.NameClusterAssociatewItem(readNode, ClusterName1);

            Assert.IsNotNull(cluster.TryGetNode(NodeName11));
            Assert.AreEqual(readNode, dc1Instance.TryGetNode(NodeName11));
            Assert.AreEqual(readNode, cluster.TryGetNode(NodeName11));
            Assert.AreEqual(cluster, readNode.Cluster);

            Assert.IsNull(cluster.TryGetNode(NodeName21));

            var readNode2 = currentCluster.TryGetNode(NodeName21);

            Assert.IsNotNull(readNode2.DataCenter);
            Assert.AreEqual(DC21, readNode2.DataCenter.Name);
            Assert.AreEqual(currentCluster, readNode2.Cluster);
            Assert.AreEqual(0, Cluster.GetUnAssocaitedNodes().Count());

            Assert.AreEqual(cluster, Cluster.NameClusterAssociatewItem(readNode2, ClusterName1));

            Assert.AreEqual(currentCluster.IsMaster ? 2 : dcCnt, currentCluster.DataCenters.Count());
            Assert.AreEqual(2, cluster.DataCenters.Count());

            if (!currentCluster.IsMaster)
            {
                nbrNodes = 0;
                foreach (var node in currentCluster.Nodes)
                {
                    Assert.IsNotNull(node.DataCenter);
                    Assert.IsNotNull(node.Cluster);

                    Assert.AreEqual(currentCluster, node.Cluster);
                    Assert.AreEqual(currentCluster, node.DataCenter.Cluster);
                    Assert.AreNotEqual(cluster, node.Cluster);
                    Assert.AreNotEqual(cluster, node.DataCenter.Cluster);

                    if (node.Id.NodeName().StartsWith("11."))
                    {
                        Assert.AreEqual(DC11, node.DataCenter.Name);
                        ++nbrNodes;
                    }
                    else if (node.Id.NodeName().StartsWith("173."))
                    {
                        Assert.AreEqual(DC21, node.DataCenter.Name);
                        ++nbrNodes;
                    }

                }
                Assert.AreEqual(0, nbrNodes);
            }

            nbrNodes = 0;
            foreach (var node in cluster.Nodes)
            {
                Assert.IsNotNull(node.DataCenter);
                Assert.IsNotNull(node.Cluster);
                Assert.IsFalse(node.Cluster.IsMaster);
                Assert.IsFalse(node.DataCenter.Cluster.IsMaster);
                Assert.AreEqual(cluster, node.Cluster);
                Assert.AreEqual(cluster, node.DataCenter.Cluster);

                if (node.Id.NodeName().StartsWith("11."))
                {
                    Assert.AreEqual(DC11, node.DataCenter.Name);
                    ++nbrNodes;
                }
                else if (node.Id.NodeName().StartsWith("173."))
                {
                    Assert.AreEqual(DC21, node.DataCenter.Name);
                    ++nbrNodes;
                }
            }
            Assert.AreEqual(24, nbrNodes);            

        }


    }

}
