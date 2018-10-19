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
    public class file_nodetool_tpstatsTests
    {
        static readonly string FilePath4 = @".\Files\DSE4-tpstats";
        static readonly string FilePath5 = @".\Files\DSE5-tpstats";
        static readonly string FilePath6 = @".\Files\DSE6-tpstats";
        static readonly string ClusterName = "TstClusterRing";
        static readonly string DC1 = "DC1";
        static readonly string NodeName1 = "10.146.117.95";
        static readonly string NodeName2 = "10.146.117.96";
        static readonly string NodeName3 = "10.146.117.97";

        [TestMethod()]
        public void file_nodetool_tpstatsTest()
        {            
            var readNode4 = Cluster.TryGetAddNode(NodeName1, null, ClusterName);
            var readNode5 = Cluster.TryGetAddNode(NodeName2, null, ClusterName);
            var readNode6 = Cluster.TryGetAddNode(NodeName3, null, ClusterName);

            var cluster = Cluster.TryGetCluster(ClusterName);

            var dc1 = Cluster.TryGetAddDataCenter(DC1, cluster);

            dc1.TryGetAddNode(readNode4);
            dc1.TryGetAddNode(readNode5);
            dc1.TryGetAddNode(readNode6);

            Assert.IsNotNull(cluster);
            
            Assert.AreEqual(0, Cluster.GetUnAssocaitedNodes().Count());

            Assert.IsNotNull(readNode4);
            Assert.AreEqual(dc1, readNode4.DataCenter);
            Assert.IsFalse(readNode4.Cluster.IsMaster);
            Assert.IsNotNull(readNode5);
            Assert.AreEqual(dc1, readNode5.DataCenter);
            Assert.IsFalse(readNode5.Cluster.IsMaster);
            Assert.IsNotNull(readNode6);
            Assert.AreEqual(dc1, readNode5.DataCenter);
            Assert.IsFalse(readNode6.Cluster.IsMaster);

            var testFile4 = Common.Path.PathUtils.BuildFilePath(FilePath4);
            var testFile5 = Common.Path.PathUtils.BuildFilePath(FilePath5);
            var testFile6 = Common.Path.PathUtils.BuildFilePath(FilePath6);

            var parseFile = new file_nodetool_tpstats(DiagnosticFile.CatagoryTypes.CommandOutputFile,
                                                        testFile4.ParentDirectoryPath,
                                                        testFile4,
                                                        readNode4, ClusterName, null, null);

            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(0, Cluster.GetUnAssocaitedNodes().Count());
            Assert.AreEqual((uint) 1, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrItemGenerated);
            Assert.AreEqual(35, parseFile.NbrItemsParsed);
            Assert.AreEqual(0, parseFile.NbrWarnings);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.IsNotNull(parseFile.Result);
            Assert.AreEqual(1, parseFile.Result.Results.Count());
            Assert.AreEqual(1, parseFile.Result.NbrItems);
            Assert.AreEqual(121, ((AggregatedStats)parseFile.Result.Results.First()).Items);

           parseFile = new file_nodetool_tpstats(DiagnosticFile.CatagoryTypes.CommandOutputFile,
                                                         testFile5.ParentDirectoryPath,
                                                         testFile5,
                                                         readNode5, ClusterName, null, null);

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(0, Cluster.GetUnAssocaitedNodes().Count());
            Assert.AreEqual((uint)1, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrItemGenerated);
            Assert.AreEqual(43, parseFile.NbrItemsParsed);
            Assert.AreEqual(0, parseFile.NbrWarnings);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.IsNotNull(parseFile.Result);
            Assert.AreEqual(1, parseFile.Result.Results.Count());
            Assert.AreEqual(1, parseFile.Result.NbrItems);
            Assert.AreEqual(185, ((AggregatedStats)parseFile.Result.Results.First()).Items);

            parseFile = new file_nodetool_tpstats(DiagnosticFile.CatagoryTypes.CommandOutputFile,
                                                         testFile6.ParentDirectoryPath,
                                                         testFile6,
                                                         readNode6, ClusterName, null, null);

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(0, Cluster.GetUnAssocaitedNodes().Count());
            Assert.AreEqual((uint)1, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrItemGenerated);
            Assert.AreEqual(65, parseFile.NbrItemsParsed);
            Assert.AreEqual(0, parseFile.NbrWarnings);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.IsNotNull(parseFile.Result);
            Assert.AreEqual(1, parseFile.Result.Results.Count());
            Assert.AreEqual(1, parseFile.Result.NbrItems);
            Assert.AreEqual(225, ((AggregatedStats)parseFile.Result.Results.First()).Items);
        }

        

    }

}
