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
    public class file_system_hostsTests
    {
        static readonly string FilePath = @".\Files\hosts";
        static readonly string ClusterName = "TstCluster";
        static readonly string DC1 = "DC1";
        static readonly string DC2 = "DC1_SPARK";
        static readonly string NodeName1 = "10.20.70.52";
        static readonly string NodeName2 = "10.20.70.20";
        static readonly string NodeName3 = "10.0.0.1";

        private DSEDiagnosticLibrary.Cluster _cluster;
        private DSEDiagnosticLibrary.IDataCenter _datacenter1;
        private DSEDiagnosticLibrary.IDataCenter _datacenter2;
        private DSEDiagnosticLibrary.INode _node1;
        private DSEDiagnosticLibrary.INode _node2;
        private DSEDiagnosticLibrary.INode _node3;

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

            this._node2 = DSEDiagnosticLibrary.Cluster.TryGetAddNode(NodeName2, this._datacenter2);
            Assert.AreEqual(NodeName2, this._node2?.Id.NodeName());
            Assert.AreEqual(this._node2, this._datacenter2.TryGetNode(NodeName2));

            this._node3 = DSEDiagnosticLibrary.Cluster.TryGetAddNode(NodeName3, this._datacenter2);
            Assert.AreEqual(NodeName3, this._node3?.Id.NodeName());
            Assert.AreEqual(this._node3, this._datacenter2.TryGetNode(NodeName3));
        }

        [TestMethod()]
        public void file_system_hostsTest()
        {
            this.CreateClusterDCNodeDDL();

            Assert.AreEqual(this._cluster, DSEDiagnosticLibrary.Cluster.GetCurrentOrMaster());
            Assert.IsNotNull(this._node1);
            Assert.IsNotNull(this._node2);
            Assert.IsNotNull(this._node3);

            var testLogFile = Common.Path.PathUtils.BuildFilePath(FilePath);

            var parseFile = new file_system_hosts(DiagnosticFile.CatagoryTypes.SystemOutputFile,
                                                    testLogFile.ParentDirectoryPath,
                                                    testLogFile,
                                                    this._node1, null, null, null);

            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual("pconcasvp01.mcs.corp.moxiesoft.com", this._node1.Id.HostName);
            Assert.AreEqual(1, this._node1.Id.HostNames.Count());
            Assert.IsTrue(this._node1.Id.Equals("pconcasvp01.mcs.corp.moxiesoft.com"));
            Assert.IsTrue(this._node1.Id.Equals("pconcasvp01"));
            Assert.IsFalse(this._node1.Id.Equals("none"));
            Assert.AreEqual(NodeName1, this._node1.Id.NodeName());
            Assert.IsTrue(NodeIdentifier.HostNameEqual("pconcasvp01.mcs.corp.moxiesoft.com", this._node1.Id.HostName));
            Assert.IsTrue(NodeIdentifier.HostNameEqual("pconcasvp01", this._node1.Id.HostName));


            parseFile = new file_system_hosts(DiagnosticFile.CatagoryTypes.SystemOutputFile,
                                                    testLogFile.ParentDirectoryPath,
                                                    testLogFile,
                                                    this._node2, null, null, null);

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual("pmcs-con-cas04.mcs.corp.moxiesoft.com", this._node2.Id.HostName);
            Assert.AreEqual(NodeName2, this._node2.Id.NodeName());
            Assert.IsTrue(NodeIdentifier.HostNameEqual("pmcs-con-cas04.mcs.corp.moxiesoft.com", this._node2.Id.HostName));
            Assert.IsTrue(NodeIdentifier.HostNameEqual("pmcs-con-cas04", this._node2.Id.HostName));

            parseFile = new file_system_hosts(DiagnosticFile.CatagoryTypes.SystemOutputFile,
                                                   testLogFile.ParentDirectoryPath,
                                                   testLogFile,
                                                   this._node3, null, null, null);

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.IsNull(this._node3.Id.HostName);
            Assert.AreEqual(NodeName3, this._node3.Id.NodeName());            
        }


    }
}