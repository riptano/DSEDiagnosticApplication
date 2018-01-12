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
    public class cql_ddlTests
    {       
        static readonly string ClusterName = "TstCluster";
        static readonly string DC1 = "DC1";
        static readonly string DC2 = "DC2";
        static readonly string NodeName1 = "10.0.0.1";
        static readonly string NodeName2 = "10.0.0.2";       
        static readonly string TimeZoneName = "UTC";

        private DSEDiagnosticLibrary.Cluster _cluster;
        private DSEDiagnosticLibrary.IDataCenter _datacenter1;
        private DSEDiagnosticLibrary.IDataCenter _datacenter2;
        private DSEDiagnosticLibrary.INode _node1;
        private DSEDiagnosticLibrary.INode _node2;
        
        public void CreateClusterDCNode()
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
        }

        [TestMethod()]
        public void cql_ddlTest()
        {
            this.CreateClusterDCNode();

            var ddlPath = Common.Path.PathUtils.BuildFilePath(@".\DDL\Simple_DDL");
            var ddlParsing = new DSEDiagnosticFileParser.cql_ddl(ddlPath, null, null);

            ddlParsing.ProcessFile();

            var tstKS = this._datacenter1.TryGetKeyspace("tstKeyspace");

            Assert.IsNotNull(tstKS);
            Assert.AreEqual(tstKS, Cluster.TryGetKeySpace(tstKS.GetHashCode()));
            Assert.AreEqual(1, this._cluster.GetKeyspaces("tstKeyspace").Count());
            Assert.AreEqual(tstKS, this._cluster.GetKeyspaces("tstKeyspace").First());

            Assert.AreEqual("tstKeyspace", tstKS.Name);
            Assert.AreEqual("NetworkTopologyStrategy", tstKS.ReplicationStrategy);
            Assert.AreEqual(2, tstKS.Replications.Count());
            Assert.AreEqual((ushort) 3, tstKS.Replications.ElementAt(0).RF);
            Assert.AreEqual(this._datacenter1, tstKS.Replications.ElementAt(0).DataCenter);
            Assert.AreEqual((ushort)2, tstKS.Replications.ElementAt(1).RF);
            Assert.AreEqual(this._datacenter2, tstKS.Replications.ElementAt(1).DataCenter);
            Assert.IsFalse(tstKS.IsDSEKeyspace);
            Assert.IsFalse(tstKS.IsPerformanceKeyspace);
            Assert.IsFalse(tstKS.IsPreLoaded);
            Assert.IsFalse(tstKS.IsSystemKeyspace);
            Assert.IsFalse(tstKS.LocalStrategy);
            Assert.IsTrue(tstKS.DurableWrites);

            Assert.AreEqual(2, tstKS.DDLs.Count());

            Assert.IsInstanceOfType(tstKS.DDLs.ElementAt(0), typeof(DSEDiagnosticLibrary.CQLUserDefinedType));
            var UDTInstance = tstKS.DDLs.ElementAt(0) as DSEDiagnosticLibrary.CQLUserDefinedType;
            Assert.AreEqual(3, UDTInstance.Columns.Count());
            Assert.AreEqual("colTimestamp", UDTInstance.Columns.ElementAt(0).Name);
            Assert.AreEqual("timestamp", UDTInstance.Columns.ElementAt(0).CQLType.Name);

            var baseType = UDTInstance.Columns.ElementAt(0).CQLType.BaseType;
            Assert.IsNotNull(baseType);
            Assert.AreEqual(3, baseType.CQLSubType.Count());
            Assert.AreEqual(UDTInstance.Columns.ElementAt(0).CQLType, baseType.CQLSubType.ElementAt(0));
            Assert.AreEqual(UDTInstance.Columns.ElementAt(1).CQLType, baseType.CQLSubType.ElementAt(1));
            Assert.AreEqual(UDTInstance.Columns.ElementAt(2).CQLType, baseType.CQLSubType.ElementAt(2));

            Assert.IsFalse(UDTInstance.Columns.ElementAt(0).CQLType.IsBlob);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(0).CQLType.IsCollection);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(0).CQLType.IsCounter);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(0).CQLType.IsFrozen);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(0).CQLType.IsTuple);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(0).CQLType.IsUDT);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(0).CQLType.HasBlob);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(0).CQLType.HasCollection);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(0).CQLType.HasCounter);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(0).CQLType.HasFrozen);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(0).CQLType.HasTuple);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(0).CQLType.HasUDT);
            Assert.AreEqual(0, UDTInstance.Columns.ElementAt(0).CQLType.CQLSubType.Count());

            Assert.AreEqual("colText", UDTInstance.Columns.ElementAt(1).Name);
            Assert.AreEqual("text", UDTInstance.Columns.ElementAt(1).CQLType.Name);            
            Assert.IsFalse(UDTInstance.Columns.ElementAt(1).CQLType.IsBlob);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(1).CQLType.IsCollection);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(1).CQLType.IsCounter);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(1).CQLType.IsFrozen);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(1).CQLType.IsTuple);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(1).CQLType.IsUDT);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(1).CQLType.HasBlob);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(1).CQLType.HasCollection);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(1).CQLType.HasCounter);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(1).CQLType.HasFrozen);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(1).CQLType.HasTuple);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(1).CQLType.HasUDT);
            Assert.AreEqual(0, UDTInstance.Columns.ElementAt(1).CQLType.CQLSubType.Count());

            Assert.AreEqual("colLastFrozenListInt", UDTInstance.Columns.ElementAt(2).Name);

            //list<frozen <list<int>>>
            Assert.AreEqual("list", UDTInstance.Columns.ElementAt(2).CQLType.Name);            
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.IsBlob);
            Assert.IsTrue(UDTInstance.Columns.ElementAt(2).CQLType.IsCollection);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.IsCounter);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.IsFrozen);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.IsTuple);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.IsUDT);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.HasBlob);
            Assert.IsTrue(UDTInstance.Columns.ElementAt(2).CQLType.HasCollection);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.HasCounter);
            Assert.IsTrue(UDTInstance.Columns.ElementAt(2).CQLType.HasFrozen);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.HasTuple);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.HasUDT);
            Assert.AreEqual(1, UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.Count());

            //Frozen
            Assert.AreEqual("frozen", UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().Name);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().IsBlob);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().IsCollection);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().IsCounter);
            Assert.IsTrue(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().IsFrozen);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().IsTuple);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().IsUDT);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().HasBlob);
            Assert.IsTrue(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().HasCollection);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().HasCounter);
            Assert.IsTrue(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().HasFrozen);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().HasTuple);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().HasUDT);
            Assert.AreEqual(1, UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.Count());

            //List
            Assert.AreEqual("list", UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().Name);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().IsBlob);
            Assert.IsTrue(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().IsCollection);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().IsCounter);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().IsFrozen);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().IsTuple);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().IsUDT);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().HasBlob);
            Assert.IsTrue(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().HasCollection);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().HasCounter);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().HasFrozen);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().HasTuple);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().HasUDT);
            Assert.AreEqual(1, UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().CQLSubType.Count());

            //int
            Assert.AreEqual("int", UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().CQLSubType.First().Name);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().CQLSubType.First().IsBlob);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().CQLSubType.First().IsCollection);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().CQLSubType.First().IsCounter);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().CQLSubType.First().IsFrozen);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().CQLSubType.First().IsTuple);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().CQLSubType.First().IsUDT);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().CQLSubType.First().HasBlob);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().CQLSubType.First().HasCollection);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().CQLSubType.First().HasCounter);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().CQLSubType.First().HasFrozen);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().CQLSubType.First().HasTuple);
            Assert.IsFalse(UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().CQLSubType.First().HasUDT);
            Assert.AreEqual(0, UDTInstance.Columns.ElementAt(2).CQLType.CQLSubType.First().CQLSubType.First().CQLSubType.First().CQLSubType.Count());


            Assert.IsInstanceOfType(tstKS.DDLs.ElementAt(1), typeof(DSEDiagnosticLibrary.CQLTable));
            var cqlTbl = tstKS.DDLs.ElementAt(1) as DSEDiagnosticLibrary.CQLTable;

            Assert.AreEqual(29, cqlTbl.Columns.Count());
            Assert.AreEqual("colBigIntPK", cqlTbl.Columns.ElementAt(0).Name);
            Assert.IsTrue(cqlTbl.Columns.ElementAt(0).IsPrimaryKey);
            Assert.IsFalse(cqlTbl.Columns.ElementAt(1).IsPrimaryKey);

            Assert.AreEqual("colUDTTimestampTextList", cqlTbl.Columns.ElementAt(27).Name);
            Assert.AreEqual(13, cqlTbl.Properties.Count);
            Assert.IsInstanceOfType(cqlTbl.Properties["compaction"], typeof(Dictionary<string,object>));            
            Assert.AreEqual("org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy", ((Dictionary<string, object>)cqlTbl.Properties["compaction"])["class"]);
            Assert.AreEqual(0.1m, ((decimal)cqlTbl.Properties["dclocal_read_repair_chance"]));

        }
    }
}