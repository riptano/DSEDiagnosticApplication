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
    public class file_cassandra_log4netTests
    {
        static readonly string DDLPath = @".\DDL\describe_schema";
        static readonly string ClusterName = "TstCluster";        
        static readonly string DC1 = "DC1";
        static readonly string DC2= "DC1_SPARK";
        static readonly string NodeName1 = "10.0.0.1";
        static readonly string NodeName2 = "10.0.0.2";
        static readonly string TstKeySpaceName = "usprodsec";
        static readonly string TimeZoneName = "UTC";

        private DSEDiagnosticLibrary.Cluster _cluster;
        private DSEDiagnosticLibrary.IDataCenter _datacenter1;
        private DSEDiagnosticLibrary.IDataCenter _datacenter2;
        private DSEDiagnosticLibrary.INode _node1;
        private DSEDiagnosticLibrary.INode _node2;

        [TestMethod()]
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

            var ddlPath = Common.Path.PathUtils.BuildFilePath(DDLPath);
            var ddlParsing = new DSEDiagnosticFileParser.cql_ddl(ddlPath, null, null);

            ddlParsing.ProcessFile();

            var tstKS = this._datacenter1.TryGetKeyspace(TstKeySpaceName);

            Assert.IsNotNull(tstKS);
            Assert.AreEqual(TstKeySpaceName, tstKS.Name);

            Assert.IsNotNull(tstKS.TryGetTable("session_3"));
        }

        [TestMethod()]
        public void ProcessFileTest_CompactionFlush()
        {
            this.CreateClusterDCNodeDDL();

            Assert.AreEqual(this._cluster, DSEDiagnosticLibrary.Cluster.GetCurrentOrMaster());
            Assert.IsNotNull(this._node1);

            var testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\CompactionSingle.log");
            
            var parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, this._node1, null, null, null);

            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)4, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(4, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(this._node1, parseFile.Node);

            var result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(this._node1, result.Node);
            Assert.AreEqual(4, result.Results.Count());
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());
            
            var logeventResults = result.Results.Cast<DSEDiagnosticLibrary.LogCassandraEvent>();

            DSEDiagnosticLibrary.LogCassandraEvent beginEvent = null;

            foreach(var logEvent in logeventResults)
            {
                if(beginEvent == null)
                {
                    beginEvent = logEvent;
                    Assert.IsTrue(logEvent.Type.HasFlag(DSEDiagnosticLibrary.EventTypes.SessionBegin));
                    Assert.AreEqual(4, logEvent.SSTables.Count());
                    Assert.AreEqual(0, logEvent.ParentEvents.Count());
                }
                else
                {
                    Assert.IsNotNull(beginEvent);
                    Assert.AreEqual(beginEvent.Id, logEvent.Id);
                    Assert.AreEqual(beginEvent.EventTime, logEvent.EventTimeBegin.Value);
                    Assert.AreEqual(logEvent.EventTime - beginEvent.EventTime, logEvent.Duration.Value);
                    Assert.IsTrue(logEvent.Type.HasFlag(DSEDiagnosticLibrary.EventTypes.SessionEnd));                    
                    Assert.AreEqual(1, logEvent.ParentEvents.Count());
                    Assert.AreEqual(beginEvent.GetHashCode(), logEvent.ParentEvents.First().GetHashCode());
                    Assert.AreEqual(beginEvent.Keyspace, logEvent.Keyspace);
                    Assert.AreEqual(beginEvent.TableViewIndex, logEvent.TableViewIndex);
                    Assert.AreEqual(1, logEvent.SSTables.Count());
                    beginEvent = null;
                }

                Assert.AreEqual(DSEDiagnosticLibrary.EventClasses.Compaction | DSEDiagnosticLibrary.EventClasses.Information, logEvent.Class);
                Assert.AreEqual(TimeZoneName, logEvent.TimeZone?.Name);
                Assert.IsNotNull(logEvent.Keyspace);                
                Assert.AreEqual(this._node1, logEvent.Node);
                Assert.IsNotNull(logEvent.TableViewIndex);
                Assert.AreEqual(1, logEvent.DDLItems.Count());                
            }

            Assert.AreEqual("system.compactions_in_progress", logeventResults.ElementAt(0).TableViewIndex.FullName);
            Assert.AreEqual("system.compactions_in_progress", logeventResults.ElementAt(1).TableViewIndex.FullName);
            Assert.AreEqual("usprodsec.session_3", logeventResults.ElementAt(2).TableViewIndex.FullName);
            Assert.AreEqual("usprodsec.session_3", logeventResults.ElementAt(3).TableViewIndex.FullName);

            //Read new log file
            testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\CompactionFlush.log");

            parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, this._node1, null, null, null);

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)34, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(43, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(this._node1, parseFile.Node);

            result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(this._node1, result.Node);
            Assert.AreEqual(34, result.Results.Count());
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());

            logeventResults = result.Results.Cast<DSEDiagnosticLibrary.LogCassandraEvent>();

            var newSession = true;
            var itemNbr = 0;

            foreach (var logEvent in logeventResults)
            {
                ++itemNbr;

                if (logEvent.Type.HasFlag(DSEDiagnosticLibrary.EventTypes.SessionBegin))
                {
                    if(logEvent.ParentEvents.IsEmpty())
                    {
                        Assert.IsTrue(newSession, "Should be a new non-nested session");
                        beginEvent = logEvent;
                        newSession = false;
                    }                    
                    else
                    {
                        Assert.IsFalse(newSession, "Should be a new nested session");

                        Assert.AreEqual(beginEvent.GetHashCode(), logEvent.ParentEvents.Last().GetHashCode());                        
                        Assert.AreEqual(beginEvent.Id, logEvent.Id);

                        if ((logEvent.Class.HasFlag(EventClasses.Compaction)
                                || logEvent.Class.HasFlag(EventClasses.MemtableFlush)
                                || logEvent.Class.HasFlag(EventClasses.AntiCompaction))
                            && (beginEvent.Class.HasFlag(EventClasses.Compaction)
                                    || beginEvent.Class.HasFlag(EventClasses.MemtableFlush)
                                    || beginEvent.Class.HasFlag(EventClasses.AntiCompaction)))
                        {
                            Assert.AreEqual(beginEvent.Keyspace, logEvent.Keyspace);

                            if (!(logEvent.TableViewIndex is DSEDiagnosticLibrary.ICQLTable) && beginEvent.TableViewIndex is DSEDiagnosticLibrary.ICQLTable)
                                Assert.AreEqual(beginEvent.TableViewIndex, ((DSEDiagnosticLibrary.ICQLIndex)logEvent.TableViewIndex).Table);
                            else
                                Assert.AreEqual(beginEvent.TableViewIndex, logEvent.TableViewIndex);
                        }
                        beginEvent = logEvent;
                    }
                }
                else
                {
                    Assert.IsNotNull(beginEvent);
                    Assert.AreEqual(beginEvent.Id, logEvent.Id);
                    Assert.AreEqual(beginEvent.GetHashCode(), logEvent.ParentEvents.Last().GetHashCode());
                    Assert.AreEqual(beginEvent.Id, logEvent.Id);

                    if ((logEvent.Class.HasFlag(EventClasses.Compaction)
                            || logEvent.Class.HasFlag(EventClasses.MemtableFlush)
                            || logEvent.Class.HasFlag(EventClasses.AntiCompaction))
                        && (beginEvent.Class.HasFlag(EventClasses.Compaction)
                                    || beginEvent.Class.HasFlag(EventClasses.MemtableFlush)
                                    || beginEvent.Class.HasFlag(EventClasses.AntiCompaction)))
                    {

                        Assert.AreEqual(beginEvent.Keyspace, logEvent.Keyspace);
                        Assert.AreEqual(beginEvent.TableViewIndex, logEvent.TableViewIndex);
                    }

                    if (logEvent.Type.HasFlag(DSEDiagnosticLibrary.EventTypes.SessionEnd))
                    {
                        Assert.AreEqual(beginEvent.EventTime, logEvent.EventTimeBegin.Value);
                        
                        if (logEvent.ParentEvents.Count() <= 1)
                        {
                            newSession = true;
                            beginEvent = (DSEDiagnosticLibrary.LogCassandraEvent)logEvent.ParentEvents.FirstOrDefault()?.Value;
                        }
                        else
                        {
                            if (logEvent.Class.HasFlag(EventClasses.HintHandOff))
                            {
                                beginEvent = (DSEDiagnosticLibrary.LogCassandraEvent)logEvent.ParentEvents.Last().Value;
                            }
                            else
                            {
                                beginEvent = (DSEDiagnosticLibrary.LogCassandraEvent)logEvent.ParentEvents.TakeLast(2).First().Value;
                            }

                            if(beginEvent.ParentEvents.IsEmpty())
                            {
                                newSession = true;
                            }
                        }
                    }
                                      
                }
                
                Assert.AreEqual(TimeZoneName, logEvent.TimeZone?.Name);

                if (logEvent.Class.HasFlag(EventClasses.Compaction)
                            || logEvent.Class.HasFlag(EventClasses.MemtableFlush)
                            || logEvent.Class.HasFlag(EventClasses.AntiCompaction))
                {
                    Assert.IsNotNull(logEvent.Keyspace);
                    Assert.IsNotNull(logEvent.TableViewIndex);
                    Assert.AreEqual(1, logEvent.DDLItems.Count());
                }
                else
                {
                    Assert.AreEqual(0, logEvent.DDLItems.Count());
                }
                   
                Assert.AreEqual(this._node1, logEvent.Node);                
            }
        }

        [TestMethod()]
        public void ProcessFileTest_LogFile()
        {
            this.CreateClusterDCNodeDDL();

            Assert.AreEqual(this._cluster, DSEDiagnosticLibrary.Cluster.GetCurrentOrMaster());
            Assert.IsNotNull(this._node1);

            var testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\FullTest.log");

            var parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, this._node1, null, null, null);

            var nbrLinesParsed = parseFile.ProcessFile();

            var result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(this._node1, result.Node);
            // Assert.AreEqual(4, result.Results.Count());
            //Assert.AreEqual(0, result.OrphanedSessionEvents.Count());

            var logeventResults = result.ResultsTask.Result;


            var totalSessionEvents = logeventResults.Where(re => (re.GetValue(Common.Patterns.Collections.MemoryMapperElementCreationTypes.SearchView).Type & EventTypes.SessionElement) == EventTypes.SessionElement)
                                                    .Count();

            var totalAntiCompactionEvents = logeventResults.Where(re => (re.GetValue(Common.Patterns.Collections.MemoryMapperElementCreationTypes.SearchView).Class & EventClasses.AntiCompaction) == EventClasses.AntiCompaction)
                                                    .Count();

            var totalCompactionEvents = logeventResults.Where(re => (re.GetValue(Common.Patterns.Collections.MemoryMapperElementCreationTypes.SearchView).Class & EventClasses.Compaction) == EventClasses.Compaction)
                                                    .Count();

            var totalHintHandOddEvents = logeventResults.Where(re => (re.GetValue(Common.Patterns.Collections.MemoryMapperElementCreationTypes.SearchView).Class & EventClasses.HintHandOff) == EventClasses.HintHandOff)
                                                    .Count();

            var totalMemtableFlushEvents = logeventResults.Where(re => (re.GetValue(Common.Patterns.Collections.MemoryMapperElementCreationTypes.SearchView).Class & EventClasses.MemtableFlush) == EventClasses.MemtableFlush)
                                                    .Count();

            var totalNodeDetectionEvents = logeventResults.Where(re => (re.GetValue(Common.Patterns.Collections.MemoryMapperElementCreationTypes.SearchView).Class & EventClasses.NodeDetection) == EventClasses.NodeDetection)
                                                    .Count();

            var totalGCEvents = logeventResults.Where(re => (re.GetValue(Common.Patterns.Collections.MemoryMapperElementCreationTypes.SearchView).Class & EventClasses.GC) == EventClasses.GC)
                                                    .Count();

            var totalRepairEvents = logeventResults.Where(re => (re.GetValue(Common.Patterns.Collections.MemoryMapperElementCreationTypes.SearchView).Class & EventClasses.Repair) == EventClasses.Repair)
                                                    .Count();

            var totalConfigEvents = logeventResults.Where(re => (re.GetValue(Common.Patterns.Collections.MemoryMapperElementCreationTypes.SearchView).Class & EventClasses.Config) == EventClasses.Config)
                                                    .Count();
        }
    }
}