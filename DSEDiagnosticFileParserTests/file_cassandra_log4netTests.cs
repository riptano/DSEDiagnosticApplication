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

        const bool LogEventsAreMemoryMapped = true;

        
        public void CreateClusterDCNodeDDL()
        {
            DSEDiagnosticLibrary.LibrarySettings.LogEventsAreMemoryMapped = LogEventsAreMemoryMapped;

            this._cluster = DSEDiagnosticLibrary.Cluster.TryGetAddCluster(ClusterName);

            Assert.AreEqual(ClusterName, this._cluster?.Name);
            Assert.AreEqual(this._cluster, DSEDiagnosticLibrary.Cluster.TryGetCluster(ClusterName));

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
            var ddlParsing = new DSEDiagnosticFileParser.cql_ddl(ddlPath, ClusterName, null);

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

            Assert.AreEqual(this._cluster.Name, ClusterName);
            Assert.IsNotNull(this._node1);

            var testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\CompactionSingle.log");
            
            var parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, this._node1, null, null, null);
            parseFile.CheckOverlappingDateRange = false;
            parseFile.DebugLogProcessing = file_cassandra_log4net.DebugLogProcessingTypes.AllMsg;

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
            parseFile.CheckOverlappingDateRange = false;
            parseFile.DebugLogProcessing = file_cassandra_log4net.DebugLogProcessingTypes.AllMsg;

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)35, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(43, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(this._node1, parseFile.Node);

            result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(this._node1, result.Node);
            Assert.AreEqual(35, result.Results.Count());
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
        public void ProcessFileTest_NodeConfig()
        {
            this.CreateClusterDCNodeDDL();

            Assert.AreEqual(this._cluster.Name, ClusterName);
            Assert.IsNotNull(this._node1);

            var testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\NodeConfig.log");

            var parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, this._node1, null, null, null);
            parseFile.CheckOverlappingDateRange = false;
            parseFile.DebugLogProcessing = file_cassandra_log4net.DebugLogProcessingTypes.AllMsg;

            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)1, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(1, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(this._node1, parseFile.Node);

            Assert.AreEqual(1, ((file_cassandra_log4net.LogResults)parseFile.Result).Results.Count());
            Assert.IsNotNull(((file_cassandra_log4net.LogResults)parseFile.Result).ResultsTask.Result.First().Value.LogProperties);
            Assert.AreEqual(155, ((file_cassandra_log4net.LogResults)parseFile.Result).ResultsTask.Result.First().Value.LogProperties.Count);

            var valueString = @"allocate_tokens_for_keyspace=null; authenticator=AllowAllAuthenticator; authorizer=AllowAllAuthorizer; auto_bootstrap=true; auto_snapshot=true; batch_size_fail_threshold_in_kb=640; batch_size_warn_threshold_in_kb=64; batchlog_replay_throttle_in_kb=1024; broadcast_address=null; broadcast_rpc_address=null; buffer_pool_use_heap_if_exhausted=true; cas_contention_timeout_in_ms=1000; client_encryption_options=<REDACTED>; cluster_name=Andersen 5.0 Test Cluster; column_index_size_in_kb=64; commit_failure_policy=stop; commitlog_compression=null; commitlog_directory=/mnt/cassandra/commitlog/commitlog; commitlog_max_compression_buffers_in_pool=3; commitlog_periodic_queue_size=-1; commitlog_segment_size_in_mb=32; commitlog_sync=periodic; commitlog_sync_batch_window_in_ms=null; commitlog_sync_period_in_ms=10000; commitlog_total_space_in_mb=null; compaction_large_partition_warning_threshold_mb=100; compaction_throughput_mb_per_sec=16; concurrent_compactors=null; concurrent_counter_writes=32; concurrent_materialized_view_writes=32; concurrent_reads=32; concurrent_replicates=null; concurrent_writes=32; counter_cache_keys_to_save=2147483647; counter_cache_save_period=7200; counter_cache_size_in_mb=null; counter_write_request_timeout_in_ms=5000; cross_node_timeout=false; data_file_directories=[Ljava.lang.String@1a45193b; disk_access_mode=auto; disk_failure_policy=stop; disk_optimization_estimate_percentile=0.95; disk_optimization_page_cross_chance=0.1; disk_optimization_strategy=ssd; dynamic_snitch=true; dynamic_snitch_badness_threshold=0.1; dynamic_snitch_reset_interval_in_ms=600000; dynamic_snitch_update_interval_in_ms=100; enable_scripted_user_defined_functions=false; enable_user_defined_functions=false; enable_user_defined_functions_threads=true; encryption_options=null; endpoint_snitch=com.datastax.bdp.snitch.DseDelegateSnitch; file_cache_size_in_mb=512; gc_warn_threshold_in_ms=1000; hinted_handoff_disabled_datacenters=; hinted_handoff_enabled=true; hinted_handoff_throttle_in_kb=1024; hints_compression=null; hints_directory=/mnt/cassandra/savedcache/hints; hints_flush_period_in_ms=10000; incremental_backups=false; index_interval=null; index_summary_capacity_in_mb=null; index_summary_resize_interval_in_minutes=60; initial_token=null; inter_dc_stream_throughput_outbound_megabits_per_sec=200; inter_dc_tcp_nodelay=false; internode_authenticator=null; internode_compression=dc; internode_recv_buff_size_in_bytes=null; internode_send_buff_size_in_bytes=null; key_cache_keys_to_save=2147483647; key_cache_save_period=14400; key_cache_size_in_mb=null; listen_address=192.168.247.69; listen_interface=null; listen_interface_prefer_ipv6=false; listen_on_broadcast_address=false; max_hint_window_in_ms=10800000; max_hints_delivery_threads=2; max_hints_file_size_in_mb=128; max_mutation_size_in_kb=null; max_streaming_retries=3; max_value_size_in_mb=256; memtable_allocation_type=heap_buffers; memtable_cleanup_threshold=null; memtable_flush_writers=null; memtable_heap_space_in_mb=null; memtable_offheap_space_in_mb=null; native_transport_max_concurrent_connections=-1; native_transport_max_concurrent_connections_per_ip=-1; native_transport_max_frame_size_in_mb=256; native_transport_max_threads=128; native_transport_port=9042; native_transport_port_ssl=null; num_tokens=128; otc_coalescing_strategy=TIMEHORIZON; otc_coalescing_window_us=200; partitioner=org.apache.cassandra.dht.Murmur3Partitioner; permissions_cache_max_entries=1000; permissions_update_interval_in_ms=-1; permissions_validity_in_ms=2000; phi_convict_threshold=8.0; range_request_timeout_in_ms=10000; read_request_timeout_in_ms=5000; request_scheduler=org.apache.cassandra.scheduler.NoScheduler; request_scheduler_id=null; request_scheduler_options=null; request_timeout_in_ms=10000; role_manager=com.datastax.bdp.cassandra.auth.DseRoleManager; roles_cache_max_entries=1000; roles_update_interval_in_ms=-1; roles_validity_in_ms=2000; row_cache_class_name=org.apache.cassandra.cache.OHCProvider; row_cache_keys_to_save=2147483647; row_cache_save_period=0; row_cache_size_in_mb=0; rpc_address=192.168.247.69; rpc_interface=null; rpc_interface_prefer_ipv6=false; rpc_keepalive=true; rpc_listen_backlog=50; rpc_max_threads=2147483647; rpc_min_threads=16; rpc_port=9160; rpc_recv_buff_size_in_bytes=null; rpc_send_buff_size_in_bytes=null; rpc_server_type=sync; saved_caches_directory=/mnt/cassandra/savedcache/saved_caches; seed_provider=org.apache.cassandra.locator.SimpleSeedProvider{seeds=192.168.247.69}; server_encryption_options=<REDACTED>; snapshot_before_compaction=false; ssl_storage_port=7001; sstable_preemptive_open_interval_in_mb=50; start_native_transport=true; start_rpc=true; storage_port=7000; stream_throughput_outbound_megabits_per_sec=200; streaming_socket_timeout_in_ms=86400000; thrift_framed_transport_size_in_mb=15; thrift_max_message_length_in_mb=16; tombstone_failure_threshold=100000; tombstone_warn_threshold=1000; tracetype_query_ttl=86400; tracetype_repair_ttl=604800; trickle_fsync=true; trickle_fsync_interval_in_kb=10240; truncate_request_timeout_in_ms=60000; unlogged_batch_across_partitions_warn_threshold=10; user_defined_function_fail_timeout=1500; user_defined_function_warn_timeout=500; user_function_timeout_policy=die; windows_timer_interval=1; write_request_timeout_in_ms=2000";
            var valuePairs = valueString.Split(';');
            var props = ((file_cassandra_log4net.LogResults)parseFile.Result).ResultsTask.Result.First().Value.LogProperties;

            foreach (var itemvalueStr in valuePairs)
            {
                var itemvalue = itemvalueStr.Split('=');
                var propValue = props[itemvalue[0].Trim()];

                if(itemvalue.Length > 2)
                {
                    itemvalue[1] = string.Join("=", itemvalue.Skip(1));
                }

                if (itemvalue[0].Trim() == "data_file_directories")
                {
                    itemvalue[1] = "[Ljava.lang.String;@1a45193b";
                }

                if (propValue is UnitOfMeasure)
                {
                    Assert.AreEqual(Decimal.Parse(itemvalue[1].Trim()), ((UnitOfMeasure)propValue).Value);
                }
                else
                {
                    Assert.AreEqual(itemvalue[1].Trim().ToLower(), propValue?.ToString().ToLower() ?? "null");
                }

            }

        }

        [TestMethod()]
        public void ProcessFileTest_LogFile()
        {
            /*
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
            var totalExceptionEvents = logeventResults.Where(re => (re.GetValue(Common.Patterns.Collections.MemoryMapperElementCreationTypes.SearchView).Class & EventClasses.Exception) == EventClasses.Exception)
                                                    .Count();
                                                    */
        }

        [TestMethod()]
        public void ProcessFileTest_Exception()
        {
            this.CreateClusterDCNodeDDL();

            Assert.AreEqual(this._cluster.Name, ClusterName);
            Assert.IsNotNull(this._node1);
            Assert.AreEqual(this._node1.Cluster, this._cluster);

            var testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\ExceptionSimple.log");

            var parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, this._node1, null, null, null);
            parseFile.CheckOverlappingDateRange = false;
            parseFile.DebugLogProcessing = file_cassandra_log4net.DebugLogProcessingTypes.AllMsg;

            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)9, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(180, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(this._node1, parseFile.Node);
            Assert.AreEqual(9, parseFile.Result.Results.Count());

            var logException = parseFile.Result.Results.ElementAt(0) as LogCassandraEvent;

            Assert.AreEqual("java.io.IOException", logException.Exception);
            Assert.AreEqual(2, logException.ExceptionPath.Count());
            Assert.AreEqual("Info(Unexpected exception during request; channel = [id: 0x40c292ba, /10.0.0.2:XXXX :> /10.0.0.1:XXXX])", logException.ExceptionPath.ElementAt(0));
            Assert.AreEqual("java.io.IOException(Error while read(...): Connection reset by peer)", logException.ExceptionPath.ElementAt(1));
            Assert.AreEqual(1, logException.AssociatedNodes.Count());
            Assert.AreEqual(this._node2, logException.AssociatedNodes.First());
            Assert.AreEqual(1, logException.LogProperties.Count);
            Assert.AreEqual(0, logException.SSTables.Count());

            logException = parseFile.Result.Results.ElementAt(1) as LogCassandraEvent;

            Assert.AreEqual("org.apache.cassandra.exceptions.ReadTimeoutException", logException.Exception);

            Assert.AreEqual(2, logException.ExceptionPath.Count());
            Assert.AreEqual("Error(Unexpected exception during request; channel = [id: 0xd30bc260, /10.0.0.2:XXXX => /10.0.0.1:XXXX])", logException.ExceptionPath.ElementAt(0));
            Assert.AreEqual("org.apache.cassandra.exceptions.ReadTimeoutException(Operation timed out - received only 0 responses.)", logException.ExceptionPath.ElementAt(1));
            Assert.AreEqual(1, logException.AssociatedNodes.Count());
            Assert.AreEqual(this._node2, logException.AssociatedNodes.First());
            Assert.AreEqual(1, logException.LogProperties.Count);
            Assert.AreEqual(0, logException.SSTables.Count());

            logException = parseFile.Result.Results.ElementAt(2) as LogCassandraEvent;

            Assert.AreEqual("org.apache.cassandra.exceptions.UnavailableException", logException.Exception);

            Assert.AreEqual(2, logException.ExceptionPath.Count());
            Assert.AreEqual("Error(Unexpected exception during request; channel = [id: 0x16977ddb, /10.0.0.2:XXXX => /10.0.0.1:XXXX])", logException.ExceptionPath.ElementAt(0));
            Assert.AreEqual("org.apache.cassandra.exceptions.UnavailableException(Cannot achieve consistency level LOCAL_ONE)", logException.ExceptionPath.ElementAt(1));            
            Assert.AreEqual(1, logException.AssociatedNodes.Count());           
            Assert.AreEqual(this._node2, logException.AssociatedNodes.First());
            Assert.AreEqual(2, logException.LogProperties.Count);
            Assert.AreEqual("LOCAL_ONE", logException.LogProperties["consistencylevel"]);
            Assert.AreEqual(0, logException.SSTables.Count());

            logException = parseFile.Result.Results.ElementAt(3) as LogCassandraEvent;

            Assert.AreEqual("java.lang.InterruptedException", logException.Exception);

            Assert.AreEqual(2, logException.ExceptionPath.Count());
            Assert.AreEqual("Error(Exception in thread Thread[RepairJobTask:4,5,RMI Runtime])", logException.ExceptionPath.ElementAt(0));
            Assert.AreEqual("java.lang.InterruptedException(Test String)", logException.ExceptionPath.ElementAt(1));
            Assert.AreEqual(0, logException.AssociatedNodes.Count());
            //Assert.AreEqual(this._node2, logException.AssociatedNodes.First());
            Assert.AreEqual(0, logException.LogProperties.Count);
            //Assert.AreEqual("LOCAL_ONE", logException.LogProperties["consistencylevel"]);
            Assert.AreEqual(0, logException.SSTables.Count());

            logException = parseFile.Result.Results.ElementAt(4) as LogCassandraEvent;

            Assert.AreEqual("java.lang.AssertionError", logException.Exception);

            Assert.AreEqual(2, logException.ExceptionPath.Count());
            Assert.AreEqual("Error(Exception in thread Thread[AntiEntropyStage:1,5,main])", logException.ExceptionPath.ElementAt(0));
            Assert.AreEqual("java.lang.AssertionError(null)", logException.ExceptionPath.ElementAt(1));
            Assert.AreEqual(0, logException.AssociatedNodes.Count());
            //Assert.AreEqual(this._node2, logException.AssociatedNodes.First());
            Assert.AreEqual(0, logException.LogProperties.Count);
            //Assert.AreEqual("LOCAL_ONE", logException.LogProperties["consistencylevel"]);
            Assert.AreEqual(0, logException.SSTables.Count());

            logException = parseFile.Result.Results.ElementAt(5) as LogCassandraEvent;

            Assert.AreEqual("java.lang.AssertionError", logException.Exception);

            Assert.AreEqual(2, logException.ExceptionPath.Count());
            Assert.AreEqual("Error(Error in ThreadPoolExecutor)", logException.ExceptionPath.ElementAt(0));
            Assert.AreEqual("java.lang.AssertionError(Data component is missing for sstable/apps/cassandra/data/data2/system/local-7ad54392bcdd35a684174e047860b377/system-local-ka-11048)", logException.ExceptionPath.ElementAt(1));
            Assert.AreEqual(0, logException.AssociatedNodes.Count());
            //Assert.AreEqual(this._node2, logException.AssociatedNodes.First());
            Assert.AreEqual(1, logException.LogProperties.Count);
            //Assert.AreEqual("LOCAL_ONE", logException.LogProperties["consistencylevel"]);
            Assert.AreEqual(1, logException.SSTables.Count());
            Assert.AreEqual("/apps/cassandra/data/data2/system/local-7ad54392bcdd35a684174e047860b377/system-local-ka-11048", logException.SSTables.ElementAt(0));

            logException = parseFile.Result.Results.ElementAt(6) as LogCassandraEvent;

            Assert.AreEqual("org.apache.cassandra.exceptions.ReadTimeoutException", logException.Exception);

            Assert.AreEqual(2, logException.ExceptionPath.Count());
            Assert.AreEqual("Warn(Error attempting to check lease for periodic update task)", logException.ExceptionPath.ElementAt(0));
            Assert.AreEqual("org.apache.cassandra.exceptions.ReadTimeoutException(Operation timed out - received only 7 responses.)", logException.ExceptionPath.ElementAt(1));
            Assert.AreEqual(0, logException.AssociatedNodes.Count());
            //Assert.AreEqual(this._node2, logException.AssociatedNodes.First());
            Assert.AreEqual(0, logException.LogProperties.Count);
            //Assert.AreEqual("LOCAL_ONE", logException.LogProperties["consistencylevel"]);
            Assert.AreEqual(0, logException.SSTables.Count());
            //Assert.AreEqual("/apps/cassandra/data/data2/system/local-7ad54392bcdd35a684174e047860b377/system-local-ka-11048", logException.SSTables.ElementAt(0));

            logException = parseFile.Result.Results.ElementAt(7) as LogCassandraEvent;

            Assert.AreEqual("java.io.IOException", logException.Exception);

            Assert.AreEqual(2, logException.ExceptionPath.Count());
            Assert.AreEqual("Error(Repair session 9fc39482-d353-11e6-9940-7d76bd094de7 for range [(-5041742244861454083,-5031228534686663405], (-5031228534686663405,-5027634690056969021], (-4998749603121144073,-4997554617625990702], (4801964342755268407,4821940070161401515], (-5484640490263850666,-5482659095377290339], (-4992832937061935230,-4983094210752029477], (6080817576420974861,6085344121835895255], (-5631687778475504058,-5628789915215383820], (-8239902477410357537,-8234601316997007237], (-4997554617625990702,-4992832937061935230]] failed with error Endpoint /10.0.0.2 died)", logException.ExceptionPath.ElementAt(0));
            Assert.AreEqual("java.io.IOException(Endpoint /10.0.0.2 died)", logException.ExceptionPath.ElementAt(1));
            Assert.AreEqual(1, logException.AssociatedNodes.Count());
            Assert.AreEqual(this._node2, logException.AssociatedNodes.First());
            Assert.AreEqual(3, logException.LogProperties.Count);
            //Assert.AreEqual("LOCAL_ONE", logException.LogProperties["consistencylevel"]);
            Assert.AreEqual(0, logException.SSTables.Count());
            //Assert.AreEqual("/apps/cassandra/data/data2/system/local-7ad54392bcdd35a684174e047860b377/system-local-ka-11048", logException.SSTables.ElementAt(0));
            Assert.AreEqual(10, logException.TokenRanges.Count());
            Assert.AreEqual("(-5041742244861454083,-5031228534686663405]", logException.TokenRanges.First().Range);
            Assert.AreEqual("9fc39482-d353-11e6-9940-7d76bd094de7", logException.Id.ToString());

            logException = parseFile.Result.Results.ElementAt(8) as LogCassandraEvent;

            Assert.AreEqual("java.io.IOException", logException.Exception);

            Assert.AreEqual(2, logException.ExceptionPath.Count());
            Assert.AreEqual("Error([repair #9fefd4a1-d353-11e6-9940-7d76bd094de7] session completed with the following error)", logException.ExceptionPath.ElementAt(0));
            Assert.AreEqual("java.io.IOException(Endpoint /10.0.0.2 died)", logException.ExceptionPath.ElementAt(1));
            Assert.AreEqual(1, logException.AssociatedNodes.Count());
            Assert.AreEqual(this._node2, logException.AssociatedNodes.First());
            Assert.AreEqual(2, logException.LogProperties.Count);
            //Assert.AreEqual("LOCAL_ONE", logException.LogProperties["consistencylevel"]);
            Assert.AreEqual(0, logException.SSTables.Count());
            //Assert.AreEqual("/apps/cassandra/data/data2/system/local-7ad54392bcdd35a684174e047860b377/system-local-ka-11048", logException.SSTables.ElementAt(0));
            Assert.AreEqual(0, logException.TokenRanges.Count());
            //Assert.AreEqual("(-5041742244861454083,-5031228534686663405]", logException.TokenRanges.First().Range);
            Assert.AreEqual("9fefd4a1-d353-11e6-9940-7d76bd094de7", logException.Id.ToString());
        }


        [TestMethod()]
        public void ProcessFileTest_SchemaChangingShardingEvts()
        {
            this.CreateClusterDCNodeDDL();

            Assert.AreEqual(this._cluster.Name, ClusterName);
            Assert.IsNotNull(this._node1);
            Assert.AreEqual(this._node1.Cluster, this._cluster);

            var testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\SchemaChangesShardingEvts.log");

            var parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, this._node1, null, null, null);
            parseFile.CheckOverlappingDateRange = false;
            parseFile.DebugLogProcessing = file_cassandra_log4net.DebugLogProcessingTypes.AllMsg;

            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)25, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(36, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(this._node1, parseFile.Node);

            Assert.AreEqual(25, ((file_cassandra_log4net.LogResults)parseFile.Result).Results.Count());
            Assert.AreEqual(0, ((file_cassandra_log4net.LogResults)parseFile.Result).OrphanedSessionEvents.Count());

            var logEvents = ((file_cassandra_log4net.LogResults)parseFile.Result).Results.Cast<LogCassandraEvent>();
            LogCassandraEvent logParentEvent = null;
            int nIdx = 0;

            Assert.AreEqual(0, logEvents.ElementAt(nIdx).ParentEvents.Count());
            Assert.AreEqual(EventClasses.NodeDetection | EventClasses.Information | EventClasses.Change, logEvents.ElementAt(nIdx).Class);
            Assert.AreEqual(SourceTypes.CassandraLog, logEvents.ElementAt(nIdx).Source);
            Assert.AreEqual(EventTypes.SessionBegin, logEvents.ElementAt(nIdx).Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.521+00"), logEvents.ElementAt(nIdx).EventTime);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeBegin.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.521+00"), logEvents.ElementAt(nIdx).EventTimeBegin);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeEnd.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.522+00"), logEvents.ElementAt(nIdx).EventTimeEnd);
            Assert.IsTrue(logEvents.ElementAt(nIdx).Duration.HasValue);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).Duration.Value.TotalMilliseconds);
            Assert.AreEqual(2, logEvents.ElementAt(nIdx).LogProperties.Count);
            Assert.AreEqual("NODE", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Key);
            Assert.AreEqual("10.0.0.2", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("action", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Key);
            Assert.AreEqual("now part of the cluster", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Value.ToString());
            Assert.AreEqual(this._node1, logEvents.ElementAt(nIdx).Node);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).AssociatedNodes.Count());
            Assert.AreEqual(this._node2, logEvents.ElementAt(nIdx).AssociatedNodes.First());
            Assert.IsNull(logEvents.ElementAt(nIdx).TableViewIndex);
            Assert.IsNull(logEvents.ElementAt(nIdx).Keyspace);
            logParentEvent = logEvents.ElementAt(nIdx);

            nIdx = 1;

            Assert.AreEqual(1, logEvents.ElementAt(nIdx).ParentEvents.Count());
            Assert.AreEqual(EventClasses.NodeDetection | EventClasses.Information| EventClasses.Change, logEvents.ElementAt(nIdx).Class);
            Assert.AreEqual(SourceTypes.CassandraLog, logEvents.ElementAt(nIdx).Source);
            Assert.AreEqual(EventTypes.SessionEnd, logEvents.ElementAt(nIdx).Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.522+00"), logEvents.ElementAt(nIdx).EventTime);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeBegin.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.521+00"), logEvents.ElementAt(nIdx).EventTimeBegin);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeEnd.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.522+00"), logEvents.ElementAt(nIdx).EventTimeEnd);
            Assert.IsTrue(logEvents.ElementAt(nIdx).Duration.HasValue);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).Duration.Value.TotalMilliseconds);
            Assert.AreEqual(2, logEvents.ElementAt(nIdx).LogProperties.Count);
            Assert.AreEqual("NODE", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Key);
            Assert.AreEqual("10.0.0.2", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("action", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Key);
            Assert.AreEqual("UP", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Value.ToString());
            Assert.AreEqual(this._node1, logEvents.ElementAt(nIdx).Node);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).AssociatedNodes.Count());
            Assert.AreEqual(this._node2, logEvents.ElementAt(nIdx).AssociatedNodes.First());
            Assert.IsNull(logEvents.ElementAt(nIdx).TableViewIndex);
            Assert.IsNull(logEvents.ElementAt(nIdx).Keyspace);

            Assert.AreEqual(logParentEvent.GetHashCode(), logEvents.ElementAt(nIdx).ParentEvents.First().GetHashCode());
            Assert.AreEqual(logParentEvent.Id, logEvents.ElementAt(nIdx).Id);
            Assert.AreEqual(logParentEvent.EventTime, logEvents.ElementAt(nIdx).EventTimeBegin);

            nIdx = 2; //Begin Session

            Assert.AreEqual(0, logEvents.ElementAt(nIdx).ParentEvents.Count());
            Assert.AreEqual(EventClasses.NodeDetection | EventClasses.Information | EventClasses.Shard, logEvents.ElementAt(nIdx).Class);
            Assert.AreEqual(SourceTypes.CassandraLog, logEvents.ElementAt(nIdx).Source);
            Assert.AreEqual(EventTypes.SessionBegin, logEvents.ElementAt(nIdx).Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.522+00"), logEvents.ElementAt(nIdx).EventTime);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeBegin.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.522+00"), logEvents.ElementAt(nIdx).EventTimeBegin);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeEnd.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.918+00"), logEvents.ElementAt(nIdx).EventTimeEnd);
            Assert.IsTrue(logEvents.ElementAt(nIdx).Duration.HasValue);
            Assert.AreEqual(396, logEvents.ElementAt(nIdx).Duration.Value.TotalMilliseconds);
            Assert.AreEqual(3, logEvents.ElementAt(nIdx).LogProperties.Count);
            Assert.AreEqual("NODE", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Key);
            Assert.AreEqual("10.0.0.2", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("action", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Key);
            Assert.AreEqual("NORMAL", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Value.ToString());
            Assert.AreEqual("token", logEvents.ElementAt(nIdx).LogProperties.ElementAt(2).Key);
            Assert.AreEqual(-1051573540106914992, logEvents.ElementAt(nIdx).LogProperties.ElementAt(2).Value);
            Assert.AreEqual(this._node1, logEvents.ElementAt(nIdx).Node);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).AssociatedNodes.Count());
            Assert.AreEqual(this._node2, logEvents.ElementAt(nIdx).AssociatedNodes.First());
            Assert.IsNull(logEvents.ElementAt(nIdx).TableViewIndex);
            Assert.IsNull(logEvents.ElementAt(nIdx).Keyspace);
            logParentEvent = logEvents.ElementAt(nIdx);

            nIdx = 3;

            Assert.AreEqual(1, logEvents.ElementAt(nIdx).ParentEvents.Count());
            Assert.AreEqual(EventClasses.NodeDetection | EventClasses.Information | EventClasses.Shard, logEvents.ElementAt(nIdx).Class);
            Assert.AreEqual(SourceTypes.CassandraLog, logEvents.ElementAt(nIdx).Source);
            Assert.AreEqual(EventTypes.SessionItem, logEvents.ElementAt(nIdx).Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.525+00"), logEvents.ElementAt(nIdx).EventTime);
            Assert.IsFalse(logEvents.ElementAt(nIdx).EventTimeBegin.HasValue);
            //Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.522+00"), logEvents.ElementAt(nIdx).EventTimeBegin);
            Assert.IsFalse(logEvents.ElementAt(nIdx).EventTimeEnd.HasValue);
            //Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.918+00"), logEvents.ElementAt(nIdx).EventTimeEnd);
            Assert.IsFalse(logEvents.ElementAt(nIdx).Duration.HasValue);
            //Assert.AreEqual(396, logEvents.ElementAt(nIdx).Duration.Value.TotalMilliseconds);
            Assert.AreEqual(2, logEvents.ElementAt(nIdx).LogProperties.Count);
            Assert.AreEqual("NODE", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Key);
            Assert.AreEqual("10.0.0.2", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Value.ToString());
            Assert.AreEqual("token", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Key);
            Assert.AreEqual(-1051573540106914992, logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Value);
            Assert.AreEqual(this._node1, logEvents.ElementAt(nIdx).Node);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).AssociatedNodes.Count());
            Assert.AreEqual(this._node2, logEvents.ElementAt(nIdx).AssociatedNodes.First());
            Assert.IsNull(logEvents.ElementAt(nIdx).TableViewIndex);
            Assert.IsNull(logEvents.ElementAt(nIdx).Keyspace);

            Assert.AreEqual(logParentEvent.GetHashCode(), logEvents.ElementAt(nIdx).ParentEvents.First().GetHashCode());
            Assert.AreEqual(logParentEvent.Id, logEvents.ElementAt(nIdx).Id);
            //Assert.AreEqual(logParentEvent.EventTime, logEvents.ElementAt(nIdx).EventTimeBegin);

            nIdx = 4;

            Assert.AreEqual(1, logEvents.ElementAt(nIdx).ParentEvents.Count());
            Assert.AreEqual(EventClasses.NodeDetection | EventClasses.Shard | EventClasses.Warning, logEvents.ElementAt(nIdx).Class);
            Assert.AreEqual(SourceTypes.CassandraLog, logEvents.ElementAt(nIdx).Source);
            Assert.AreEqual(EventTypes.SessionItem, logEvents.ElementAt(nIdx).Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.905+00"), logEvents.ElementAt(nIdx).EventTime);
            Assert.IsFalse(logEvents.ElementAt(nIdx).EventTimeBegin.HasValue);
            //Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.522+00"), logEvents.ElementAt(nIdx).EventTimeBegin);
            Assert.IsFalse(logEvents.ElementAt(nIdx).EventTimeEnd.HasValue);
            //Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.918+00"), logEvents.ElementAt(nIdx).EventTimeEnd);
            Assert.IsFalse(logEvents.ElementAt(nIdx).Duration.HasValue);
            //Assert.AreEqual(396, logEvents.ElementAt(nIdx).Duration.Value.TotalMilliseconds);
            Assert.AreEqual(2, logEvents.ElementAt(nIdx).LogProperties.Count);
            Assert.AreEqual("NODE", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Key);
            Assert.AreEqual("10.0.0.2", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Value.ToString());
            Assert.AreEqual("token", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Key);
            Assert.AreEqual(-1051573540106914992, logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Value);
            Assert.AreEqual(this._node1, logEvents.ElementAt(nIdx).Node);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).AssociatedNodes.Count());
            Assert.AreEqual(this._node2, logEvents.ElementAt(nIdx).AssociatedNodes.First());
            Assert.IsNull(logEvents.ElementAt(nIdx).TableViewIndex);
            Assert.IsNull(logEvents.ElementAt(nIdx).Keyspace);

            Assert.AreEqual(logParentEvent.GetHashCode(), logEvents.ElementAt(nIdx).ParentEvents.First().GetHashCode());
            Assert.AreEqual(logParentEvent.Id, logEvents.ElementAt(nIdx).Id);
            //Assert.AreEqual(logParentEvent.EventTime, logEvents.ElementAt(nIdx).EventTimeBegin);

            nIdx = 5;

            Assert.AreEqual(1, logEvents.ElementAt(nIdx).ParentEvents.Count());
            Assert.AreEqual(EventClasses.NodeDetection | EventClasses.Shard | EventClasses.Information, logEvents.ElementAt(nIdx).Class);
            Assert.AreEqual(SourceTypes.CassandraLog, logEvents.ElementAt(nIdx).Source);
            Assert.AreEqual(EventTypes.SessionEnd, logEvents.ElementAt(nIdx).Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.918+00"), logEvents.ElementAt(nIdx).EventTime);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeBegin.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.522+00"), logEvents.ElementAt(nIdx).EventTimeBegin);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeEnd.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.918+00"), logEvents.ElementAt(nIdx).EventTimeEnd);
            Assert.IsTrue(logEvents.ElementAt(nIdx).Duration.HasValue);
            Assert.AreEqual(396, logEvents.ElementAt(nIdx).Duration.Value.TotalMilliseconds);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).LogProperties.Count);
            Assert.AreEqual("NODE", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Key);
            Assert.AreEqual("10.0.0.2", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual(this._node1, logEvents.ElementAt(nIdx).Node);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).AssociatedNodes.Count());
            Assert.AreEqual(this._node2, logEvents.ElementAt(nIdx).AssociatedNodes.First());
            Assert.IsNull(logEvents.ElementAt(nIdx).TableViewIndex);
            Assert.IsNull(logEvents.ElementAt(nIdx).Keyspace);

            Assert.AreEqual(logParentEvent.GetHashCode(), logEvents.ElementAt(nIdx).ParentEvents.First().GetHashCode());
            Assert.AreEqual(logParentEvent.Id, logEvents.ElementAt(nIdx).Id);
            Assert.AreEqual(logParentEvent.EventTime, logEvents.ElementAt(nIdx).EventTimeBegin);

            nIdx = 6; //Begin

            Assert.AreEqual(0, logEvents.ElementAt(nIdx).ParentEvents.Count());
            Assert.AreEqual(EventClasses.NodeDetection | EventClasses.Shard | EventClasses.Information, logEvents.ElementAt(nIdx).Class);
            Assert.AreEqual(SourceTypes.CassandraLog, logEvents.ElementAt(nIdx).Source);
            Assert.AreEqual(EventTypes.SessionBegin, logEvents.ElementAt(nIdx).Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.919+00"), logEvents.ElementAt(nIdx).EventTime);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeBegin.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.919+00"), logEvents.ElementAt(nIdx).EventTimeBegin);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeEnd.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:55.704+00"), logEvents.ElementAt(nIdx).EventTimeEnd);
            Assert.IsTrue(logEvents.ElementAt(nIdx).Duration.HasValue);
            Assert.AreEqual(logEvents.ElementAt(nIdx).EventTimeEnd - logEvents.ElementAt(nIdx).EventTimeBegin, logEvents.ElementAt(nIdx).Duration.Value);
            Assert.AreEqual(3, logEvents.ElementAt(nIdx).LogProperties.Count);
            Assert.AreEqual("NODE", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Key);
            Assert.AreEqual("10.0.0.2", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("action", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Key);
            Assert.AreEqual("TEST", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Value.ToString());
            Assert.AreEqual("token", logEvents.ElementAt(nIdx).LogProperties.ElementAt(2).Key);
            Assert.AreEqual(1051573540106914992, logEvents.ElementAt(nIdx).LogProperties.ElementAt(2).Value);
            Assert.AreEqual(this._node1, logEvents.ElementAt(nIdx).Node);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).AssociatedNodes.Count());
            Assert.AreEqual(this._node2, logEvents.ElementAt(nIdx).AssociatedNodes.First());
            Assert.IsNull(logEvents.ElementAt(nIdx).TableViewIndex);
            Assert.IsNull(logEvents.ElementAt(nIdx).Keyspace);
            logParentEvent = logEvents.ElementAt(nIdx);

            //Assert.AreEqual(logParentEvent.GetHashCode(), logEvents.ElementAt(nIdx).ParentEvents.First().GetHashCode());
            //Assert.AreEqual(logParentEvent.Id, logEvents.ElementAt(nIdx).Id);
            //Assert.AreEqual(logParentEvent.EventTime, logEvents.ElementAt(nIdx).EventTimeBegin);

            nIdx = 7; 

            Assert.AreEqual(1, logEvents.ElementAt(nIdx).ParentEvents.Count());
            Assert.AreEqual(EventClasses.NodeDetection | EventClasses.Shard | EventClasses.Information, logEvents.ElementAt(nIdx).Class);
            Assert.AreEqual(SourceTypes.CassandraLog, logEvents.ElementAt(nIdx).Source);
            Assert.AreEqual(EventTypes.SessionEnd, logEvents.ElementAt(nIdx).Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:55.704+00"), logEvents.ElementAt(nIdx).EventTime);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeBegin.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:49.919+00"), logEvents.ElementAt(nIdx).EventTimeBegin);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeEnd.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:53:55.704+00"), logEvents.ElementAt(nIdx).EventTimeEnd);
            Assert.IsTrue(logEvents.ElementAt(nIdx).Duration.HasValue);
            Assert.AreEqual(logEvents.ElementAt(nIdx).EventTimeEnd - logEvents.ElementAt(nIdx).EventTimeBegin, logEvents.ElementAt(nIdx).Duration.Value);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).LogProperties.Count);
            Assert.AreEqual("NODE", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Key);
            Assert.AreEqual("10.0.0.2", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Value.ToString());
            //Assert.AreEqual("status", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Key);
            //Assert.AreEqual("TEST", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Value.ToString());
            //Assert.AreEqual("token", logEvents.ElementAt(nIdx).LogProperties.ElementAt(2).Key);
            //Assert.AreEqual(1051573540106914992, logEvents.ElementAt(nIdx).LogProperties.ElementAt(2).Value);
            Assert.AreEqual(this._node1, logEvents.ElementAt(nIdx).Node);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).AssociatedNodes.Count());
            Assert.AreEqual(this._node2, logEvents.ElementAt(nIdx).AssociatedNodes.First());
            Assert.IsNull(logEvents.ElementAt(nIdx).TableViewIndex);
            Assert.IsNull(logEvents.ElementAt(nIdx).Keyspace);
            //logParentEvent = logEvents.ElementAt(nIdx);

            Assert.AreEqual(logParentEvent.GetHashCode(), logEvents.ElementAt(nIdx).ParentEvents.First().GetHashCode());
            Assert.AreEqual(logParentEvent.Id, logEvents.ElementAt(nIdx).Id);
            Assert.AreEqual(logParentEvent.EventTime, logEvents.ElementAt(nIdx).EventTimeBegin);

            nIdx = 8; //begin

            Assert.AreEqual(0, logEvents.ElementAt(nIdx).ParentEvents.Count());
            Assert.AreEqual(EventClasses.NodeDetection | EventClasses.Shard | EventClasses.Information, logEvents.ElementAt(nIdx).Class);
            Assert.AreEqual(SourceTypes.CassandraLog, logEvents.ElementAt(nIdx).Source);
            Assert.AreEqual(EventTypes.SessionBegin, logEvents.ElementAt(nIdx).Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:56:01.123+00"), logEvents.ElementAt(nIdx).EventTime);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeBegin.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:56:01.123+00"), logEvents.ElementAt(nIdx).EventTimeBegin);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeEnd.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:56:01.704+00"), logEvents.ElementAt(nIdx).EventTimeEnd);
            Assert.IsTrue(logEvents.ElementAt(nIdx).Duration.HasValue);
            Assert.AreEqual(logEvents.ElementAt(nIdx).EventTimeEnd - logEvents.ElementAt(nIdx).EventTimeBegin, logEvents.ElementAt(nIdx).Duration.Value);
            Assert.AreEqual(3, logEvents.ElementAt(nIdx).LogProperties.Count);
            Assert.AreEqual("NODE", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Key);
            Assert.AreEqual("10.0.0.2", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("action", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Key);
            Assert.AreEqual("SCHEMA", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Value.ToString());
            Assert.AreEqual("DDLSCHEMAID", logEvents.ElementAt(nIdx).LogProperties.ElementAt(2).Key);
            Assert.AreEqual("541fdec0-47c9-33fd-b79c-4a37acd21019", logEvents.ElementAt(nIdx).LogProperties.ElementAt(2).Value);
            Assert.AreEqual(this._node1, logEvents.ElementAt(nIdx).Node);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).AssociatedNodes.Count());
            Assert.AreEqual(this._node2, logEvents.ElementAt(nIdx).AssociatedNodes.First());
            Assert.IsNotNull(logEvents.ElementAt(nIdx).TableViewIndex);
            Assert.IsNotNull(logEvents.ElementAt(nIdx).Keyspace);
            Assert.AreEqual("coafstatim.application", logEvents.ElementAt(nIdx).TableViewIndex.FullName);
            Assert.AreEqual("coafstatim", logEvents.ElementAt(nIdx).Keyspace.Name);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).DDLItems.Count());

            logParentEvent = logEvents.ElementAt(nIdx);

            //Assert.AreEqual(logParentEvent.GetHashCode(), logEvents.ElementAt(nIdx).ParentEvents.First().GetHashCode());
            //Assert.AreEqual(logParentEvent.Id, logEvents.ElementAt(nIdx).Id);
            //Assert.AreEqual(logParentEvent.EventTime, logEvents.ElementAt(nIdx).EventTimeBegin);

            nIdx = 9;

            Assert.AreEqual(1, logEvents.ElementAt(nIdx).ParentEvents.Count());
            Assert.AreEqual(EventClasses.NodeDetection | EventClasses.Shard | EventClasses.Information, logEvents.ElementAt(nIdx).Class);
            Assert.AreEqual(SourceTypes.CassandraLog, logEvents.ElementAt(nIdx).Source);
            Assert.AreEqual(EventTypes.SessionEnd, logEvents.ElementAt(nIdx).Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:56:01.704+00"), logEvents.ElementAt(nIdx).EventTime);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeBegin.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:56:01.123+00"), logEvents.ElementAt(nIdx).EventTimeBegin);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeEnd.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:56:01.704+00"), logEvents.ElementAt(nIdx).EventTimeEnd);
            Assert.IsTrue(logEvents.ElementAt(nIdx).Duration.HasValue);
            Assert.AreEqual(logEvents.ElementAt(nIdx).EventTimeEnd - logEvents.ElementAt(nIdx).EventTimeBegin, logEvents.ElementAt(nIdx).Duration.Value);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).LogProperties.Count);
            Assert.AreEqual("NODE", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Key);
            Assert.AreEqual("10.0.0.2", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Value.ToString());
            //Assert.AreEqual("status", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Key);
            //Assert.AreEqual("TEST", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Value.ToString());
            //Assert.AreEqual("token", logEvents.ElementAt(nIdx).LogProperties.ElementAt(2).Key);
            //Assert.AreEqual(1051573540106914992, logEvents.ElementAt(nIdx).LogProperties.ElementAt(2).Value);
            Assert.AreEqual(this._node1, logEvents.ElementAt(nIdx).Node);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).AssociatedNodes.Count());
            Assert.AreEqual(this._node2, logEvents.ElementAt(nIdx).AssociatedNodes.First());
            Assert.IsNull(logEvents.ElementAt(nIdx).TableViewIndex);
            Assert.IsNull(logEvents.ElementAt(nIdx).Keyspace);
            //logParentEvent = logEvents.ElementAt(nIdx);

            Assert.AreEqual(logParentEvent.GetHashCode(), logEvents.ElementAt(nIdx).ParentEvents.First().GetHashCode());
            Assert.AreEqual(logParentEvent.Id, logEvents.ElementAt(nIdx).Id);
            Assert.AreEqual(logParentEvent.EventTime, logEvents.ElementAt(nIdx).EventTimeBegin);

            nIdx = 10; //begin

            Assert.AreEqual(0, logEvents.ElementAt(nIdx).ParentEvents.Count());
            Assert.AreEqual(EventClasses.NodeDetection | EventClasses.Shard | EventClasses.Information, logEvents.ElementAt(nIdx).Class);
            Assert.AreEqual(SourceTypes.CassandraLog, logEvents.ElementAt(nIdx).Source);
            Assert.AreEqual(EventTypes.SessionBegin, logEvents.ElementAt(nIdx).Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:56:02.345+00"), logEvents.ElementAt(nIdx).EventTime);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeBegin.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:56:02.345+00"), logEvents.ElementAt(nIdx).EventTimeBegin);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeEnd.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:56:02.704+00"), logEvents.ElementAt(nIdx).EventTimeEnd);
            Assert.IsTrue(logEvents.ElementAt(nIdx).Duration.HasValue);
            Assert.AreEqual(logEvents.ElementAt(nIdx).EventTimeEnd - logEvents.ElementAt(nIdx).EventTimeBegin, logEvents.ElementAt(nIdx).Duration.Value);
            Assert.AreEqual(2, logEvents.ElementAt(nIdx).LogProperties.Count);
            Assert.AreEqual("NODE", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Key);
            Assert.AreEqual("10.0.0.2", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("action", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Key);
            Assert.AreEqual("dead", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Value.ToString());           
            Assert.AreEqual(this._node1, logEvents.ElementAt(nIdx).Node);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).AssociatedNodes.Count());
            Assert.AreEqual(this._node2, logEvents.ElementAt(nIdx).AssociatedNodes.First());
            Assert.IsNull(logEvents.ElementAt(nIdx).TableViewIndex);
            Assert.IsNull(logEvents.ElementAt(nIdx).Keyspace);            
            logParentEvent = logEvents.ElementAt(nIdx);

            //Assert.AreEqual(logParentEvent.GetHashCode(), logEvents.ElementAt(nIdx).ParentEvents.First().GetHashCode());
            //Assert.AreEqual(logParentEvent.Id, logEvents.ElementAt(nIdx).Id);
            //Assert.AreEqual(logParentEvent.EventTime, logEvents.ElementAt(nIdx).EventTimeBegin);

            nIdx = 11;

            Assert.AreEqual(1, logEvents.ElementAt(nIdx).ParentEvents.Count());
            Assert.AreEqual(EventClasses.NodeDetection | EventClasses.Shard | EventClasses.Information, logEvents.ElementAt(nIdx).Class);
            Assert.AreEqual(SourceTypes.CassandraLog, logEvents.ElementAt(nIdx).Source);
            Assert.AreEqual(EventTypes.SessionEnd, logEvents.ElementAt(nIdx).Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:56:02.704+00"), logEvents.ElementAt(nIdx).EventTime);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeBegin.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:56:02.345+00"), logEvents.ElementAt(nIdx).EventTimeBegin);
            Assert.IsTrue(logEvents.ElementAt(nIdx).EventTimeEnd.HasValue);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:56:02.704+00"), logEvents.ElementAt(nIdx).EventTimeEnd);
            Assert.IsTrue(logEvents.ElementAt(nIdx).Duration.HasValue);
            Assert.AreEqual(logEvents.ElementAt(nIdx).EventTimeEnd - logEvents.ElementAt(nIdx).EventTimeBegin, logEvents.ElementAt(nIdx).Duration.Value);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).LogProperties.Count);
            Assert.AreEqual("NODE", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Key);
            Assert.AreEqual("10.0.0.2", logEvents.ElementAt(nIdx).LogProperties.ElementAt(0).Value.ToString());
            //Assert.AreEqual("status", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Key);
            //Assert.AreEqual("TEST", logEvents.ElementAt(nIdx).LogProperties.ElementAt(1).Value.ToString());
            //Assert.AreEqual("token", logEvents.ElementAt(nIdx).LogProperties.ElementAt(2).Key);
            //Assert.AreEqual(1051573540106914992, logEvents.ElementAt(nIdx).LogProperties.ElementAt(2).Value);
            Assert.AreEqual(this._node1, logEvents.ElementAt(nIdx).Node);
            Assert.AreEqual(1, logEvents.ElementAt(nIdx).AssociatedNodes.Count());
            Assert.AreEqual(this._node2, logEvents.ElementAt(nIdx).AssociatedNodes.First());
            Assert.IsNull(logEvents.ElementAt(nIdx).TableViewIndex);
            Assert.IsNull(logEvents.ElementAt(nIdx).Keyspace);
            //logParentEvent = logEvents.ElementAt(nIdx);

            Assert.AreEqual(logParentEvent.GetHashCode(), logEvents.ElementAt(nIdx).ParentEvents.First().GetHashCode());
            Assert.AreEqual(logParentEvent.Id, logEvents.ElementAt(nIdx).Id);
            Assert.AreEqual(logParentEvent.EventTime, logEvents.ElementAt(nIdx).EventTimeBegin);

            //Single Instance
            nIdx = 12;
            var singleItem = logEvents.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(0, singleItem.ParentEvents.Count());
            Assert.AreEqual(EventClasses.Detection | EventClasses.Schema | EventClasses.Change | EventClasses.Information, singleItem.Class);
            Assert.AreEqual(SourceTypes.CassandraLog, singleItem.Source);
            Assert.AreEqual(EventTypes.SingleInstance, singleItem.Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:58:01.123+00"), singleItem.EventTime);
            Assert.IsFalse(singleItem.EventTimeBegin.HasValue);
            Assert.IsFalse(singleItem.EventTimeEnd.HasValue);
            Assert.IsFalse(singleItem.Duration.HasValue);
            Assert.AreEqual(3, singleItem.LogProperties.Count);
            Assert.AreEqual("action", singleItem.LogProperties.ElementAt(0).Key);
            Assert.AreEqual("Update", singleItem.LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("type", singleItem.LogProperties.ElementAt(1).Key);
            Assert.AreEqual("Keyspace", singleItem.LogProperties.ElementAt(1).Value.ToString());            
            Assert.AreEqual("KEYSPACE", singleItem.LogProperties.ElementAt(2).Key);
            Assert.AreEqual("OpsCenter", singleItem.LogProperties.ElementAt(2).Value.ToString());

            Assert.AreEqual(this._node1, singleItem.Node);
            Assert.AreEqual(0, singleItem.AssociatedNodes.Count());
            Assert.AreEqual(0, singleItem.DDLItems.Count());
            Assert.IsNull(singleItem.TableViewIndex);
            Assert.IsNotNull(singleItem.Keyspace);
            Assert.AreEqual("OpsCenter", singleItem.Keyspace.Name);

            nIdx = 13;
            singleItem = logEvents.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(0, singleItem.ParentEvents.Count());
            Assert.AreEqual(EventClasses.Detection | EventClasses.Change | EventClasses.Schema| EventClasses.Information, singleItem.Class);
            Assert.AreEqual(SourceTypes.CassandraLog, singleItem.Source);
            Assert.AreEqual(EventTypes.SingleInstance, singleItem.Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:58:02.456+00"), singleItem.EventTime);
            Assert.IsFalse(singleItem.EventTimeBegin.HasValue);
            Assert.IsFalse(singleItem.EventTimeEnd.HasValue);
            Assert.IsFalse(singleItem.Duration.HasValue);
            Assert.AreEqual(5, singleItem.LogProperties.Count);
            Assert.AreEqual("action", singleItem.LogProperties.ElementAt(0).Key);
            Assert.AreEqual("Create", singleItem.LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("type", singleItem.LogProperties.ElementAt(1).Key);
            Assert.AreEqual("ColumnFamily", singleItem.LogProperties.ElementAt(1).Value.ToString());            
            Assert.AreEqual("DDLSCHEMAID", singleItem.LogProperties.ElementAt(2).Key);
            Assert.AreEqual("541fdec0-47c9-33fd-b79c-4a37acd21019", singleItem.LogProperties.ElementAt(2).Value.ToString());
            Assert.AreEqual("KEYSPACE", singleItem.LogProperties.ElementAt(3).Key);
            Assert.AreEqual("coafstatim", singleItem.LogProperties.ElementAt(3).Value.ToString());
            Assert.AreEqual("DDLITEMNAME", singleItem.LogProperties.ElementAt(4).Key);
            Assert.AreEqual("application", singleItem.LogProperties.ElementAt(4).Value.ToString());

            Assert.AreEqual(this._node1, singleItem.Node);
            Assert.AreEqual(0, singleItem.AssociatedNodes.Count());
            Assert.AreEqual(1, singleItem.DDLItems.Count());
            Assert.IsNotNull(singleItem.TableViewIndex);
            Assert.IsNotNull(singleItem.Keyspace);
            Assert.AreEqual("coafstatim.application", singleItem.TableViewIndex.FullName);
            Assert.AreEqual("coafstatim", singleItem.Keyspace.Name);

            nIdx = 14;
            singleItem = logEvents.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(0, singleItem.ParentEvents.Count());
            Assert.AreEqual(EventClasses.Detection| EventClasses.Schema | EventClasses.Change | EventClasses.Information, singleItem.Class);
            Assert.AreEqual(SourceTypes.CassandraLog, singleItem.Source);
            Assert.AreEqual(EventTypes.SingleInstance, singleItem.Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:58:03.789+00"), singleItem.EventTime);
            Assert.IsFalse(singleItem.EventTimeBegin.HasValue);
            Assert.IsFalse(singleItem.EventTimeEnd.HasValue);
            Assert.IsFalse(singleItem.Duration.HasValue);
            Assert.AreEqual(3, singleItem.LogProperties.Count);
            Assert.AreEqual("action", singleItem.LogProperties.ElementAt(0).Key);
            Assert.AreEqual("Update", singleItem.LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("type", singleItem.LogProperties.ElementAt(1).Key);
            Assert.AreEqual("ColumnFamily", singleItem.LogProperties.ElementAt(1).Value.ToString());
            Assert.AreEqual("DDLITEMNAME", singleItem.LogProperties.ElementAt(2).Key);
            Assert.AreEqual("coafstatim/application", singleItem.LogProperties.ElementAt(2).Value.ToString());

            Assert.AreEqual(this._node1, singleItem.Node);
            Assert.AreEqual(0, singleItem.AssociatedNodes.Count());
            Assert.AreEqual(1, singleItem.DDLItems.Count());
            Assert.IsNotNull(singleItem.TableViewIndex);
            Assert.IsNotNull(singleItem.Keyspace);
            Assert.AreEqual("coafstatim.application", singleItem.TableViewIndex.FullName);
            Assert.AreEqual("coafstatim", singleItem.Keyspace.Name);

            nIdx = 15;
            singleItem = logEvents.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(0, singleItem.ParentEvents.Count());
            Assert.AreEqual(EventClasses.Detection| EventClasses.Change | EventClasses.Schema | EventClasses.Information, singleItem.Class);
            Assert.AreEqual(SourceTypes.CassandraLog, singleItem.Source);
            Assert.AreEqual(EventTypes.SingleInstance, singleItem.Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-07 22:58:04.012+00"), singleItem.EventTime);
            Assert.IsFalse(singleItem.EventTimeBegin.HasValue);
            Assert.IsFalse(singleItem.EventTimeEnd.HasValue);
            Assert.IsFalse(singleItem.Duration.HasValue);
            Assert.AreEqual(3, singleItem.LogProperties.Count);
            Assert.AreEqual("action", singleItem.LogProperties.ElementAt(0).Key);
            Assert.AreEqual("Drop", singleItem.LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("type", singleItem.LogProperties.ElementAt(1).Key);
            Assert.AreEqual("Keyspace", singleItem.LogProperties.ElementAt(1).Value.ToString());
            Assert.AreEqual("KEYSPACE", singleItem.LogProperties.ElementAt(2).Key);
            Assert.AreEqual("coafstatim", singleItem.LogProperties.ElementAt(2).Value.ToString());

            Assert.AreEqual(this._node1, singleItem.Node);
            Assert.AreEqual(0, singleItem.AssociatedNodes.Count());
            Assert.AreEqual(0, singleItem.DDLItems.Count());
            Assert.IsNull(singleItem.TableViewIndex);
            Assert.IsNotNull(singleItem.Keyspace);
            //Assert.AreEqual("coafstatim.application", singleItem.TableViewIndex.FullName);
            Assert.AreEqual("coafstatim", singleItem.Keyspace.Name);

            nIdx = 16;
            singleItem = logEvents.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(0, singleItem.ParentEvents.Count());
            Assert.AreEqual(EventClasses.Detection| EventClasses.Schema | EventClasses.Change | EventClasses.Information, singleItem.Class);
            Assert.AreEqual(SourceTypes.CassandraLog, singleItem.Source);
            Assert.AreEqual(EventTypes.SingleInstance, singleItem.Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-18 21:49:08.725+00"), singleItem.EventTime);
            Assert.IsFalse(singleItem.EventTimeBegin.HasValue);
            Assert.IsFalse(singleItem.EventTimeEnd.HasValue);
            Assert.IsFalse(singleItem.Duration.HasValue);
            Assert.AreEqual(3, singleItem.LogProperties.Count);
            Assert.AreEqual("action", singleItem.LogProperties.ElementAt(0).Key);
            Assert.AreEqual("Update", singleItem.LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("type", singleItem.LogProperties.ElementAt(1).Key);
            Assert.AreEqual("table", singleItem.LogProperties.ElementAt(1).Value.ToString());
            Assert.AreEqual("DDLITEMNAME", singleItem.LogProperties.ElementAt(2).Key);
            Assert.AreEqual("coafstatim/application", singleItem.LogProperties.ElementAt(2).Value.ToString());

            Assert.AreEqual(this._node1, singleItem.Node);
            Assert.AreEqual(0, singleItem.AssociatedNodes.Count());
            Assert.AreEqual(1, singleItem.DDLItems.Count());
            Assert.IsNotNull(singleItem.TableViewIndex);
            Assert.IsNotNull(singleItem.Keyspace);
            Assert.AreEqual("coafstatim.application", singleItem.TableViewIndex.FullName);
            Assert.AreEqual("coafstatim", singleItem.Keyspace.Name);

            nIdx = 17;
            singleItem = logEvents.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(0, singleItem.ParentEvents.Count());
            Assert.AreEqual(EventClasses.Detection| EventClasses.Change | EventClasses.Schema | EventClasses.Information, singleItem.Class);
            Assert.AreEqual(SourceTypes.CassandraLog, singleItem.Source);
            Assert.AreEqual(EventTypes.SingleInstance, singleItem.Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-18 21:56:17.200+00"), singleItem.EventTime);
            Assert.IsFalse(singleItem.EventTimeBegin.HasValue);
            Assert.IsFalse(singleItem.EventTimeEnd.HasValue);
            Assert.IsFalse(singleItem.Duration.HasValue);
            Assert.AreEqual(5, singleItem.LogProperties.Count);
            Assert.AreEqual("action", singleItem.LogProperties.ElementAt(0).Key);
            Assert.AreEqual("Create", singleItem.LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("type", singleItem.LogProperties.ElementAt(1).Key);
            Assert.AreEqual("table", singleItem.LogProperties.ElementAt(1).Value.ToString());
            Assert.AreEqual("DDLSCHEMAID", singleItem.LogProperties.ElementAt(2).Key);
            Assert.AreEqual("541fdec0-47c9-33fd-b79c-4a37acd21019", singleItem.LogProperties.ElementAt(2).Value.ToString());
            Assert.AreEqual("KEYSPACE", singleItem.LogProperties.ElementAt(3).Key);
            Assert.AreEqual("coafstatim", singleItem.LogProperties.ElementAt(3).Value.ToString());
            Assert.AreEqual("DDLITEMNAME", singleItem.LogProperties.ElementAt(4).Key);
            Assert.AreEqual("application", singleItem.LogProperties.ElementAt(4).Value.ToString());

            Assert.AreEqual(this._node1, singleItem.Node);
            Assert.AreEqual(0, singleItem.AssociatedNodes.Count());
            Assert.AreEqual(1, singleItem.DDLItems.Count());
            Assert.IsNotNull(singleItem.TableViewIndex);
            Assert.IsNotNull(singleItem.Keyspace);
            Assert.AreEqual("coafstatim.application", singleItem.TableViewIndex.FullName);
            Assert.AreEqual("coafstatim", singleItem.Keyspace.Name);

            nIdx = 18;
            singleItem = logEvents.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(0, singleItem.ParentEvents.Count());
            Assert.AreEqual(EventClasses.Detection| EventClasses.Schema | EventClasses.Change | EventClasses.Information, singleItem.Class);
            Assert.AreEqual(SourceTypes.CassandraLog, singleItem.Source);
            Assert.AreEqual(EventTypes.SingleInstance, singleItem.Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-18 21:55:04.410+00"), singleItem.EventTime);
            Assert.IsFalse(singleItem.EventTimeBegin.HasValue);
            Assert.IsFalse(singleItem.EventTimeEnd.HasValue);
            Assert.IsFalse(singleItem.Duration.HasValue);
            Assert.AreEqual(3, singleItem.LogProperties.Count);
            Assert.AreEqual("action", singleItem.LogProperties.ElementAt(0).Key);
            Assert.AreEqual("Drop", singleItem.LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("type", singleItem.LogProperties.ElementAt(1).Key);
            Assert.AreEqual("table", singleItem.LogProperties.ElementAt(1).Value.ToString());           
            Assert.AreEqual("DDLITEMNAME", singleItem.LogProperties.ElementAt(2).Key);
            Assert.AreEqual("coafstatim/application", singleItem.LogProperties.ElementAt(2).Value.ToString());

            Assert.AreEqual(this._node1, singleItem.Node);
            Assert.AreEqual(0, singleItem.AssociatedNodes.Count());
            Assert.AreEqual(1, singleItem.DDLItems.Count());
            Assert.IsNotNull(singleItem.TableViewIndex);
            Assert.IsNotNull(singleItem.Keyspace);
            Assert.AreEqual("coafstatim.application", singleItem.TableViewIndex.FullName);
            Assert.IsInstanceOfType(singleItem.TableViewIndex, typeof(ICQLTable));
            Assert.AreEqual("coafstatim", singleItem.Keyspace.Name);

            nIdx = 19;
            singleItem = logEvents.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(0, singleItem.ParentEvents.Count());
            Assert.AreEqual(EventClasses.Detection| EventClasses.Change | EventClasses.Schema | EventClasses.Information, singleItem.Class);
            Assert.AreEqual(SourceTypes.CassandraLog, singleItem.Source);
            Assert.AreEqual(EventTypes.SingleInstance, singleItem.Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-18 21:59:14.423+00"), singleItem.EventTime);
            Assert.IsFalse(singleItem.EventTimeBegin.HasValue);
            Assert.IsFalse(singleItem.EventTimeEnd.HasValue);
            Assert.IsFalse(singleItem.Duration.HasValue);
            Assert.AreEqual(7, singleItem.LogProperties.Count);
            Assert.AreEqual("action", singleItem.LogProperties.ElementAt(0).Key);
            Assert.AreEqual("Create", singleItem.LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("type", singleItem.LogProperties.ElementAt(1).Key);
            Assert.AreEqual("view", singleItem.LogProperties.ElementAt(1).Value.ToString());            
            Assert.AreEqual("KEYSPACE", singleItem.LogProperties.ElementAt(2).Key);
            Assert.AreEqual("TestKS", singleItem.LogProperties.ElementAt(2).Value.ToString());
            Assert.AreEqual("DDLITEMNAME", singleItem.LogProperties.ElementAt(3).Key);
            Assert.AreEqual("cyclist_by_age", singleItem.LogProperties.ElementAt(3).Value.ToString());
            Assert.AreEqual("basetaleid", singleItem.LogProperties.ElementAt(4).Key);
            Assert.AreEqual("08bdd2d2-126e-11e7-8866-d792efac3044", singleItem.LogProperties.ElementAt(4).Value.ToString());
            Assert.AreEqual("basetablename", singleItem.LogProperties.ElementAt(5).Key);
            Assert.AreEqual("cyclist_mv", singleItem.LogProperties.ElementAt(5).Value.ToString());
            Assert.AreEqual("DDLSCHEMAID", singleItem.LogProperties.ElementAt(6).Key);
            Assert.AreEqual("1d109250-fccd-11e7-83c7-9fe59a0dddc1", singleItem.LogProperties.ElementAt(6).Value.ToString());

            Assert.AreEqual(this._node1, singleItem.Node);
            Assert.AreEqual(0, singleItem.AssociatedNodes.Count());
            Assert.AreEqual(1, singleItem.DDLItems.Count());
            Assert.IsNotNull(singleItem.TableViewIndex);
            Assert.IsNotNull(singleItem.Keyspace);
            Assert.AreEqual("TestKS.cyclist_by_age", singleItem.TableViewIndex.FullName);
            Assert.IsInstanceOfType(singleItem.TableViewIndex, typeof(ICQLMaterializedView));
            Assert.IsNotNull(((ICQLMaterializedView)singleItem.TableViewIndex).Table);
            Assert.AreEqual("TestKS.cyclist_mv", ((ICQLMaterializedView)singleItem.TableViewIndex).Table.FullName);
            Assert.AreEqual("TestKS", singleItem.Keyspace.Name);

            nIdx = 20;
            singleItem = logEvents.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(0, singleItem.ParentEvents.Count());
            Assert.AreEqual(EventClasses.Detection | EventClasses.Schema | EventClasses.Change| EventClasses.Information, singleItem.Class);
            Assert.AreEqual(SourceTypes.CassandraLog, singleItem.Source);
            Assert.AreEqual(EventTypes.SingleInstance, singleItem.Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-18 21:58:33.418+00"), singleItem.EventTime);
            Assert.IsFalse(singleItem.EventTimeBegin.HasValue);
            Assert.IsFalse(singleItem.EventTimeEnd.HasValue);
            Assert.IsFalse(singleItem.Duration.HasValue);
            Assert.AreEqual(3, singleItem.LogProperties.Count);
            Assert.AreEqual("action", singleItem.LogProperties.ElementAt(0).Key);
            Assert.AreEqual("Drop", singleItem.LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("type", singleItem.LogProperties.ElementAt(1).Key);
            Assert.AreEqual("table", singleItem.LogProperties.ElementAt(1).Value.ToString());
            Assert.AreEqual("DDLITEMNAME", singleItem.LogProperties.ElementAt(2).Key);
            Assert.AreEqual("TestKS/cyclist_by_age", singleItem.LogProperties.ElementAt(2).Value.ToString());

            Assert.AreEqual(this._node1, singleItem.Node);
            Assert.AreEqual(0, singleItem.AssociatedNodes.Count());
            Assert.AreEqual(1, singleItem.DDLItems.Count());
            Assert.IsNotNull(singleItem.TableViewIndex);
            Assert.IsNotNull(singleItem.Keyspace);
            Assert.AreEqual("TestKS.cyclist_by_age", singleItem.TableViewIndex.FullName);
            Assert.IsInstanceOfType(singleItem.TableViewIndex, typeof(ICQLMaterializedView));
            Assert.AreEqual("TestKS", singleItem.Keyspace.Name);

            nIdx = 21;
            singleItem = logEvents.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(0, singleItem.ParentEvents.Count());
            Assert.AreEqual(EventClasses.Detection| EventClasses.Change | EventClasses.Schema | EventClasses.Information, singleItem.Class);
            Assert.AreEqual(SourceTypes.CassandraLog, singleItem.Source);
            Assert.AreEqual(EventTypes.SingleInstance, singleItem.Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-18 22:02:20.249+00"), singleItem.EventTime);
            Assert.IsFalse(singleItem.EventTimeBegin.HasValue);
            Assert.IsFalse(singleItem.EventTimeEnd.HasValue);
            Assert.IsFalse(singleItem.Duration.HasValue);
            Assert.AreEqual(3, singleItem.LogProperties.Count);
            Assert.AreEqual("action", singleItem.LogProperties.ElementAt(0).Key);
            Assert.AreEqual("Create", singleItem.LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("type", singleItem.LogProperties.ElementAt(1).Key);
            Assert.AreEqual("Keyspace", singleItem.LogProperties.ElementAt(1).Value.ToString());
            Assert.AreEqual("KEYSPACE", singleItem.LogProperties.ElementAt(2).Key);
            Assert.AreEqual("coafstatim", singleItem.LogProperties.ElementAt(2).Value.ToString());
            
            Assert.AreEqual(this._node1, singleItem.Node);
            Assert.AreEqual(0, singleItem.AssociatedNodes.Count());
            Assert.AreEqual(0, singleItem.DDLItems.Count());
            Assert.IsNull(singleItem.TableViewIndex);
            Assert.IsNotNull(singleItem.Keyspace);           
            Assert.AreEqual("coafstatim", singleItem.Keyspace.Name);

            nIdx = 22;
            singleItem = logEvents.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(0, singleItem.ParentEvents.Count());
            Assert.AreEqual(EventClasses.Detection | EventClasses.Schema | EventClasses.Change | EventClasses.Information, singleItem.Class);
            Assert.AreEqual(SourceTypes.CassandraLog, singleItem.Source);
            Assert.AreEqual(EventTypes.SingleInstance, singleItem.Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-18 22:01:07.689+00"), singleItem.EventTime);
            Assert.IsFalse(singleItem.EventTimeBegin.HasValue);
            Assert.IsFalse(singleItem.EventTimeEnd.HasValue);
            Assert.IsFalse(singleItem.Duration.HasValue);
            Assert.AreEqual(3, singleItem.LogProperties.Count);
            Assert.AreEqual("action", singleItem.LogProperties.ElementAt(0).Key);
            Assert.AreEqual("Drop", singleItem.LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("type", singleItem.LogProperties.ElementAt(1).Key);
            Assert.AreEqual("Keyspace", singleItem.LogProperties.ElementAt(1).Value.ToString());
            Assert.AreEqual("KEYSPACE", singleItem.LogProperties.ElementAt(2).Key);
            Assert.AreEqual("coafstatim", singleItem.LogProperties.ElementAt(2).Value.ToString());

            Assert.AreEqual(this._node1, singleItem.Node);
            Assert.AreEqual(0, singleItem.AssociatedNodes.Count());
            Assert.AreEqual(0, singleItem.DDLItems.Count());
            Assert.IsNull(singleItem.TableViewIndex);
            Assert.IsNotNull(singleItem.Keyspace);            
            Assert.AreEqual("coafstatim", singleItem.Keyspace.Name);

            nIdx = 23;
            singleItem = logEvents.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(0, singleItem.ParentEvents.Count());
            Assert.AreEqual(EventClasses.Detection | EventClasses.Schema | EventClasses.Change | EventClasses.Information, singleItem.Class);
            Assert.AreEqual(SourceTypes.CassandraLog, singleItem.Source);
            Assert.AreEqual(EventTypes.SingleInstance, singleItem.Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-18 22:01:07.689+00"), singleItem.EventTime);
            Assert.IsFalse(singleItem.EventTimeBegin.HasValue);
            Assert.IsFalse(singleItem.EventTimeEnd.HasValue);
            Assert.IsFalse(singleItem.Duration.HasValue);
            Assert.AreEqual(3, singleItem.LogProperties.Count);
            Assert.AreEqual("action", singleItem.LogProperties.ElementAt(0).Key);
            Assert.AreEqual("Drop", singleItem.LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("type", singleItem.LogProperties.ElementAt(1).Key);
            Assert.AreEqual("Keyspace", singleItem.LogProperties.ElementAt(1).Value.ToString());
            Assert.AreEqual("KEYSPACE", singleItem.LogProperties.ElementAt(2).Key);
            Assert.AreEqual("noks", singleItem.LogProperties.ElementAt(2).Value.ToString());

            Assert.AreEqual(this._node1, singleItem.Node);
            Assert.AreEqual(0, singleItem.AssociatedNodes.Count());
            Assert.AreEqual(0, singleItem.DDLItems.Count());
            Assert.IsNull(singleItem.TableViewIndex);
            Assert.IsNull(singleItem.Keyspace);
            
            nIdx = 24;
            singleItem = logEvents.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(0, singleItem.ParentEvents.Count());
            Assert.AreEqual(EventClasses.Detection | EventClasses.Schema | EventClasses.Change | EventClasses.Information, singleItem.Class);
            Assert.AreEqual(SourceTypes.CassandraLog, singleItem.Source);
            Assert.AreEqual(EventTypes.SingleInstance, singleItem.Type);
            Assert.AreEqual(DateTimeOffset.Parse("2018-01-18 22:01:07.689+00"), singleItem.EventTime);
            Assert.IsFalse(singleItem.EventTimeBegin.HasValue);
            Assert.IsFalse(singleItem.EventTimeEnd.HasValue);
            Assert.IsFalse(singleItem.Duration.HasValue);
            Assert.AreEqual(3, singleItem.LogProperties.Count);
            Assert.AreEqual("action", singleItem.LogProperties.ElementAt(0).Key);
            Assert.AreEqual("Drop", singleItem.LogProperties.ElementAt(0).Value.ToString());
            Assert.AreEqual("type", singleItem.LogProperties.ElementAt(1).Key);
            Assert.AreEqual("table", singleItem.LogProperties.ElementAt(1).Value.ToString());
            Assert.AreEqual("DDLITEMNAME", singleItem.LogProperties.ElementAt(2).Key);
            Assert.AreEqual("noks/application", singleItem.LogProperties.ElementAt(2).Value.ToString());

            Assert.AreEqual(this._node1, singleItem.Node);
            Assert.AreEqual(0, singleItem.AssociatedNodes.Count());
            Assert.AreEqual(0, singleItem.DDLItems.Count());
            Assert.IsNull(singleItem.TableViewIndex);
            Assert.IsNull(singleItem.Keyspace);            
        }

        [TestMethod()]
        public void ProcessFileTest_UnhandledLogEvts()
        {
            this.CreateClusterDCNodeDDL();

            Assert.AreEqual(this._cluster.Name, ClusterName);
            Assert.IsNotNull(this._node1);
            Assert.AreEqual(this._node1.Cluster, this._cluster);

            var testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\UnHandledLogEvents.log");

            var parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, this._node1, null, null, null);
            parseFile.CheckOverlappingDateRange = false;
            parseFile.DebugLogProcessing = file_cassandra_log4net.DebugLogProcessingTypes.AllMsg;

            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)4, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(7, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(this._node1, parseFile.Node);
           
            Assert.AreEqual(4, ((file_cassandra_log4net.LogResults)parseFile.Result).Results.Count());
            
            var logEvent = ((file_cassandra_log4net.LogResults)parseFile.Result).ResultsTask.Result.ElementAt(0).Value;
            Assert.IsNotNull(logEvent.LogProperties);
            Assert.AreEqual(1, logEvent.LogProperties.Count);
            Assert.AreEqual(DSEDiagnosticLibrary.EventClasses.NotHandled, logEvent.Class);
            Assert.AreEqual(1, logEvent.ExceptionPath.Count());
            Assert.AreEqual("Warn(Live sstable /data/cassandra/data/usprodsec/session_3-ca531e218ed211e6ab872748a53d9d02/snapshots/c42b15b1-f0df-11e6-aac6-57cd9ce61adc/usprodsec-session_3-ka-29379-Data.db from level 0 is not on corresponding level in the leveled manifest. This is not a problem per se, but may indicate an orphaned sstable due to a failed compaction not cleaned up properly.)", logEvent.ExceptionPath.First());

            logEvent = ((file_cassandra_log4net.LogResults)parseFile.Result).ResultsTask.Result.ElementAt(1).Value;
            Assert.IsNotNull(logEvent.LogProperties);
            Assert.AreEqual(0, logEvent.LogProperties.Count);
            Assert.AreEqual(DSEDiagnosticLibrary.EventClasses.NotHandled, logEvent.Class);
            Assert.AreEqual(1, logEvent.ExceptionPath.Count());
            Assert.AreEqual("Warn(Warn Message)", logEvent.ExceptionPath.First());

            logEvent = ((file_cassandra_log4net.LogResults)parseFile.Result).ResultsTask.Result.ElementAt(2).Value;
            Assert.IsNotNull(logEvent.LogProperties);
            Assert.AreEqual(1, logEvent.LogProperties.Count);
            Assert.AreEqual(DSEDiagnosticLibrary.EventClasses.NotHandled, logEvent.Class);
            Assert.AreEqual(1, logEvent.ExceptionPath.Count());
            Assert.AreEqual("Error(Error Message sstable /data/cassandra/data/usprodsec/session_3-ca531e218ed211e6ab872748a53d9d02/snapshots/c42b15b1-f0df-11e6-aac6-57cd9ce61adc/usprodsec-session_3-ka-29379-Data.db)", logEvent.ExceptionPath.First());

            logEvent = ((file_cassandra_log4net.LogResults)parseFile.Result).ResultsTask.Result.ElementAt(3).Value;
            Assert.IsNotNull(logEvent.LogProperties);
            Assert.AreEqual(0, logEvent.LogProperties.Count);
            Assert.AreEqual(DSEDiagnosticLibrary.EventClasses.NotHandled, logEvent.Class);
            Assert.AreEqual(1, logEvent.ExceptionPath.Count());
            Assert.AreEqual("Fatal(Fatal Message)", logEvent.ExceptionPath.First());

            parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, this._node1, null, null, null);
            parseFile.CheckOverlappingDateRange = false;
            parseFile.DebugLogProcessing = file_cassandra_log4net.DebugLogProcessingTypes.AllMsg;
            parseFile.DefaultLogLevelHandling = file_cassandra_log4net.DefaultLogLevelHandlers.Disabled;

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)0, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(7, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(this._node1, parseFile.Node);
        }

        [TestMethod()]
        public void ProcessFileTest_json_solr_index()
        {
            this.CreateClusterDCNodeDDL();

            Assert.AreEqual(this._cluster.Name, ClusterName);
            Assert.IsNotNull(this._node1);

            var testLogFile = Common.Path.PathUtils.BuildFilePath(@".\Json\solr_index_size.json");

            var parseFile = new json_solr_index_size(DiagnosticFile.CatagoryTypes.SystemOutputFile, testLogFile.ParentDirectoryPath, testLogFile, this._node1, null, null, null);

            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)2, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(1, parseFile.NbrWarnings);
            Assert.AreEqual(3, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.SystemOutputFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Node.Cluster);
            Assert.AreEqual(this._node1, parseFile.Node);

            Assert.AreEqual(2, ((json_solr_index_size.StatResults)parseFile.Result).Results.Count());
            Assert.AreEqual(1, ((json_solr_index_size.StatResults)parseFile.Result).UnDefinedCQLObjects.Count());
            Assert.AreEqual(((json_solr_index_size.StatResults)parseFile.Result).Results.Count(), parseFile.Node.AggregatedStats.Count());

            var undefinedCQL = ((json_solr_index_size.StatResults)parseFile.Result).UnDefinedCQLObjects;
            Assert.AreEqual("noks.notbl", undefinedCQL.ElementAt(0));

            var stats = ((json_solr_index_size.StatResults)parseFile.Result).Results;

            Assert.AreEqual(1, stats.ElementAt(0).Items);
            Assert.IsNotNull(((IAggregatedStats)stats.ElementAt(0)).TableViewIndex);
            Assert.AreEqual(Cluster.TryGetTableIndexViewbyString("coafstatim.application", parseFile.Node.Cluster), ((IAggregatedStats)stats.ElementAt(0)).TableViewIndex);
            Assert.AreEqual("solr index storage size", ((IAggregatedStats)stats.ElementAt(0)).Data.ElementAt(0).Key);
            Assert.IsInstanceOfType(((IAggregatedStats)stats.ElementAt(0)).Data.ElementAt(0).Value, typeof(UnitOfMeasure));
            Assert.AreEqual(54269877m, ((UnitOfMeasure) ((IAggregatedStats)stats.ElementAt(0)).Data.ElementAt(0).Value).Value);
            Assert.AreEqual(51.755787849m, ((UnitOfMeasure)((IAggregatedStats)stats.ElementAt(0)).Data.ElementAt(0).Value).ConvertTo(UnitOfMeasure.Types.MiB | UnitOfMeasure.Types.Storage));

            Assert.AreEqual(1, stats.ElementAt(1).Items);
            Assert.IsNotNull(((IAggregatedStats)stats.ElementAt(1)).TableViewIndex);
            Assert.AreEqual(Cluster.TryGetTableIndexViewbyString("usprodcfg.nodehealth_3_0_0", parseFile.Node.Cluster), ((IAggregatedStats)stats.ElementAt(1)).TableViewIndex);
            Assert.AreEqual("solr index storage size", ((IAggregatedStats)stats.ElementAt(1)).Data.ElementAt(0).Key);
            Assert.IsInstanceOfType(((IAggregatedStats)stats.ElementAt(1)).Data.ElementAt(0).Value, typeof(UnitOfMeasure));
            Assert.AreEqual(515723686841m, ((UnitOfMeasure)((IAggregatedStats)stats.ElementAt(1)).Data.ElementAt(0).Value).Value);
            Assert.AreEqual(491832.434502602m, ((UnitOfMeasure)((IAggregatedStats)stats.ElementAt(1)).Data.ElementAt(0).Value).ConvertTo(UnitOfMeasure.Types.MiB | UnitOfMeasure.Types.Storage));
        }


        [TestMethod()]
        public void ProcessFileTest_GC()
        {
            this.CreateClusterDCNodeDDL();

            Assert.AreEqual(this._cluster, DSEDiagnosticLibrary.Cluster.TryGetCluster(ClusterName));
            Assert.IsNotNull(this._node1);

            var testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\GC.log");

            var parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, this._node1, null, null, null);
            parseFile.CheckOverlappingDateRange = false;
            parseFile.DebugLogProcessing = file_cassandra_log4net.DebugLogProcessingTypes.AllMsg;

            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)3, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(3, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(this._node1, parseFile.Node);

            var result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(this._node1, result.Node);
            Assert.AreEqual(3, result.Results.Count());
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());

            var nIdx = 0;
            var logEvent = result.Results.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(EventClasses.GC | EventClasses.Information, logEvent.Class);
            Assert.AreEqual(DateTime.Parse("2017-01-12 19:11:57.055"), logEvent.EventTimeLocal);
            Assert.AreEqual(DateTimeOffset.Parse("2017-01-12 19:11:57.055 +00"), logEvent.EventTime);
            Assert.AreEqual(logEvent.EventTime, logEvent.EventTimeEnd);
            Assert.AreEqual(new TimeSpan(0, 0, 0, 0, 215), logEvent.Duration);
            Assert.AreEqual(logEvent.EventTime - new TimeSpan(0, 0, 0, 0, 215), logEvent.EventTimeBegin);
            Assert.AreEqual("ParNew", this._node1.Machine.Java.GCType);
            Assert.AreEqual(5, logEvent.LogProperties.Count());
            Assert.AreEqual("ParNew", logEvent.LogProperties["GC"]);

            nIdx = 1;
            logEvent = result.Results.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(EventClasses.GC | EventClasses.Information, logEvent.Class);
            Assert.AreEqual(DateTime.Parse("2017-01-16 13:51:39.295"), logEvent.EventTimeLocal);
            Assert.AreEqual(DateTimeOffset.Parse("2017-01-16 13:51:39.295 +00"), logEvent.EventTime);
            Assert.AreEqual(logEvent.EventTime, logEvent.EventTimeEnd);
            Assert.AreEqual(new TimeSpan(0, 0, 0, 0, 491), logEvent.Duration);
            Assert.AreEqual(logEvent.EventTime - new TimeSpan(0, 0, 0, 0, 491), logEvent.EventTimeBegin);
            Assert.AreEqual("ParNew", this._node1.Machine.Java.GCType);
            Assert.AreEqual(6, logEvent.LogProperties.Count());
            Assert.AreEqual("CMS", logEvent.LogProperties["GC"]);

            nIdx = 2;
            logEvent = result.Results.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(EventClasses.GC | EventClasses.Information, logEvent.Class);
            Assert.AreEqual(DateTime.Parse("2017-05-01 01:34:24.475"), logEvent.EventTimeLocal);
            Assert.AreEqual(DateTimeOffset.Parse("2017-05-01 01:34:24.475 +00"), logEvent.EventTime);
            Assert.AreEqual(logEvent.EventTime, logEvent.EventTimeEnd);
            Assert.AreEqual(new TimeSpan(0, 0, 0, 0, 265), logEvent.Duration);
            Assert.AreEqual(logEvent.EventTime - new TimeSpan(0, 0, 0, 0, 265), logEvent.EventTimeBegin);
            Assert.AreEqual("ParNew", this._node1.Machine.Java.GCType);
            Assert.AreEqual(6, logEvent.LogProperties.Count());
            Assert.AreEqual("G1", logEvent.LogProperties["GC"]);
        }

        [TestMethod()]
        public void ProcessFileTest_PoolStatus()
        {
            this.CreateClusterDCNodeDDL();

            Assert.AreEqual(this._cluster, DSEDiagnosticLibrary.Cluster.TryGetCluster(ClusterName));
            Assert.IsNotNull(this._node1);

            var testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\PoolGC.log");

            var parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, this._node1, null, null, null);
            parseFile.CheckOverlappingDateRange = false;
            parseFile.DebugLogProcessing = file_cassandra_log4net.DebugLogProcessingTypes.AllMsg;

            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)31, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(34, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(this._node1, parseFile.Node);

            var result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(this._node1, result.Node);
            Assert.AreEqual(31, result.Results.Count());
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());

            var nIdx = 0;
            var logEvent = result.Results.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(EventClasses.Pools | EventClasses.Stats | EventClasses.Information, logEvent.Class);
            Assert.AreEqual(DateTime.Parse("2017-01-16 13:51:39.296"), logEvent.EventTimeLocal);
            Assert.AreEqual(DateTimeOffset.Parse("2017-01-16 13:51:39.296 +00"), logEvent.EventTime);
            Assert.IsFalse(logEvent.EventTimeEnd.HasValue);
            Assert.IsFalse(logEvent.Duration.HasValue);
            Assert.IsFalse(logEvent.EventTimeBegin.HasValue);           
            Assert.AreEqual(6, logEvent.LogProperties.Count());
            Assert.AreEqual("MutationStage", logEvent.LogProperties["poolname"]);
            Assert.AreEqual(0L, logEvent.LogProperties["nbractive"]);
            Assert.AreEqual(0L, logEvent.LogProperties["nbrpending"]);
            Assert.AreEqual(144664687L, logEvent.LogProperties["nbrcompleted"]);
            Assert.AreEqual(0L, logEvent.LogProperties["nbrblocked"]);
            Assert.AreEqual(0L, logEvent.LogProperties["nbralltimeblocked"]);
            Assert.IsNull(logEvent.Keyspace);
            Assert.IsNull(logEvent.TableViewIndex);

            nIdx = 4;
            logEvent = result.Results.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(EventClasses.Pools | EventClasses.Stats | EventClasses.Information, logEvent.Class);
            Assert.AreEqual(DateTime.Parse("2017-01-16 13:51:39.298"), logEvent.EventTimeLocal);
            Assert.AreEqual(DateTimeOffset.Parse("2017-01-16 13:51:39.298 +00"), logEvent.EventTime);
            Assert.IsFalse(logEvent.EventTimeEnd.HasValue);
            Assert.IsFalse(logEvent.Duration.HasValue);
            Assert.IsFalse(logEvent.EventTimeBegin.HasValue);
            Assert.AreEqual(6, logEvent.LogProperties.Count());
            Assert.AreEqual("CounterMutationStage", logEvent.LogProperties["poolname"]);
            Assert.AreEqual(1L, logEvent.LogProperties["nbractive"]);
            Assert.AreEqual(2L, logEvent.LogProperties["nbrpending"]);
            Assert.AreEqual(3L, logEvent.LogProperties["nbrcompleted"]);
            Assert.AreEqual(4L, logEvent.LogProperties["nbrblocked"]);
            Assert.AreEqual(5L, logEvent.LogProperties["nbralltimeblocked"]);
            Assert.IsNull(logEvent.Keyspace);
            Assert.IsNull(logEvent.TableViewIndex);

            nIdx = 23;
            logEvent = result.Results.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(EventClasses.Caches | EventClasses.Stats | EventClasses.Information, logEvent.Class);
            Assert.AreEqual(DateTime.Parse("2017-01-16 13:51:39.303"), logEvent.EventTimeLocal);
            Assert.AreEqual(DateTimeOffset.Parse("2017-01-16 13:51:39.303 +00"), logEvent.EventTime);
            Assert.IsFalse(logEvent.EventTimeEnd.HasValue);
            Assert.IsFalse(logEvent.Duration.HasValue);
            Assert.IsFalse(logEvent.EventTimeBegin.HasValue);
            Assert.AreEqual(4, logEvent.LogProperties.Count());
            Assert.AreEqual("KeyCache", logEvent.LogProperties["cachetype"]);
            Assert.AreEqual(new UnitOfMeasure(104720385, UnitOfMeasure.Types.Byte), logEvent.LogProperties["size"]);
            Assert.AreEqual(new UnitOfMeasure(104857600, UnitOfMeasure.Types.Byte), logEvent.LogProperties["capacity"]);
            Assert.AreEqual("all", logEvent.LogProperties["keystosave"]);            
            Assert.IsNull(logEvent.Keyspace);
            Assert.IsNull(logEvent.TableViewIndex);

            nIdx = 25;
            logEvent = result.Results.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(EventClasses.MemtableFlush | EventClasses.Stats | EventClasses.Information, logEvent.Class);
            Assert.AreEqual(DateTime.Parse("2017-01-16 13:51:39.303"), logEvent.EventTimeLocal);
            Assert.AreEqual(DateTimeOffset.Parse("2017-01-16 13:51:39.303 +00"), logEvent.EventTime);
            Assert.IsFalse(logEvent.EventTimeEnd.HasValue);
            Assert.IsFalse(logEvent.Duration.HasValue);
            Assert.IsFalse(logEvent.EventTimeBegin.HasValue);
            Assert.AreEqual(3, logEvent.LogProperties.Count());
            Assert.AreEqual("push.push_device", logEvent.LogProperties["DDLITEMNAME"]);
            Assert.AreEqual(new UnitOfMeasure(18369, UnitOfMeasure.Types.Operations | UnitOfMeasure.Types.Rate | UnitOfMeasure.Types.SEC), logEvent.LogProperties["memtable"]);
            Assert.AreEqual(new UnitOfMeasure(3612697, UnitOfMeasure.Types.Byte), logEvent.LogProperties["datasize"]);            
            Assert.IsNotNull(logEvent.Keyspace);
            Assert.IsNotNull(logEvent.TableViewIndex);
            Assert.AreEqual("push", logEvent.Keyspace.Name);
            Assert.AreEqual("push_device", logEvent.TableViewIndex.Name);
        }

        [TestMethod()]
        public void ProcessFileTest_SolrHardCommit()
        {
            this.CreateClusterDCNodeDDL();

            Assert.AreEqual(this._cluster, DSEDiagnosticLibrary.Cluster.TryGetCluster(ClusterName));
            Assert.IsNotNull(this._node1);

            var testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\SolrHardCommit.log");

            var parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, this._node1, null, null, null);
            parseFile.CheckOverlappingDateRange = false;
            parseFile.DebugLogProcessing = file_cassandra_log4net.DebugLogProcessingTypes.AllMsg;

            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)9, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(9, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(this._node1, parseFile.Node);

            var result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(this._node1, result.Node);
            Assert.AreEqual(9, result.Results.Count());
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());

            var nIdx = 0;
            var logEvent = result.Results.ElementAt(nIdx) as LogCassandraEvent;
            LogCassandraEvent parentSession;
            LogCassandraEvent currentParrent;

            Assert.AreEqual(EventClasses.SolrHardCommit | EventClasses.Information, logEvent.Class);
            Assert.AreEqual(EventTypes.SessionBegin, logEvent.Type);
            Assert.IsNotNull(logEvent.Id);           
            Assert.AreEqual(DateTime.Parse("2017-05-12 23:20:01.570"), logEvent.EventTimeLocal);
            Assert.AreEqual(DateTimeOffset.Parse("2017-05-12 23:20:01.570 +00"), logEvent.EventTime);
            Assert.IsTrue(logEvent.EventTimeEnd.HasValue);
            Assert.IsTrue(logEvent.Duration.HasValue);
            Assert.IsTrue(logEvent.EventTimeBegin.HasValue);
            Assert.AreEqual(new TimeSpan(0, 0, 0, 55, 193), logEvent.Duration);
            Assert.AreEqual(logEvent.EventTime, logEvent.EventTimeBegin);
            Assert.AreEqual(logEvent.EventTime + logEvent.Duration.Value, logEvent.EventTimeEnd);

            Assert.AreEqual(0, logEvent.ParentEvents.Count());
            Assert.AreEqual(3, logEvent.LogProperties.Count());           
            Assert.IsNotNull(logEvent.Keyspace);
            Assert.IsNotNull(logEvent.TableViewIndex);
            Assert.AreEqual("coafstatim", logEvent.Keyspace.Name);
            Assert.AreEqual("coafstatim.application", logEvent.TableViewIndex.FullName);
            Assert.AreEqual(21, logEvent.DDLItems.Count());
            parentSession = logEvent;
            currentParrent = logEvent;

            nIdx = 1;
            logEvent = result.Results.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(EventClasses.SolrHardCommit | EventClasses.Information, logEvent.Class);
            Assert.AreEqual(EventTypes.SessionItem, logEvent.Type);
            Assert.IsNotNull(logEvent.Id);
            Assert.AreEqual(parentSession.Id, logEvent.Id);
            Assert.AreEqual(DateTime.Parse("2017-05-12 23:20:23.571"), logEvent.EventTimeLocal);
            Assert.AreEqual(DateTimeOffset.Parse("2017-05-12 23:20:23.571 +00"), logEvent.EventTime);
            Assert.IsFalse(logEvent.EventTimeEnd.HasValue);
            Assert.IsFalse(logEvent.Duration.HasValue);
            Assert.IsFalse(logEvent.EventTimeBegin.HasValue);
            //Assert.AreEqual(new TimeSpan(0, 0, 0, 55, 193), logEvent.Duration);
            //Assert.AreEqual(logEvent.EventTime, logEvent.EventTimeBegin);
            //Assert.AreEqual(logEvent.EventTime + logEvent.Duration.Value, logEvent.EventTimeEnd);

            Assert.AreEqual(1, logEvent.ParentEvents.Count());
            Assert.AreEqual(1, logEvent.LogProperties.Count());
            Assert.IsNotNull(logEvent.Keyspace);
            Assert.IsNotNull(logEvent.TableViewIndex);
            Assert.AreEqual("coafstatim", logEvent.Keyspace.Name);
            Assert.AreEqual("coafstatim.application", logEvent.TableViewIndex.FullName);
            Assert.AreEqual(1, logEvent.DDLItems.Count());

            nIdx = 2;
            logEvent = result.Results.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(EventClasses.SolrHardCommit | EventClasses.Information, logEvent.Class);
            Assert.AreEqual(EventTypes.SessionBegin, logEvent.Type);
            Assert.IsNotNull(logEvent.Id);
            Assert.AreEqual(parentSession.Id, logEvent.Id);
            Assert.AreEqual(DateTime.Parse("2017-05-12 23:20:34.571"), logEvent.EventTimeLocal);
            Assert.AreEqual(DateTimeOffset.Parse("2017-05-12 23:20:34.571 +00"), logEvent.EventTime);
            Assert.IsTrue(logEvent.EventTimeEnd.HasValue);            
            Assert.IsTrue(logEvent.Duration.HasValue);
            Assert.IsTrue(logEvent.EventTimeBegin.HasValue);
            Assert.AreEqual(new TimeSpan(0, 0, 0, 22, 192), logEvent.Duration);
            Assert.AreEqual(logEvent.EventTime, logEvent.EventTimeBegin);
            Assert.AreEqual(logEvent.EventTime + logEvent.Duration.Value, logEvent.EventTimeEnd);

            Assert.AreEqual(1, logEvent.ParentEvents.Count());
            Assert.AreEqual(1, logEvent.LogProperties.Count());
            Assert.IsNull(logEvent.Keyspace);
            Assert.IsNull(logEvent.TableViewIndex);
            //Assert.AreEqual("coafstatim", logEvent.Keyspace.Name);
            //Assert.AreEqual("coafstatim.application", logEvent.TableViewIndex.FullName);
            Assert.AreEqual(0, logEvent.DDLItems.Count());
            currentParrent = logEvent;

            nIdx = 3;
            logEvent = result.Results.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(EventClasses.SolrHardCommit | EventClasses.Information, logEvent.Class);
            Assert.AreEqual(EventTypes.SessionEnd, logEvent.Type);
            Assert.IsNotNull(logEvent.Id);
            Assert.AreEqual(parentSession.Id, logEvent.Id);
            Assert.AreEqual(DateTime.Parse("2017-05-12 23:20:56.763"), logEvent.EventTimeLocal);
            Assert.AreEqual(DateTimeOffset.Parse("2017-05-12 23:20:56.763 +00"), logEvent.EventTime);
            Assert.IsTrue(logEvent.EventTimeEnd.HasValue);
            Assert.IsTrue(logEvent.Duration.HasValue);
            Assert.IsTrue(logEvent.EventTimeBegin.HasValue);
            Assert.AreEqual(new TimeSpan(0, 0, 0, 22, 192), logEvent.Duration);

            Assert.AreEqual(currentParrent.EventTime, logEvent.EventTimeBegin);
            Assert.AreEqual(logEvent.EventTime - logEvent.Duration.Value, logEvent.EventTimeBegin);
            Assert.AreEqual(logEvent.EventTime, logEvent.EventTimeEnd);

            Assert.AreEqual(2, logEvent.ParentEvents.Count());
            Assert.AreEqual(currentParrent.GetHashCode(), logEvent.ParentEvents.Last().GetHashCode());
            Assert.AreEqual(parentSession.Id, logEvent.ParentEvents.First().GetValue().Id);
            Assert.AreEqual(currentParrent.EventTimeEnd.Value, logEvent.EventTime);
            Assert.AreEqual(parentSession.EventTimeEnd.Value, logEvent.EventTime);
            Assert.AreEqual(2, logEvent.LogProperties.Count());
            Assert.IsNull(logEvent.Keyspace);
            Assert.IsNull(logEvent.TableViewIndex);
            //Assert.AreEqual("coafstatim", logEvent.Keyspace.Name);
            //Assert.AreEqual("coafstatim.application", logEvent.TableViewIndex.FullName);
            Assert.AreEqual(0, logEvent.DDLItems.Count());


            nIdx = 4;
            logEvent = result.Results.ElementAt(nIdx) as LogCassandraEvent;
           
            Assert.AreEqual(EventClasses.SolrHardCommit | EventClasses.Information, logEvent.Class);
            Assert.AreEqual(EventTypes.SessionBegin, logEvent.Type);
            Assert.IsNotNull(logEvent.Id);
            Assert.AreEqual(DateTime.Parse("2017-09-23 16:32:01.763"), logEvent.EventTimeLocal);
            Assert.AreEqual(DateTimeOffset.Parse("2017-09-23 16:32:01.763 +00"), logEvent.EventTime);
            Assert.IsTrue(logEvent.EventTimeEnd.HasValue);
            Assert.IsTrue(logEvent.Duration.HasValue);
            Assert.IsTrue(logEvent.EventTimeBegin.HasValue);
            Assert.AreEqual(new TimeSpan(0, 0, 0, 55, 036), logEvent.Duration);
            Assert.AreEqual(logEvent.EventTime, logEvent.EventTimeBegin);
            Assert.AreEqual(logEvent.EventTime + logEvent.Duration.Value, logEvent.EventTimeEnd);

            Assert.AreEqual(0, logEvent.ParentEvents.Count());
            Assert.AreEqual(2, logEvent.LogProperties.Count());
            Assert.IsNotNull(logEvent.Keyspace);
            Assert.IsNotNull(logEvent.TableViewIndex);
            Assert.AreEqual("coafstatim", logEvent.Keyspace.Name);
            Assert.AreEqual("coafstatim.application.coafstatim_application_appcretddt_index", logEvent.TableViewIndex.FullName);
            Assert.AreEqual(2, logEvent.DDLItems.Count());
            parentSession = logEvent;
            currentParrent = logEvent;

            nIdx = 5;
            logEvent = result.Results.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(EventClasses.SolrHardCommit | EventClasses.Information, logEvent.Class);
            Assert.AreEqual(EventTypes.SessionItem, logEvent.Type);
            Assert.IsNotNull(logEvent.Id);
            Assert.AreEqual(parentSession.Id, logEvent.Id);
            Assert.AreEqual(DateTime.Parse("2017-09-23 16:32:23.775"), logEvent.EventTimeLocal);
            Assert.AreEqual(DateTimeOffset.Parse("2017-09-23 16:32:23.775 +00"), logEvent.EventTime);
            Assert.IsFalse(logEvent.EventTimeEnd.HasValue);
            Assert.IsFalse(logEvent.Duration.HasValue);
            Assert.IsFalse(logEvent.EventTimeBegin.HasValue);
            //Assert.AreEqual(new TimeSpan(0, 0, 0, 55, 193), logEvent.Duration);
            //Assert.AreEqual(logEvent.EventTime, logEvent.EventTimeBegin);
            //Assert.AreEqual(logEvent.EventTime + logEvent.Duration.Value, logEvent.EventTimeEnd);

            Assert.AreEqual(1, logEvent.ParentEvents.Count());
            Assert.AreEqual(1, logEvent.LogProperties.Count());
            Assert.IsNotNull(logEvent.Keyspace);
            Assert.IsNotNull(logEvent.TableViewIndex);
            Assert.AreEqual("coafstatim", logEvent.Keyspace.Name);
            Assert.AreEqual("coafstatim.application", logEvent.TableViewIndex.FullName);
            Assert.AreEqual(1, logEvent.DDLItems.Count());

            nIdx = 6;
            logEvent = result.Results.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(EventClasses.SolrHardCommit | EventClasses.Information, logEvent.Class);
            Assert.AreEqual(EventTypes.SessionBegin, logEvent.Type);
            Assert.IsNotNull(logEvent.Id);
            Assert.AreEqual(parentSession.Id, logEvent.Id);
            Assert.AreEqual(DateTime.Parse("2017-09-23 16:32:34.784"), logEvent.EventTimeLocal);
            Assert.AreEqual(DateTimeOffset.Parse("2017-09-23 16:32:34.784 +00"), logEvent.EventTime);
            Assert.IsTrue(logEvent.EventTimeEnd.HasValue);
            Assert.IsTrue(logEvent.Duration.HasValue);
            Assert.IsTrue(logEvent.EventTimeBegin.HasValue);
            Assert.AreEqual(new TimeSpan(0, 0, 0, 11, 015), logEvent.Duration);
            Assert.AreEqual(logEvent.EventTime, logEvent.EventTimeBegin);
            Assert.AreEqual(logEvent.EventTime + logEvent.Duration.Value, logEvent.EventTimeEnd);

            Assert.AreEqual(1, logEvent.ParentEvents.Count());
            Assert.AreEqual(1, logEvent.LogProperties.Count());
            Assert.IsNull(logEvent.Keyspace);
            Assert.IsNull(logEvent.TableViewIndex);
            //Assert.AreEqual("coafstatim", logEvent.Keyspace.Name);
            //Assert.AreEqual("coafstatim.application", logEvent.TableViewIndex.FullName);
            Assert.AreEqual(0, logEvent.DDLItems.Count());
            currentParrent = logEvent;

            nIdx = 7;
            logEvent = result.Results.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(EventClasses.SolrHardCommit | EventClasses.Information, logEvent.Class);
            Assert.AreEqual(EventTypes.SessionEnd, logEvent.Type);
            Assert.IsNotNull(logEvent.Id);
            Assert.AreEqual(parentSession.Id, logEvent.Id);
            Assert.AreEqual(DateTime.Parse("2017-09-23 16:32:45.799"), logEvent.EventTimeLocal);
            Assert.AreEqual(DateTimeOffset.Parse("2017-09-23 16:32:45.799 +00"), logEvent.EventTime);
            Assert.IsTrue(logEvent.EventTimeEnd.HasValue);
            Assert.IsTrue(logEvent.Duration.HasValue);
            Assert.IsTrue(logEvent.EventTimeBegin.HasValue);
            Assert.AreEqual(new TimeSpan(0, 0, 0, 11, 015), logEvent.Duration);

            Assert.AreEqual(currentParrent.EventTime, logEvent.EventTimeBegin);
            Assert.AreEqual(logEvent.EventTime - logEvent.Duration.Value, logEvent.EventTimeBegin);
            Assert.AreEqual(logEvent.EventTime, logEvent.EventTimeEnd);

            Assert.AreEqual(2, logEvent.ParentEvents.Count());
            Assert.AreEqual(currentParrent.GetHashCode(), logEvent.ParentEvents.Last().GetHashCode());
            Assert.AreEqual(parentSession.Id, logEvent.ParentEvents.First().GetValue().Id);
            Assert.AreEqual(currentParrent.EventTimeEnd.Value, logEvent.EventTime);
            Assert.AreNotEqual(parentSession.EventTimeEnd.Value, logEvent.EventTime);
            Assert.AreEqual(2, logEvent.LogProperties.Count());
            Assert.IsNull(logEvent.Keyspace);
            Assert.IsNull(logEvent.TableViewIndex);
            //Assert.AreEqual("coafstatim", logEvent.Keyspace.Name);
            //Assert.AreEqual("coafstatim.application", logEvent.TableViewIndex.FullName);
            Assert.AreEqual(0, logEvent.DDLItems.Count());

            nIdx = 8;
            logEvent = result.Results.ElementAt(nIdx) as LogCassandraEvent;

            Assert.AreEqual(EventClasses.SolrHardCommit | EventClasses.Information, logEvent.Class);
            Assert.AreEqual(EventTypes.SessionEnd, logEvent.Type);
            Assert.IsNotNull(logEvent.Id);
            Assert.AreEqual(parentSession.Id, logEvent.Id);
            Assert.AreEqual(DateTime.Parse("2017-09-23 16:32:56.799"), logEvent.EventTimeLocal);
            Assert.AreEqual(DateTimeOffset.Parse("2017-09-23 16:32:56.799 +00"), logEvent.EventTime);
            Assert.IsTrue(logEvent.EventTimeEnd.HasValue);
            Assert.IsTrue(logEvent.Duration.HasValue);
            Assert.IsTrue(logEvent.EventTimeBegin.HasValue);
            Assert.AreEqual(new TimeSpan(0, 0, 0, 55, 036), logEvent.Duration);

            Assert.AreEqual(parentSession.EventTime, logEvent.EventTimeBegin);
            Assert.AreEqual(logEvent.EventTime - logEvent.Duration.Value, logEvent.EventTimeBegin);
            Assert.AreEqual(logEvent.EventTime, logEvent.EventTimeEnd);

            Assert.AreEqual(1, logEvent.ParentEvents.Count());
           //Assert.AreEqual(currentParrent.GetHashCode(), logEvent.ParentEvents.Last().GetHashCode());
            Assert.AreEqual(parentSession.Id, logEvent.ParentEvents.First().GetValue().Id);
            //Assert.AreEqual(currentParrent.EventTimeEnd.Value, logEvent.EventTime);
            Assert.AreEqual(parentSession.EventTimeEnd.Value, logEvent.EventTime);
            Assert.AreEqual(2, logEvent.LogProperties.Count());
            Assert.IsNotNull(logEvent.Keyspace);
            Assert.IsNotNull(logEvent.TableViewIndex);
            Assert.AreEqual("coafstatim", logEvent.Keyspace.Name);
            Assert.AreEqual("coafstatim.application.coafstatim_application_appcretddt_index", logEvent.TableViewIndex.FullName);
            Assert.AreEqual(1, logEvent.DDLItems.Count());
        }

        [TestMethod()]
        public void ProcessFileTest_OverlappingLogTimeRange()
        {
            this.CreateClusterDCNodeDDL();

            Assert.AreEqual(this._cluster, DSEDiagnosticLibrary.Cluster.TryGetCluster(ClusterName));
            Assert.IsNotNull(this._node1);

            var tstNode = DSEDiagnosticLibrary.Cluster.TryGetAddNode("10.0.1.2", this._datacenter2);
            Assert.AreEqual("10.0.1.2", tstNode?.Id.NodeName());
            Assert.AreEqual(tstNode, this._datacenter2.TryGetNode("10.0.1.2"));

            //Range: 2011-02-10 13:33:00,000 to 2011-05-18 22:00:00,000 
            var testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\Log1.log");

            var parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, tstNode, null, null, null);
            
            var nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)5, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(0, parseFile.NbrWarnings);
            Assert.AreEqual(6, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(tstNode, parseFile.Node);

            var result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(tstNode, result.Node);
            Assert.AreEqual(5, result.Results.Count());
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());
            Assert.AreEqual(1, tstNode.LogFiles.Count());
            Assert.AreEqual(DateTimeOffset.Parse("2011-02-10 13:33:00.000 +00"), tstNode.LogFiles.First().LogDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2011-05-18 22:00:00.000 +00"), tstNode.LogFiles.First().LogDateRange.Max);
            Assert.AreEqual(DateTimeOffset.Parse("2011-02-10 13:33:00.000 +00"), tstNode.LogFiles.First().LogFileDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2011-05-18 22:00:00.00 +00"), tstNode.LogFiles.First().LogFileDateRange.Max);

            //Log File Range: 2011-01-10 13:33:00,000 to 2011-02-10 13:31:59,999
            //Range: 2011-01-10 13:33:00,000* to 2011-05-18 22:00:00,000 
            testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\Log1-Before.log");
            parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, tstNode, null, null, null);

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)5, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(0, parseFile.NbrWarnings);
            Assert.AreEqual(6, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(tstNode, parseFile.Node);

            result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(tstNode, result.Node);
            Assert.AreEqual(5, result.Results.Count());
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());
            Assert.AreEqual(2, tstNode.LogFiles.Count());
            Assert.AreEqual(DateTimeOffset.Parse("2011-01-10 13:33:00.000  +00"), tstNode.LogFiles.ElementAt(1).LogDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2011-02-10 13:31:59.999 +00"), tstNode.LogFiles.ElementAt(1).LogDateRange.Max);
            Assert.AreEqual(DateTimeOffset.Parse("2011-01-10 13:33:00.000  +00"), tstNode.LogFiles.ElementAt(1).LogFileDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2011-02-10 13:31:59.999 +00"), tstNode.LogFiles.ElementAt(1).LogFileDateRange.Max);

            //Log File Range: 2011-05-18 22:00:00,001 to 2011-06-18 22:00:00,000
            //Range: 2011-01-10 13:33:00,000 to 2011-06-18 22:00:00,000* 
            testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\Log1-After.log");
            parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, tstNode, null, null, null);

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)5, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(0, parseFile.NbrWarnings);
            Assert.AreEqual(6, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(tstNode, parseFile.Node);

            result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(tstNode, result.Node);
            Assert.AreEqual(5, result.Results.Count());
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());
            Assert.AreEqual(3, tstNode.LogFiles.Count());
            Assert.AreEqual(DateTimeOffset.Parse("2011-05-18 22:00:00.001  +00"), tstNode.LogFiles.ElementAt(2).LogDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2011-06-18 22:00:00.000 +00"), tstNode.LogFiles.ElementAt(2).LogDateRange.Max);
            Assert.AreEqual(DateTimeOffset.Parse("2011-05-18 22:00:00.001  +00"), tstNode.LogFiles.ElementAt(2).LogFileDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2011-06-18 22:00:00.000 +00"), tstNode.LogFiles.ElementAt(2).LogFileDateRange.Max);

            //Log File Range: 2011-03-10 13:33:00,000 to 2011-04-18 22:00:00,000 
            //Range: 2011-01-10 13:33:00,000 to 2011-06-18 22:00:00,000
            testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\Log1-Within.log");
            parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, tstNode, null, null, null);

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)0, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(1, parseFile.NbrWarnings);
            Assert.AreEqual(0, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(tstNode, parseFile.Node);

            result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(tstNode, result.Node);
            Assert.IsNull(result.Results);
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());            
            Assert.AreEqual(4, tstNode.LogFiles.Count());
            Assert.AreEqual(DateTimeOffset.Parse("2011-03-10 13:33:00.000  +00"), tstNode.LogFiles.ElementAt(3).LogDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2011-04-18 22:00:00.00 +00"), tstNode.LogFiles.ElementAt(3).LogDateRange.Max);
            Assert.AreEqual(DateTimeOffset.Parse("2011-03-10 13:33:00.000  +00"), tstNode.LogFiles.ElementAt(3).LogFileDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2011-04-18 22:00:00.000 +00"), tstNode.LogFiles.ElementAt(3).LogFileDateRange.Max);

            //Log File Range: 22011-02-10 13:33:00,000  to 2011-05-18 22:00:00,000
            //Range: 2011-01-10 13:33:00,000 to 2011-06-18 22:00:00,000
            testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\Log1-Same.log");
            parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, tstNode, null, null, null);

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)0, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(1, parseFile.NbrWarnings);
            Assert.AreEqual(0, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(tstNode, parseFile.Node);

            result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(tstNode, result.Node);
            Assert.IsNull(result.Results);
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());
            Assert.AreEqual(5, tstNode.LogFiles.Count());
            Assert.AreEqual(DateTimeOffset.Parse("2011-02-10 13:33:00.000  +00"), tstNode.LogFiles.ElementAt(4).LogDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2011-05-18 22:00:00.000 +00"), tstNode.LogFiles.ElementAt(4).LogDateRange.Max);
            Assert.AreEqual(DateTimeOffset.Parse("2011-02-10 13:33:00.000  +00"), tstNode.LogFiles.ElementAt(4).LogFileDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2011-05-18 22:00:00.000 +00"), tstNode.LogFiles.ElementAt(4).LogFileDateRange.Max);

            //Log File Range: 2011-01-08 13:33:00,000  to 2011-02-10 13:31:59,998
            //Range: 2011-01-08 13:33:00,000* to 2011-06-18 22:00:00,000
            testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\Log1-OL-Before.log");
            parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, tstNode, null, null, null);

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)5, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(1, parseFile.NbrWarnings);
            Assert.AreEqual(8, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(tstNode, parseFile.Node);

            result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(tstNode, result.Node);
            Assert.AreEqual(5, result.Results.Count());
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());
            Assert.AreEqual(6, tstNode.LogFiles.Count());
            Assert.AreEqual(DateTimeOffset.Parse("2011-01-08 13:33:00.000  +00"), tstNode.LogFiles.ElementAt(5).LogDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2011-01-09 13:31:59.999 +00"), tstNode.LogFiles.ElementAt(5).LogDateRange.Max);
            Assert.AreEqual(DateTimeOffset.Parse("2011-01-08 13:33:00.000  +00"), tstNode.LogFiles.ElementAt(5).LogFileDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2011-02-10 13:31:59.998 +00"), tstNode.LogFiles.ElementAt(5).LogFileDateRange.Max);

            //Log File Range: 2011-05-18 22:00:00,002  to 2011-07-18 23:00:00,000
            //Range: 2011-01-08 13:33:00,000 to 2011-07-18 23:00:00,000*
            testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\Log1-OL-After.log");
            parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, tstNode, null, null, null);

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)5, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(1, parseFile.NbrWarnings);
            Assert.AreEqual(12, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(tstNode, parseFile.Node);

            result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(tstNode, result.Node);
            Assert.AreEqual(5, result.Results.Count());
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());
            Assert.AreEqual(7, tstNode.LogFiles.Count());
            Assert.AreEqual(DateTimeOffset.Parse("2011-06-18 22:00:00.000  +00"), tstNode.LogFiles.ElementAt(6).LogDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2011-07-18 23:00:00.000 +00"), tstNode.LogFiles.ElementAt(6).LogDateRange.Max);
            Assert.AreEqual(DateTimeOffset.Parse("2011-05-18 22:00:00.002  +00"), tstNode.LogFiles.ElementAt(6).LogFileDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2011-07-18 23:00:00.000 +00"), tstNode.LogFiles.ElementAt(6).LogFileDateRange.Max);

            //Debug Log (new Node)
            tstNode = DSEDiagnosticLibrary.Cluster.TryGetAddNode("10.0.2.2", this._datacenter2);
            Assert.AreEqual("10.0.2.2", tstNode?.Id.NodeName());
            Assert.AreEqual(tstNode, this._datacenter2.TryGetNode("10.0.2.2"));

            testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\Debug1.log");
            parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, tstNode, null, null, null);
            parseFile.DebugLogProcessing = file_cassandra_log4net.DebugLogProcessingTypes.Disabled;

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)0, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(0, parseFile.NbrWarnings);
            Assert.AreEqual(0, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(tstNode, parseFile.Node);

            result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(tstNode, result.Node);
            Assert.IsNull(result.Results);
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());
            Assert.AreEqual(0, tstNode.LogFiles.Count());
            
            testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\Debug1.log");
            parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, tstNode, null, null, null);
            parseFile.DebugLogProcessing = file_cassandra_log4net.DebugLogProcessingTypes.OnlyFlushCompactionMsgs;

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)5, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(0, parseFile.NbrWarnings);
            Assert.AreEqual(17, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(tstNode, parseFile.Node);

            result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(tstNode, result.Node);
            Assert.IsNotNull(result.Results);
            Assert.AreEqual(5, result.Results.Count());
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());
            Assert.AreEqual(1, tstNode.LogFiles.Count());
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:33:04.872  +00"), tstNode.LogFiles.ElementAt(0).LogDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:40:17.100 +00"), tstNode.LogFiles.ElementAt(0).LogDateRange.Max);
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:33:04.872  +00"), tstNode.LogFiles.ElementAt(0).LogFileDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:40:17.100 +00"), tstNode.LogFiles.ElementAt(0).LogFileDateRange.Max);

            //Dup Time Range
            testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\Debug1.log");
            parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, tstNode, null, null, null);
            parseFile.DebugLogProcessing = file_cassandra_log4net.DebugLogProcessingTypes.OnlyFlushCompactionMsgs;

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)0, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(1, parseFile.NbrWarnings);
            Assert.AreEqual(0, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(tstNode, parseFile.Node);

            result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(tstNode, result.Node);
            Assert.IsNull(result.Results);
            //Assert.AreEqual(5, result.Results.Count());
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());
            Assert.AreEqual(2, tstNode.LogFiles.Count());
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:33:04.872  +00"), tstNode.LogFiles.ElementAt(1).LogDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:40:17.100 +00"), tstNode.LogFiles.ElementAt(1).LogDateRange.Max);
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:33:04.872  +00"), tstNode.LogFiles.ElementAt(1).LogFileDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:40:17.100 +00"), tstNode.LogFiles.ElementAt(1).LogFileDateRange.Max);

            //Just Time Range
            testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\Debug1.log");
            parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, tstNode, null, null, null);
            parseFile.DebugLogProcessing = file_cassandra_log4net.DebugLogProcessingTypes.OnlyLogDateRange;

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)1, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(1, parseFile.NbrWarnings);
            Assert.AreEqual(2, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(tstNode, parseFile.Node);

            result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(tstNode, result.Node);
            Assert.IsNull(result.Results);
            //Assert.AreEqual(5, result.Results.Count());
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());
            Assert.AreEqual(3, tstNode.LogFiles.Count());
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:33:04.872  +00"), tstNode.LogFiles.ElementAt(2).LogDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:40:17.100 +00"), tstNode.LogFiles.ElementAt(2).LogDateRange.Max);
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:33:04.872  +00"), tstNode.LogFiles.ElementAt(2).LogFileDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:40:17.100 +00"), tstNode.LogFiles.ElementAt(2).LogFileDateRange.Max);

            // All Messages
            testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\Debug1.log");
            parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, tstNode, null, null, null);
            parseFile.DebugLogProcessing = file_cassandra_log4net.DebugLogProcessingTypes.AllMsg;
            parseFile.CheckOverlappingDateRange = false;

            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)8, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(1, parseFile.NbrWarnings);
            Assert.AreEqual(17, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(tstNode, parseFile.Node);

            result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(tstNode, result.Node);
            Assert.IsNotNull(result.Results);
            Assert.AreEqual(8, result.Results.Count());
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());
            Assert.AreEqual(4, tstNode.LogFiles.Count());
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:33:04.872  +00"), tstNode.LogFiles.ElementAt(3).LogDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:40:17.100 +00"), tstNode.LogFiles.ElementAt(3).LogDateRange.Max);
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:33:04.872  +00"), tstNode.LogFiles.ElementAt(3).LogFileDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:40:17.100 +00"), tstNode.LogFiles.ElementAt(3).LogFileDateRange.Max);

            // Test for non debug log for same dates
            testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\Log2.log"); //Same file as Debug1.log
            parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, tstNode, null, null, null);
            
            nbrLinesParsed = parseFile.ProcessFile();

            Assert.AreEqual((uint)8, nbrLinesParsed);
            Assert.AreEqual(0, parseFile.NbrErrors);
            Assert.AreEqual(0, parseFile.NbrWarnings);
            Assert.AreEqual(17, parseFile.NbrItemsParsed);
            Assert.AreEqual(DSEDiagnosticFileParser.DiagnosticFile.CatagoryTypes.LogFile, parseFile.Catagory);
            Assert.AreEqual(this._cluster, parseFile.Cluster);
            Assert.AreEqual(tstNode, parseFile.Node);

            result = parseFile.Result as DSEDiagnosticFileParser.file_cassandra_log4net.LogResults;

            Assert.IsNotNull(result);
            Assert.AreEqual(tstNode, result.Node);
            Assert.IsNotNull(result.Results);
            Assert.AreEqual(8, result.Results.Count());
            Assert.AreEqual(0, result.OrphanedSessionEvents.Count());
            Assert.AreEqual(5, tstNode.LogFiles.Count());
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:33:04.872  +00"), tstNode.LogFiles.ElementAt(4).LogDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:40:17.100 +00"), tstNode.LogFiles.ElementAt(4).LogDateRange.Max);
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:33:04.872  +00"), tstNode.LogFiles.ElementAt(4).LogFileDateRange.Min);
            Assert.AreEqual(DateTimeOffset.Parse("2017-02-10 13:40:17.100 +00"), tstNode.LogFiles.ElementAt(4).LogFileDateRange.Max);

        }
    }


}