﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
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
        public void ProcessFileTest_NodeConfig()
        {
            this.CreateClusterDCNodeDDL();

            Assert.AreEqual(this._cluster, DSEDiagnosticLibrary.Cluster.GetCurrentOrMaster());
            Assert.IsNotNull(this._node1);

            var testLogFile = Common.Path.PathUtils.BuildFilePath(@".\LogFiles\NodeConfig.log");

            var parseFile = new file_cassandra_log4net(DiagnosticFile.CatagoryTypes.LogFile, testLogFile.ParentDirectoryPath, testLogFile, this._node1, null, null, null);

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