using Microsoft.VisualStudio.TestTools.UnitTesting;
using DSEDiagnosticLibrary;
using Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticLibrary.Tests
{

    [TestClass()]
    public class LogCassandraEventTests
    {
        static readonly string ClusterName = "TstCluster";
        static readonly string DC1 = "DC1";
        static readonly string DC2 = "DC1_SPARK";
        static readonly string NodeName1 = "10.0.0.1";
        static readonly string NodeName2 = "10.0.0.2";
        //static readonly string TstKeySpaceName = "usprodsec";
        //static readonly string TimeZoneName = "UTC";

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
        }

        public void CheckEnumerable(object expectedItem, object actualItem, string propName)
        {            
            if(expectedItem == null || actualItem == null)
            {
                if (expectedItem == null && actualItem == null) return;

                if (expectedItem == null)
                    Assert.IsTrue(Common.LinqExtensions.IsEmpty((IEnumerable<dynamic>)actualItem),
                                    string.Format("Collection \"{0}\" expected is null but actual has elements.", propName));

                else
                    Assert.IsTrue(Common.LinqExtensions.IsEmpty((IEnumerable<dynamic>)expectedItem),
                                    string.Format("Collection \"{0}\" actual is null but expected has elements.", propName));

                return;
            }

            if (typeof(IReadOnlyDictionary<string, object>).IsAssignableFrom(expectedItem.GetType()))
            {
                var expectedDir = (IReadOnlyDictionary<string, object>)expectedItem;
                var currDir = (IReadOnlyDictionary<string, object>)actualItem;

                Assert.AreEqual(expectedDir.Count, currDir.Count);
                for (int nIdx = 0; nIdx < expectedDir.Count; nIdx++)
                {
                    Assert.AreEqual(expectedDir.ElementAt(nIdx).Key, currDir.ElementAt(nIdx).Key);
                    
                    if (expectedDir.ElementAt(nIdx).Value.GetType() != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(expectedDir.ElementAt(nIdx).Value.GetType()))
                    {
                        CheckEnumerable(expectedDir.ElementAt(nIdx).Value, currDir.ElementAt(nIdx).Value, string.Format("Sub Item for {0} at {1}", propName, nIdx));
                    }
                    else
                    {
                        Assert.AreEqual(expectedDir.ElementAt(nIdx).Value, currDir.ElementAt(nIdx).Value);
                    }
                }
            }
            else
            {
                if(Common.LinqExtensions.IsEmpty((IEnumerable<dynamic>)expectedItem))
                {
                    Assert.IsTrue(Common.LinqExtensions.IsEmpty((IEnumerable<dynamic>)actualItem), string.Format("Collection \"{0}\" actual has no elements but actual does.", propName));
                    return;
                }

                Assert.IsTrue(System.Linq.Enumerable.SequenceEqual((IEnumerable<dynamic>)expectedItem, (IEnumerable<dynamic>)actualItem),
                                                                    string.Format("Collection \"{0}\" did not have the same elements. {{{1}}} => {{{2}}}",
                                                                                    propName,
                                                                                    string.Join(", ", ((IEnumerable<dynamic>)expectedItem).Select(i => i.ToString())),
                                                                                    string.Join(", ", ((IEnumerable<dynamic>)actualItem).Select(i => i.ToString()))));
            }            
        }

        [TestMethod()]
        public void LogCassandraEventMMTest()
        {
            this.CreateClusterDCNodeDDL();

            Assert.AreEqual(this._cluster, DSEDiagnosticLibrary.Cluster.GetCurrentOrMaster());
            Assert.IsNotNull(this._node1);

            var mmCollection = new Common.Patterns.Collections.MemoryMapped.List<LogCassandraEvent>();
            var logTimestamp = DateTime.Now.ConvertToOffSet();
            List<ILogEvent> parentEvents = null;
            var properties = new Dictionary<string, object>();
            var ssTables = new List<string>() { @"/var/lib/cassandra/data/system/compactions_in_progress-55080ab05d9c388690a4acb25fe1f77b/system-compactions_in_progress-ka-90586-Data.db" };
            var assocNodes = new List<INode>() { this._node2 };
            

            properties.Add("IPAddress1", this._node1.Id.Addresses.First());
            properties.Add("IPAddress1EndPt", new System.Net.IPEndPoint(this._node1.Id.Addresses.First(), 1024));
            properties.Add("Node1", this._node1);            
            properties.Add("Items", new List<object>() { new System.Net.IPEndPoint(this._node1.Id.Addresses.First(), 1024), this._node1.Id.Addresses.First() });
            properties.Add("SSTable", ssTables.First());

            var logEvent = new LogCassandraEvent(Common.Path.PathUtils.BuildFilePath(@"c:\logfile.log"),
                                                    this._node1,
                                                    0,
                                                    EventClasses.Error,
                                                   logTimestamp.DateTime,
                                                   logTimestamp,
                                                   "Test Log Message",
                                                   EventTypes.SessionDefinedByDuration,
                                                   "9fefd4a1-d353-11e6-9940-7d76bd094de7",
                                                   "Test class",
                                                   parentEvents,
                                                   Common.TimeZones.LocalTimeZone,
                                                   null,
                                                   null,
                                                   new TimeSpan(0, 5, 0),
                                                   properties,
                                                   ssTables,
                                                   null,
                                                   assocNodes,
                                                   null,
                                                   "Item1",
                                                   DSEInfo.InstanceTypes.Cassandra,
                                                   true);

            mmCollection.Add(logEvent);

            var fndItem = mmCollection.FindMMValue(logEvent.GetHashCode());
            var mmValue = mmCollection.GetMMValue(0);
            var mmValueCache = mmValue.Value;

            Assert.AreEqual(fndItem, mmValue);
            Assert.AreEqual(fndItem.Value, mmValueCache);
            Assert.AreEqual(logEvent, mmValueCache);

            var logEventProps = typeof(LogCassandraEvent).GetProperties();
            
            foreach(var prop in logEventProps)
            {
                if (prop.PropertyType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(prop.PropertyType))
                {
                    CheckEnumerable(prop.GetValue(logEvent), prop.GetValue(mmValueCache), prop.Name);
                }
                else
                {
                    Assert.AreEqual(prop.GetValue(logEvent), prop.GetValue(mmValueCache));
                }
            }

            //Child Event
            var tokenRanges = new List<DSEDiagnosticLibrary.DSEInfo.TokenRangeInfo>();

            parentEvents = new List<ILogEvent>() { logEvent };
            logTimestamp = logTimestamp + new TimeSpan(0, 10, 0);            

            tokenRanges.Add(new DSEInfo.TokenRangeInfo(0, -100, new UnitOfMeasure("10 MB")));

            logEvent = new LogCassandraEvent(Common.Path.PathUtils.BuildFilePath(@"c:\logfile.log"),
                                                    this._node1,
                                                    0,
                                                    EventClasses.Error,
                                                   logTimestamp.DateTime,
                                                   logTimestamp,
                                                   "Test Log Message Child",
                                                   EventTypes.SessionDefinedByDuration,
                                                   null,
                                                   "Test class",
                                                   parentEvents,
                                                   Common.TimeZones.LocalTimeZone,
                                                   null,
                                                   null,
                                                   new TimeSpan(0, 5, 0),
                                                   null,
                                                   null,
                                                   null,
                                                   assocNodes,
                                                   tokenRanges,
                                                   "Item2",
                                                   DSEInfo.InstanceTypes.Cassandra,
                                                   true);

            mmCollection.Add(logEvent);

            fndItem = mmCollection.FindMMValue(logEvent.GetHashCode());
            mmValue = mmCollection.GetMMValue(1);
            mmValueCache = mmValue.Value;

            Assert.AreEqual(fndItem, mmValue);
            Assert.AreEqual(fndItem.Value, mmValueCache);
            Assert.AreEqual(logEvent, mmValueCache);
            
            foreach (var prop in logEventProps)
            {
                if (prop.PropertyType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(prop.PropertyType))
                {
                    CheckEnumerable(prop.GetValue(logEvent), prop.GetValue(mmValueCache), prop.Name);
                }
                else
                {
                    Assert.AreEqual(prop.GetValue(logEvent), prop.GetValue(mmValueCache));
                }
            }

            //Check Parent
            var parentLogEvent = parentEvents[0];

            Assert.AreEqual(logEvent.ParentEvents.First(), parentLogEvent);
            Assert.AreEqual(mmValueCache.ParentEvents.First(), parentLogEvent);

            var parentMMCache = mmValueCache.ParentEvents.First().Value;

            foreach (var prop in logEventProps)
            {
                if (prop.PropertyType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(prop.PropertyType))
                {
                    CheckEnumerable(prop.GetValue(parentLogEvent), prop.GetValue(parentMMCache), prop.Name);
                }
                else
                {
                    Assert.AreEqual(prop.GetValue(parentLogEvent), prop.GetValue(parentMMCache));
                }
            }
        }
    }
}