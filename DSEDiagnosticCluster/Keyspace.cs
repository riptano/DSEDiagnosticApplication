using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticLibrary
{
	public sealed class KeyspaceStats
	{
		uint Tables;
		uint Indexes;
		uint SolrIndexes;
		uint NbrDDLs;
		uint NbrActive;
		uint STCS;
		uint LCS;
		uint DTCS;
		uint TCS;
		uint TWCS;
		uint OtherStrategies;

	}

	public sealed class KeyspaceReplicationInfo
	{
		ushort RF;
		IDataCenter DataCenter;
	}

	public sealed class LocalKeyspaceInfo
	{
		public IKeyspace Keyspace;
		public ushort RF;
	}

	public interface IKeyspace : IEnumerable<IDDL>
	{
		string Name { get; }
		string Definition { get; }
		IEnumerable<IDDL> DDLs { get; }
		string ReplicationStrategy { get; }
		KeyspaceStats Stats { get; }
		IEnumerable<KeyspaceReplicationInfo> DataCenters { get; }
	}
}
