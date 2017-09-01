using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticLibrary
{
	public interface IDDL : IParsed
	{
		string Name { get; }
        string FullName { get; }
		string DDL { get; }

        IEnumerable<IAggregatedStats> AggregatedStats { get; }
        IDDL AssociateItem(IAggregatedStats aggregatedStat);

        object ToDump();
    }

    public interface IDDLStmt : IDDL
    {
        IKeyspace Keyspace { get; }
    }
}
