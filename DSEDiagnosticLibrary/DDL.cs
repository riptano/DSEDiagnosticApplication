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
        /// <summary>
        /// Usually this is just the name, but sometimes it is the name plus a reference to an associated object like in the case of an index. In this case RefferenceName is the tale name plus the index name.
        /// </summary>
        string ReferenceName { get; }
        string FullName { get; }
		string DDL { get; }

        IEnumerable<IAggregatedStats> AggregatedStats { get; }
        IDDL AssociateItem(IAggregatedStats aggregatedStat);

        string NameWAttrs(bool fullName = false, bool forceAttr = false);
        string ReferenceNameWAttrs(bool forceAttr = false);

        object ToDump();
    }

    public interface IDDLStmt : IDDL
    {
        IKeyspace Keyspace { get; }

        bool? IsActive { get; }
        bool? SetAsActive(bool isActive = true);
    }
}
