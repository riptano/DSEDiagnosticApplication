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
		string DDL { get; }
        object ToDump();
    }

    public interface IDDLStmt : IDDL
    {
        IKeyspace Keyspace { get; }
    }
}
