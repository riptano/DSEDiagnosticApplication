using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace DSEDiagnosticLibrary
{
    
    public interface IResult
    {        
        IPath Path { get; }
        Cluster Cluster { get; }
        IDataCenter DataCenter { get; }
        INode Node { get; }
        int NbrItems { get; }

        IEnumerable<IParsed> Results { get; }
    }
}
