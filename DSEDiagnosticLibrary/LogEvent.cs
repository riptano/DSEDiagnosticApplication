using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using IMMLogValue = Common.Patterns.Collections.MemoryMapped.IMMValue<DSEDiagnosticLibrary.ILogEvent>;

namespace DSEDiagnosticLibrary
{
    public interface ILogEvent : IEvent, Common.Patterns.Collections.IMemoryMapperElement
    {
        string SessionTieOutId { get; }
        IReadOnlyDictionary<string, object> LogProperties { get; }
        string LogMessage { get; }
        IEnumerable<string> SSTables { get; }
        IEnumerable<IDDLStmt> DDLItems { get; }
        IEnumerable<INode> AssociatedNodes { get; }
        IEnumerable<DSEInfo.TokenRangeInfo> TokenRanges { get; }
        string Exception { get; }
        IEnumerable<string> ExceptionPath { get; }

        new IEnumerable<IMMLogValue> ParentEvents { get; }

        /// <summary>
        /// This will return true if the two events are the same or similar based on source, class, type, keyspace, and DDL instance
        /// </summary>
        /// <param name="matchEvent"></param>
        /// <param name="mustMatchNode"></param>
        /// <returns></returns>
        bool Match(IEvent matchEvent, bool mustMatchNode = true, bool mustMatchKeyspaceDDL = true);

        /// <summary>
        /// Returns true if the arguments match.
        /// </summary>
        /// <param name="possibleDDLName">
        /// Can be null, if so this argument is ignored
        /// If possibleKeyspaceName is given, the keyspace must also match the DDL statement.
        /// </param>
        /// <param name="possibleKeyspaceName">
        /// Can be null, if so this argument is ignored.
        /// </param>
        /// <param name="possibleNode">
        /// Can be null, if so this argument is ignored
        /// </param>
        /// <param name="checkDDLItems"></param>
        /// <returns></returns>
        bool Match(string possibleDDLName, string possibleKeyspaceName, INode possibleNode = null, bool checkDDLItems = false);
    }

}
