using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace DSEDiagnosticLibrary
{
    public interface ICQLUserDefinedType : IDDLStmt, IEquatable<string>
    {
        IEnumerable<ICQLColumn> Columns { get; }
    }

    public sealed class CQLUserDefinedType : ICQLUserDefinedType, IEquatable<ICQLUserDefinedType>
    {

        public IEnumerable<ICQLColumn> Columns { get { return null; } }

        #region IParsed

        public SourceTypes Source { get { return SourceTypes.CQL; } }
        public IPath Path { get; private set; }
        public Cluster Cluster { get { return this.Keyspace.Cluster; } }
        public IDataCenter DataCenter { get { return this.Node?.DataCenter; } }
        public INode Node { get; private set; }
        public int Items { get; private set; }
        public uint LineNbr { get; private set; }

        #endregion

        #region IDDLStmt
        public IKeyspace Keyspace { get; private set; }
        public string Name { get; private set; }
        public string DDL { get; private set; }
        public object ToDump()
        { return null; }
        public bool Equals(string other)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region IEquatable<ICQLUserDefinedType>
        public bool Equals(ICQLUserDefinedType other)
        {
            throw new NotImplementedException();
        }

        #endregion

        public static IEnumerable<ICQLUserDefinedType> TryGet(IKeyspace ksInstance, string name)
        {
            return ksInstance.DDLs.Where(d => d is ICQLUserDefinedType && d.Name == name).Cast<ICQLUserDefinedType>();
        }
    }
}
