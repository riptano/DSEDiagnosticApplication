using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Common;
using Common.Path;
using Newtonsoft.Json;

namespace DSEDiagnosticLibrary
{
    public sealed class CQLColumnType : IEquatable<string>, IEquatable<CQLColumnType>
    {
        readonly Regex TupleRegEx = new Regex(LibrarySettings.TupleRegExStr, RegexOptions.Compiled | RegexOptions.IgnoreCase);
        readonly Regex FrozenRegEx = new Regex(LibrarySettings.FrozenRegExStr, RegexOptions.Compiled | RegexOptions.IgnoreCase);

        public CQLColumnType(string cqlType,
                                IEnumerable<CQLColumnType> cqlSubType,
                                string ddl,
                                ICQLColumn column = null,
                                bool isUDT = false)
        {
            if (string.IsNullOrEmpty(cqlType)) throw new NullReferenceException("cqlType cannot be null");
            if (string.IsNullOrEmpty(ddl)) throw new NullReferenceException("ddl cannot be null");

            this.Name = cqlType.Trim();
            this.Column = column;
            this.DDL = ddl.Trim();
            this.CQLSubType = cqlSubType ?? Enumerable.Empty<CQLColumnType>();

            this.IsCollection = this.HasCollection = LibrarySettings.CQLCollectionTypes.Any(c => this.Name.ToLower() == c);
            this.IsFrozen = this.HasFrozen = FrozenRegEx.IsMatch(this.DDL);
            this.IsTuple = this.HasTuple = TupleRegEx.IsMatch(this.DDL);

            if (this.CQLSubType.HasAtLeastOneElement())
            {
                if (!this.HasCollection)
                {
                    this.HasCollection = this.CQLSubType.Any(c => c.IsCollection || c.HasCollection);
                }
                if (!this.HasFrozen)
                {
                    this.HasFrozen = this.CQLSubType.Any(c => c.IsFrozen || c.HasFrozen);
                }
                if (!this.HasTuple)
                {
                    this.HasTuple = this.CQLSubType.Any(c => c.IsTuple || c.HasTuple);
                }
                this.SetBaseType(null, true);
            }
        }

        public ICQLColumn Column { get; private set; }
        public CQLColumnType BaseType { get; private set; }
        public string Name { get; private set; }
        public IEnumerable<CQLColumnType> CQLSubType { get; private set; }
        public bool IsFrozen { get; private set; }
        public bool IsCollection { get; private set; }
        public bool IsTuple { get; private set; }
        public bool IsUDT { get; private set; }
        public bool HasFrozen { get; private set; }
        public bool HasCollection { get; private set; }
        public bool HasTuple { get; private set; }
        public string DDL { get; private set; }

        #region overrides
        public override string ToString()
        {
            return this.DDL;
        }
        public override bool Equals(object obj)
        {
            if (obj is string) return this.Equals((string)obj);
            if (obj is CQLColumnType) return this.Equals((CQLColumnType)obj);

            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return this.Name.GetHashCode();
        }
        #endregion

        #region IEquatable methods
        public bool Equals(string other)
        {
            return this.Name == other;
        }

        public bool Equals(CQLColumnType other)
        {
            if (other == null) return false;
            if (ReferenceEquals(this, other)) return true;

            if(this.Name == other.Name
                    && this.IsCollection == other.IsCollection
                    && this.IsFrozen == other.IsFrozen
                    && this.IsTuple == other.IsTuple)
            {
                if (this.CQLSubType.IsEmpty()) return true;

                if(this.CQLSubType.Count() == other.CQLSubType.Count())
                    return this.CQLSubType
                                .SelectWithIndex((r, idx) => r.Equals(other.CQLSubType.ElementAt(idx)))
                                .All(b => b);
            }

            return false;
        }
        #endregion

        public object ToDump()
        {
            return this;
        }

        public CQLColumnType SetColumn(ICQLColumn associatedColumn)
        {
            this.Column = associatedColumn;
            return this;
        }

        public CQLColumnType SetBaseType(CQLColumnType baseType, bool setBaseSubTypes = false)
        {
            this.BaseType = baseType;

            if (setBaseSubTypes)
            {
                foreach (var subType in this.CQLSubType)
                {
                    if (subType.BaseType == null) subType.SetBaseType(this);
                }
            }

            return this;
        }

    }

    public interface ICQLColumn : IEquatable<string>
    {
        string Name { get; }
        ICQLTable Table { get; }
        ICQLUserDefinedType UDT { get; }
        CQLColumnType CQLType { get;}
        bool IsStatic { get; }
        bool IsInOrderBy { get;}
        bool IsPrimaryKey { get; }
        bool IsClusteringKey { get; }
        string DDL { get; }
        object ToDump();
    }

    public sealed class CQLColumn : ICQLColumn, IEquatable<ICQLColumn>
    {
        readonly Regex StaticRegEx = new Regex(LibrarySettings.StaticRegExStr, RegexOptions.Compiled | RegexOptions.IgnoreCase);
        readonly Regex PrimaryKeyRegEx = new Regex(LibrarySettings.PrimaryKeyRegExStr, RegexOptions.Compiled | RegexOptions.IgnoreCase);

        public CQLColumn(string name,
                            CQLColumnType columnType,
                            string ddl,
                            bool associateColumnToType = true)
        {
            if (string.IsNullOrEmpty(name)) throw new NullReferenceException("cqlType cannot be null");
            if (string.IsNullOrEmpty(ddl)) throw new NullReferenceException("ddl cannot be null");
            if (columnType == null) throw new NullReferenceException("columnType cannot be null");

            this.Name = StringHelpers.RemoveQuotes(name.Trim());
            this.DDL = ddl.Trim();
            this.CQLType = columnType;
            this.IsPrimaryKey = PrimaryKeyRegEx.IsMatch(this.DDL);
            this.IsStatic = StaticRegEx.IsMatch(this.DDL);

            if(associateColumnToType)
            {
                this.CQLType.SetColumn(this);
            }
        }
        public ICQLTable Table { get; private set; }
        public ICQLUserDefinedType UDT { get; private set; }
        public string Name { get; private set; }
        public CQLColumnType CQLType { get; private set; }
        public bool IsStatic { get; private set; }
        public bool IsInOrderBy { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsClusteringKey { get; set; }
        public string DDL { get; private set; }

        #region overrides
        public override string ToString()
        {
            return this.DDL;
        }
        public override bool Equals(object obj)
        {
            if (obj is string) return this.Equals((string)obj);
            if (obj is CQLColumn) return this.Equals((CQLColumn)obj);

            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return this.Name.GetHashCode();
        }
        #endregion

        #region IEquatable methods
        public bool Equals(string other)
        {
            return this.Name == StringHelpers.RemoveQuotes(other);
        }

        public bool Equals(ICQLColumn other)
        {
            if (other == null) return false;
            if (ReferenceEquals(this, other)) return true;

            return this.Name == other.Name
                        && this.CQLType.Equals(((CQLColumn)other).CQLType);
        }
        #endregion

        public CQLColumn SetTable(ICQLTable associatedTable)
        {
            this.Table = associatedTable;
            return this;
        }
        public CQLColumn SetUDT(ICQLUserDefinedType associatedUDT)
        {
            this.UDT = associatedUDT;
            return this;
        }
        public object ToDump()
        {
            return this;
        }
    }

    public sealed class CQLOrderByColumn : IEquatable<string>, IEquatable<ICQLColumn>, IEquatable<CQLOrderByColumn>
    {
        public ICQLColumn Column;
        public bool IsDescending;

        public static implicit operator CQLColumn(CQLOrderByColumn orderbyColumn) { return (CQLColumn) orderbyColumn.Column; }

        #region IEquatable

        public bool Equals(ICQLColumn column)
        {
            return this.Column.Equals(column);
        }

        public bool Equals(CQLOrderByColumn columnOrderby)
        {
            return this.Column.Equals(columnOrderby.Column) && this.IsDescending == columnOrderby.IsDescending;
        }

        public bool Equals(string name)
        {
            return this.Column.Equals(name);
        }

        #endregion

        #region overridden

        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj is string) return this.Equals((string)obj);
            if (obj is ICQLColumn) return this.Equals((ICQLColumn)obj);
            if (obj is CQLOrderByColumn) return this.Equals((CQLOrderByColumn)obj);

            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return this.Column.GetHashCode();
        }
        public override string ToString()
        {
            return string.Format("CQLOrderByColumn<{0}, {1}>", this.Column, this.IsDescending ? "Descending" : "Ascending");
        }

        #endregion
    }

    public sealed class CQLFunctionColumn : IEquatable<string>, IEquatable<ICQLColumn>, IEquatable<CQLFunctionColumn>
    {
        public CQLFunctionColumn(string function,
                                    IEnumerable<ICQLColumn> columns,
                                    string ddl)
        {
            if (columns == null || columns.IsEmpty()) throw new NullReferenceException("CQLFunctionColumn must have columns (cannot be null or a count of zero)");

            this.Function = function;
            this.Columns = columns;
            this.DDL = ddl;
        }

        public IEnumerable<ICQLColumn> Columns { get; private set; }
        public string Function { get; private set; }
        public string DDL { get; private set; }

        #region IEquatable

        public bool Equals(ICQLColumn column)
        {
            return this.Columns.Any(c => c.Equals(column));
        }

        public bool Equals(CQLFunctionColumn columnFunctin)
        {
            if (columnFunctin == null) return false;
            if (ReferenceEquals(this, columnFunctin)) return true;

            return this.DDL == columnFunctin.DDL;
        }

        public bool Equals(string name)
        {
            return this.Columns.Any(c => c.Equals(name));
        }

        #endregion

        #region overridden

        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj is string) return this.Equals((string)obj);
            if (obj is ICQLColumn) return this.Equals((ICQLColumn)obj);
            if (obj is CQLFunctionColumn) return this.Equals((CQLFunctionColumn)obj);

            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return (this.Function == null ? 0 : this.Function.GetHashCode()) + this.Columns.Sum(c => c.GetHashCode());
        }
        public override string ToString()
        {
            return string.IsNullOrEmpty(this.Function)
                        ? string.Format("CQLFunctionColumn<{0}>", string.Join(",", this.Columns.Select(c => c.Name)))
                        : string.Format("CQLFunctionColumn<{0}({1})>", this.Function, string.Join(",", this.Columns.Select(c => c.Name)));
        }

        #endregion
    }

    public sealed class CQLTableStats
    {
        public uint Collections;
        public uint Counters;
        public uint Blobs;
        public uint Statics;
        public uint Frozens;
        public uint Tuples;
        public uint UDTs;
    }

    public interface ICQLTable : IDDLStmt, IEquatable<string>
    {
        Guid Id { get; }
        IEnumerable<ICQLColumn> Columns { get; }
        IEnumerable<ICQLColumn> PrimaryKeys { get; }
        IEnumerable<ICQLColumn> ClusteringKeys { get; }
        IEnumerable<CQLOrderByColumn> OrderByCols { get; }
        IDictionary<string,object> Properties { get; }

        string Compaction { get; }
        string Compression { get; }
        bool WithCompactStorage { get; }
        bool IsActive { get; }
        CQLTableStats Stats { get; }

        bool SetAsActive(bool isActive = true);

        IEnumerable<ICQLColumn> TryGetColumns(IEnumerable<string> columns);
        ICQLColumn TryGetColumn(string columnName);
    }

    public sealed class CQLTable : ICQLTable, IEquatable<ICQLTable>
    {
        public CQLTable(IFilePath cqlFile,
                            uint lineNbr,
                            IKeyspace keyspace,
                            string name,
                            IEnumerable<ICQLColumn> columns,
                            IEnumerable<ICQLColumn> primaryKeys,
                            IEnumerable<ICQLColumn> clusteringKeys,
                            IEnumerable<CQLOrderByColumn> orderByCols,
                            IDictionary<string, object> properties,
                            string ddl,
                            INode defindingNode = null,
                            bool associateTableToColumn = true,
                            bool associateTableToKeyspace = true)
        {
            if (string.IsNullOrEmpty(name)) throw new NullReferenceException("CQLTable name cannot be null");
            if (keyspace == null) throw new NullReferenceException("CQLTable must be associated to a keyspace. It cannot be null");
            if (columns == null || columns.IsEmpty()) throw new NullReferenceException("CQLTable must have columns (cannot be null or a count of zero)");
            if (string.IsNullOrEmpty(ddl)) throw new NullReferenceException("CQLTable must have a DDL string");

            this.Stats = new CQLTableStats();
            this.Path = cqlFile;
            this.Keyspace = keyspace;
            this.Node = defindingNode ?? keyspace.Node;
            this.LineNbr = lineNbr;
            this.Name = StringHelpers.RemoveQuotes(name.Trim());
            this.DDL = ddl;
            this.Columns = columns;
            this.PrimaryKeys = primaryKeys;
            this.ClusteringKeys = clusteringKeys;
            this.OrderByCols = orderByCols ?? Enumerable.Empty<CQLOrderByColumn>();
            this.Properties = properties ?? new Dictionary<string, object>(0);
            this.Items = this.Columns.Count();

            #region Generate Primary Key/Cluster Key Enums, if required

            if(this.PrimaryKeys == null || this.PrimaryKeys.IsEmpty())
            {
                this.PrimaryKeys = this.Columns.Where(c => c.IsPrimaryKey);
            }
            if (this.ClusteringKeys == null || this.ClusteringKeys.IsEmpty())
            {
                this.ClusteringKeys = this.Columns.Where(c => c.IsClusteringKey);
            }
            #endregion

            if (this.PrimaryKeys.IsEmpty()) throw new NullReferenceException("CQLTable must have primaryKeys (count of zero)");

            #region Properties

            if (this.Properties.Count > 0)
            {
                object pValue;

                if(this.Properties.TryGetValue("compact storage", out pValue))
                {
                    this.WithCompactStorage = (bool) pValue;
                }
                if (this.Properties.TryGetValue("compaction", out pValue))
                {
                    if(pValue is string)
                    {
                        this.Properties["compaction"] = JsonConvert.DeserializeObject<Dictionary<string, object>>((string)pValue);
                    }

                    this.Compaction = StringHelpers.RemoveNamespace((string) ((Dictionary<string, object>)pValue)["class"]);

                }
                if (this.Properties.TryGetValue("compression", out pValue))
                {
                    if (pValue is string)
                    {
                        this.Properties["compression"] = JsonConvert.DeserializeObject<Dictionary<string, object>>((string)pValue);
                    }

                    var compressionProps = (Dictionary<string, object>) pValue;
                    object enabled;
                    bool isEnabled = true;

                    if(compressionProps.TryGetValue("enabled", out enabled))
                    {
                        isEnabled = bool.Parse((string) enabled);
                    }

                    if (isEnabled)
                    {
                        this.Compression = StringHelpers.RemoveNamespace((string)((Dictionary<string, object>)pValue)["class"]);
                    }
                    else
                    {
                        this.Compression = null;
                    }
                }
                if (this.Properties.TryGetValue("id", out pValue))
                {
                    if (pValue is string)
                    {
                        this.Id = new Guid((string)pValue);
                    }
                    else
                    {
                        this.Id = (Guid)pValue;
                    }
                }
            }

            #endregion

            #region stats
            foreach (var column in this.Columns)
            {
                this.Stats.Counters = 0;
                this.Stats.Blobs = 0;

                if(column is ICQLUserDefinedType)
                {
                    ++this.Stats.UDTs;
                }
                else
                {
                    var cqlColumn = (CQLColumn)column;

                    if(cqlColumn.CQLType.HasCollection)
                    {
                        ++this.Stats.Collections;
                    }
                    if (cqlColumn.CQLType.HasFrozen)
                    {
                        ++this.Stats.Frozens;
                    }
                    if (cqlColumn.CQLType.HasTuple)
                    {
                        ++this.Stats.Tuples;
                    }

                    if (cqlColumn.CQLType.Name == "counter")
                    {
                        ++this.Stats.Counters;
                    }
                    else if (cqlColumn.CQLType.Name == "blob")
                    {
                        ++this.Stats.Blobs;
                    }
                    else
                    {
                        foreach (var subType in cqlColumn.CQLType.CQLSubType)
                        {
                            if (subType.Name == "counter")
                            {
                                ++this.Stats.Counters;
                            }
                            else if (subType.Name == "blob")
                            {
                                ++this.Stats.Blobs;
                            }
                        }
                    }

                    if (cqlColumn.IsStatic)
                    {
                        ++this.Stats.Statics;
                    }
                }
            }
            #endregion

            if(associateTableToColumn)
            {
                foreach (var col in this.Columns)
                {
                    ((CQLColumn)col).SetTable(this);
                }
            }

            if(associateTableToKeyspace)
            {
                this.Keyspace.AssociateItem(this);
            }
        }

        #region ICQLTable

        public Guid Id { get; private set; }
        public IEnumerable<ICQLColumn> Columns { get; private set; }
        public IEnumerable<ICQLColumn> PrimaryKeys { get; private set; }
        public IEnumerable<ICQLColumn> ClusteringKeys { get; private set; }
        public IEnumerable<CQLOrderByColumn> OrderByCols { get; private set; }
        public IDictionary<string, object> Properties { get; private set; }

        public string Compaction { get; }
        public string Compression { get; }
        public bool WithCompactStorage { get; }
        public CQLTableStats Stats { get; private set;}
        public bool IsActive { get; private set; }
        public bool SetAsActive(bool isActive = true)
        {
            if(isActive != this.IsActive)
            {
                if(isActive)
                {
                    ++this.Keyspace.Stats.NbrActive;
                }
                else
                {
                    --this.Keyspace.Stats.NbrActive;
                }
                this.IsActive = isActive;
            }
            return this.IsActive;
        }

        public IEnumerable<ICQLColumn> TryGetColumns(IEnumerable<string> columns)
        {
            return columns.Select(n => this.TryGetColumn(StringHelpers.RemoveQuotes(n.Trim())));
        }
        public ICQLColumn TryGetColumn(string columnName)
        {
            columnName = StringHelpers.RemoveQuotes(columnName.Trim());

            return this.Columns.FirstOrDefault(c => c.Name == columnName);
        }
        #endregion

        #region IParsed
        public SourceTypes Source { get { return SourceTypes.CQL; } }
        public IPath Path { get; private set; }
        public Cluster Cluster { get { return this.Keyspace.Cluster; } }
        public IDataCenter DataCenter { get { return this.Node?.DataCenter ?? this.Keyspace.DataCenter; } }
        public INode Node { get; private set; }
        public int Items { get; private set; }
        public uint LineNbr { get; private set; }

        #endregion

        #region IDDLStmt
        public IKeyspace Keyspace { get; private set; }
        public string Name { get; private set; }
        public string DDL { get; private set; }
        public object ToDump()
        {
            return new { this.Keyspace,
                            this.Name,
                            this.Columns,
                            this.Properties,
                            this.DDL };
        }
        public bool Equals(string other)
        {
            return this.Name == StringHelpers.RemoveQuotes(other);
        }

        #endregion

        #region IEquatable<ICQLTable>
        public bool Equals(ICQLTable other)
        {
            if (other == null) return false;
            if (ReferenceEquals(this, other)) return true;

            return this.DDL == other.DDL;
        }

        #endregion

        #region overrides
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(this, obj)) return true;
            if (obj is string) return this.Equals((string)obj);
            if (obj is IKeyspace) return this.Keyspace.Equals((IKeyspace)obj);
            if (obj is IDataCenter) return this.DataCenter.Equals((IDataCenter)obj);
            if (obj is ICQLTable) return this.Equals((ICQLTable)obj);

            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return (this.Keyspace.Name + "." + this.Name).GetHashCode();
        }

        public override string ToString()
        {
            return string.Format("CQLTable{{{0}}}", this.DDL);
        }
        #endregion
        public static ICQLTable TryGet(IKeyspace ksInstance, string name)
        {
            return ksInstance.TryGetTable(name);
        }
    }
}
