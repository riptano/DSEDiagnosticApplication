using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Common;
using Common.Path;
using CTS = Common.Patterns.Collections.ThreadSafe;

namespace DSEDiagnosticLibrary
{
    [JsonObject(MemberSerialization.OptOut)]
    public sealed class CQLColumnType : IEquatable<string>, IEquatable<CQLColumnType>
    {
        readonly static Regex TupleRegEx = new Regex(LibrarySettings.TupleRegExStr, RegexOptions.Compiled | RegexOptions.IgnoreCase);
        readonly static Regex FrozenRegEx = new Regex(LibrarySettings.FrozenRegExStr, RegexOptions.Compiled | RegexOptions.IgnoreCase);
        readonly static Regex BlobRegEx = new Regex(LibrarySettings.BlobRegExStr, RegexOptions.Compiled | RegexOptions.IgnoreCase);
        readonly static Regex CounterRegEx = new Regex(LibrarySettings.CounterRegExStr, RegexOptions.Compiled | RegexOptions.IgnoreCase);

        public CQLColumnType(string cqlType,
                                IEnumerable<CQLColumnType> cqlSubType,
                                string ddl,
                                ICQLColumn column = null,
                                bool isUDT = false,
                                bool setBaseType = true)
        {
            if (string.IsNullOrEmpty(cqlType)) throw new NullReferenceException("cqlType cannot be null");
            if (string.IsNullOrEmpty(ddl)) throw new NullReferenceException("ddl cannot be null");

            this.Name = cqlType.Trim();
            this.Column = column;
            this.DDL = ddl.Trim();
            this.CQLSubType = cqlSubType ?? Enumerable.Empty<CQLColumnType>();
            this.IsUDT = this.HasUDT = isUDT;

            this.IsCollection = this.HasCollection = LibrarySettings.CQLCollectionTypes.Any(c => this.Name.ToLower() == c);

            if (this.IsCollection)
            {
                this.HasFrozen = FrozenRegEx.IsMatch(this.DDL);
                this.HasTuple = TupleRegEx.IsMatch(this.DDL);
                this.HasCounter = CounterRegEx.IsMatch(this.DDL);
                this.HasBlob = BlobRegEx.IsMatch(this.DDL);
            }
            else
            {
                this.IsFrozen = this.HasFrozen = FrozenRegEx.IsMatch(this.DDL);
                this.IsTuple = this.HasTuple = TupleRegEx.IsMatch(this.DDL);
                this.IsCounter = this.HasCounter = CounterRegEx.IsMatch(this.DDL);
                this.IsBlob = this.HasBlob = BlobRegEx.IsMatch(this.DDL);
            }

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
                if (!this.HasBlob)
                {
                    this.HasBlob = this.CQLSubType.Any(c => c.IsBlob || c.HasBlob);
                }
                if (!this.HasCounter)
                {
                    this.HasCounter = this.CQLSubType.Any(c => c.IsCounter || c.HasCounter);
                }
                if (!this.HasUDT)
                {
                    this.HasUDT = this.CQLSubType.Any(c => c.IsUDT || c.HasUDT);
                }

                if (setBaseType)
                {
                    this.SetBaseType(null, true);
                }
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
        public bool IsBlob { get; private set; }
        public bool IsCounter { get; private set; }
        public bool HasFrozen { get; private set; }
        public bool HasCollection { get; private set; }
        public bool HasTuple { get; private set; }
        public bool HasBlob { get; private set; }
        public bool HasCounter { get; private set; }
        public bool HasUDT { get; private set; }
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

        public CQLColumnType Copy(ICQLColumn assocColumn = null,
                                    bool setBaseType = true)
        {
            return new CQLColumnType(this.Name,
                                        this.CQLSubType.Select(t => t.Copy(assocColumn, setBaseType)),
                                        this.DDL,
                                        assocColumn,
                                        this.IsUDT,
                                        setBaseType);
        }

        public CQLColumnType SetColumn(ICQLColumn associatedColumn)
        {
            this.Column = associatedColumn;
            return this;
        }

        public CQLColumnType SetBaseType(CQLColumnType baseType, bool setBaseSubTypes = false)
        {
            this.BaseType = baseType;

            if (this.BaseType != null && this.BaseType.IsFrozen && !this.IsFrozen)
                this.IsFrozen = true;

            if (setBaseSubTypes)
            {
                foreach (var subType in this.CQLSubType)
                {
                    if (subType.BaseType == null)
                    {
                        subType.SetBaseType(this, true);                        
                    }
                }
            }

            return this;
        }

    }

    public interface ICQLColumn : IEquatable<string>, IEquatable<ICQLColumn>
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

        ICQLColumn Copy(ICQLTable assocatedTbl = null,
                            ICQLUserDefinedType associatedUDT = null,
                            bool associateColumnToType = true,
                            bool setBaseType = true,
                            bool resetAttribs = true);
    }

    [JsonObject(MemberSerialization.OptOut)]
    public sealed class CQLColumn : ICQLColumn
    {
        static readonly Regex StaticRegEx = new Regex(LibrarySettings.StaticRegExStr, RegexOptions.Compiled | RegexOptions.IgnoreCase);
        static readonly Regex PrimaryKeyRegEx = new Regex(LibrarySettings.PrimaryKeyRegExStr, RegexOptions.Compiled | RegexOptions.IgnoreCase);

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

        public ICQLColumn Copy(ICQLTable assocatedTbl = null,
                                    ICQLUserDefinedType associatedUDT = null,
                                    bool associateColumnToType = true,
                                    bool setBaseType = true,
                                    bool resetAttribs = false)
        {
            return new CQLColumn(this.Name,
                                    this.CQLType.Copy(null, setBaseType),
                                    this.DDL,
                                    associateColumnToType)
            {
                UDT = associatedUDT,
                Table = assocatedTbl,
                IsPrimaryKey = resetAttribs ? false : this.IsPrimaryKey,
                IsInOrderBy = resetAttribs ? false : this.IsInOrderBy,
                IsClusteringKey = resetAttribs ? false : this.IsClusteringKey
            };
        }
    }

    [JsonObject(MemberSerialization.OptOut)]
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

    [JsonObject(MemberSerialization.OptOut)]
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

        public string PrettyPrint()
        {
            return string.IsNullOrEmpty(this.Function)
                        ? string.Join(",", this.Columns.Select(c => c.DDL))
                        : string.Format("{0}({1})", this.Function, string.Join(",", this.Columns.Select(c => c.DDL)));
        }

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

        [JsonProperty(PropertyName="HashCode")]
        int _hashCode = 0;
        public override int GetHashCode()
        {
            return this._hashCode == 0
                    ? this._hashCode = (this.Function == null ? 0 : this.Function.GetHashCode()) + this.Columns.Sum(c => c.GetHashCode())
                    : this._hashCode;
        }
        public override string ToString()
        {
            return string.IsNullOrEmpty(this.Function)
                        ? string.Format("CQLFunctionColumn<{0}>", string.Join(",", this.Columns.Select(c => c.Name)))
                        : string.Format("CQLFunctionColumn<{0}({1})>", this.Function, string.Join(",", this.Columns.Select(c => c.Name)));
        }

        #endregion
    }

    [JsonObject(MemberSerialization.OptOut)]
    public sealed class CQLTableStats
    {
        public uint Collections;
        public uint Counters;
        public uint Blobs;
        public uint Statics;
        public uint Frozens;
        public uint Tuples;
        public uint UDTs;
        
        public uint NbrColumns;
        public uint NbrSecondaryIndexes;
        public uint NbrSolrIndexes;
        public uint NbrSasIIIndexes;
        public uint NbrCustomIndexes;
        public uint NbrMaterializedViews;
        public uint NbrTriggers;

        public UnitOfMeasure Storage = UnitOfMeasure.NaNValue;
        public long ReadCount;
        public long WriteCount;
        public long KeyCount;
        public long SSTableCount;
    }

    public interface ICQLTable : IDDLStmt, IProperties, IEquatable<string>, IEquatable<ICQLTable>
    {
        Guid Id { get; }
        IEnumerable<ICQLColumn> Columns { get; }
        IEnumerable<ICQLColumn> PrimaryKeys { get; }
        IEnumerable<ICQLColumn> ClusteringKeys { get; }
        IEnumerable<CQLOrderByColumn> OrderByCols { get; }

        string Compaction { get; }
        string Compression { get; }
        bool WithCompactStorage { get; }        
        CQLTableStats Stats { get; }
       
        IEnumerable<ICQLColumn> TryGetColumns(IEnumerable<string> columns);
        ICQLColumn TryGetColumn(string columnName);

        /// <summary>
        /// CQL statements associated with this table.
        /// </summary>
        IEnumerable<IDDL> DDLs { get; }

        bool IsFlagged { get; }

        IDDL AssociateItem(IDDL ddl);

        void SetID(string uuid);    
    }

    [JsonObject(MemberSerialization.OptOut)]
    public class CQLTable : ICQLTable
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
            if (string.IsNullOrEmpty(name)) throw new NullReferenceException(string.Format("{0} name cannot be null for CQL \"{1}\"", this.GetType().Name, ddl));
            if (keyspace == null) throw new NullReferenceException(string.Format("{0} must be associated to a keyspace. It cannot be null for CQL \"{1}\"", this.GetType().Name, ddl));
            if (columns == null || columns.IsEmpty()) throw new NullReferenceException(string.Format("{0} must have columns (cannot be null or a count of zero) for CQL \"{1}\"", this.GetType().Name, ddl));
            if (string.IsNullOrEmpty(ddl)) throw new NullReferenceException(string.Format("{0} \"{1}\" must have a DDL string", this.GetType().Name, name));

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
            this.IsFlagged = LibrarySettings.TablesUsageFlag.Contains(this.FullName);
            
            if(!this.IsFlagged)
            {
                this.IsFlagged = LibrarySettings.TablesUsageFlag.Contains(this.Keyspace.Name);
            }

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

            if (this.PrimaryKeys.IsEmpty()) throw new NullReferenceException(string.Format("{0} ({1}) must have primaryKeys (count of zero)", this.GetType().Name, ddl));

            #region Properties

            if (this.Properties.Count > 0)
            {                
                if(this.Properties.TryGetValue("compact storage", out object pValue))
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
                        if(compressionProps.TryGetValue("sstable_compression", out object compressionType))
                        {
                            if(string.IsNullOrEmpty((string) compressionType))
                            {
                                this.Compression = null;
                                isEnabled = false;
                            }
                            else
                            {
                                this.Compression = StringHelpers.RemoveNamespace((string)compressionType);
                            }
                        }
                        else if (compressionProps.TryGetValue("class", out compressionType))
                        {
                            if (string.IsNullOrEmpty((string)compressionType))
                            {
                                this.Compression = null;
                                isEnabled = false;
                            }
                            else
                            {
                                this.Compression = StringHelpers.RemoveNamespace((string)compressionType);
                            }
                        }
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

            this.UpdateColumnStats(this.Columns);

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
        public IEnumerable<ICQLColumn> Columns { get; }
        public IEnumerable<ICQLColumn> PrimaryKeys { get;}
        public IEnumerable<ICQLColumn> ClusteringKeys { get; }
        public IEnumerable<CQLOrderByColumn> OrderByCols { get; }
        public IDictionary<string, object> Properties { get; }

        public string Compaction { get; }
        public string Compression { get; }
        public bool WithCompactStorage { get; }
        public CQLTableStats Stats { get; }

        [JsonProperty(PropertyName="DDLs")]
        private IEnumerable<IDDL> datamemberDDLs
        {
            get { return this._ddlList.UnSafe; }
            set { this._ddlList = new CTS.List<IDDL>(value); }
        }
        private CTS.List<IDDL> _ddlList = new CTS.List<IDDL>();
        [JsonIgnore]
        public IEnumerable<IDDL> DDLs { get { return this._ddlList; } }

        public IDDL AssociateItem(IDDL ddl)
        {
            this._ddlList.Lock();
            try
            {
                var item = this._ddlList.UnSafe.FirstOrDefault(d => d.Equals(ddl));

                if (item == null)
                {
                    this._ddlList.UnSafe.Add(ddl);
                }
                else
                {
                    return item;
                }
            }
            finally
            {
                this._ddlList.UnLock();
            }

            if(ddl is ICQLMaterializedView)
            {
                ++this.Stats.NbrMaterializedViews;
            }
            else if (ddl is ICQLIndex idxInstance)
            {               
                if (idxInstance.IsCustom)
                {
                    if (idxInstance.IsSolr)
                    {
                        ++this.Stats.NbrSolrIndexes;
                    }
                    else if (idxInstance.IsSasII)
                    {
                        ++this.Stats.NbrSasIIIndexes;
                    }
                    else
                    {
                        ++this.Stats.NbrCustomIndexes;
                    }
                }
                else
                {
                    ++this.Stats.NbrSecondaryIndexes;
                }
            }
            else if(ddl is ICQLTrigger)
            {
                ++this.Stats.NbrTriggers;
            }

            return ddl;
        }

        public void SetID(string uuid)
        {
            if(!string.IsNullOrEmpty(uuid))
            {
                if (this.Id == null)
                {
                    this.Id = new Guid(uuid);
                }
                else if(this.Id.ToString("N") != uuid)
                {
                    throw new ArgumentException(string.Format("Trying to set the Id on \"{0}\" which already exists. Current Id {1}; Setting Value {2}",
                                                                this.FullName,
                                                                this.Id,
                                                                uuid),
                                                "Id");
                }
            }
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

        public bool IsFlagged { get; protected set; }

        private string DetermineAttrStr()
        {
            string strAttr = string.Empty;

            if (this.Stats.NbrSolrIndexes > 0)
                strAttr += (char)DSEDiagnosticLibrary.Properties.Settings.Default.SolrAttrChar;
            if (this.Stats.NbrSecondaryIndexes > 0 || this.Stats.NbrSasIIIndexes > 0 || this.Stats.NbrCustomIndexes > 0)
                strAttr += (char)DSEDiagnosticLibrary.Properties.Settings.Default.IndexAttrChar;

            if (this.Stats.NbrTriggers > 0)
                strAttr += (char)DSEDiagnosticLibrary.Properties.Settings.Default.TriggerAttrChar;
            if (this.Stats.NbrMaterializedViews > 0)
                strAttr += (char)DSEDiagnosticLibrary.Properties.Settings.Default.MVTblAttrChar;
            else if (this is ICQLMaterializedView)
                strAttr += (char)DSEDiagnosticLibrary.Properties.Settings.Default.MVAttrChar;

            return strAttr;
        }
        public string NameWAttrs(bool fullName = false, bool forceAttr = false)
        {
            var name = fullName ? this.FullName : this.Name;

            if(LibrarySettings.EnableAttrSymbols || forceAttr)
            {
                var strAttr = this.DetermineAttrStr();

                if (strAttr != string.Empty)
                    name += " " + strAttr;
            }

            return name;
        }

        public string ReferenceNameWAttrs(bool forceAttr = false)
        {
            var name = this.ReferenceName;

            if (LibrarySettings.EnableAttrSymbols || forceAttr)
            {
                var strAttr = this.DetermineAttrStr();

                if (strAttr != string.Empty)
                    name += " " + strAttr;
            }

            return name;
        }
        #endregion

        #region IParsed
        [JsonIgnore]
        public SourceTypes Source { get { return SourceTypes.CQL; } }
        [JsonConverter(typeof(IPathJsonConverter))]
        public IPath Path { get; private set; }
        [JsonIgnore]
        public Cluster Cluster { get { return this.Keyspace.Cluster; } }
        [JsonIgnore]
        public IDataCenter DataCenter { get { return this.Node?.DataCenter ?? this.Keyspace.DataCenter; } }
        public INode Node { get;  }
        public int Items { get;  }
        public uint LineNbr { get; }

        #endregion

        #region IDDLStmt
        public IKeyspace Keyspace { get;  }
        public string Name { get;  }
        [JsonIgnore]
        public string ReferenceName { get { return this.Name; } }
        [JsonIgnore]
        public string FullName { get { return this.Keyspace.Name + '.' + this.Name; } }
        public string DDL { get;  }

        [JsonProperty(PropertyName = "AggregatedStats")]
        private IEnumerable<IAggregatedStats> datamemberAggregatedStats
        {
            get { return this._aggregatedStats.UnSafe; }
            set { this._aggregatedStats = new CTS.List<IAggregatedStats>(value); }
        }

        private CTS.List<IAggregatedStats> _aggregatedStats = new CTS.List<IAggregatedStats>();
        [JsonIgnore]
        public IEnumerable<IAggregatedStats> AggregatedStats { get { return this._aggregatedStats; } }
        public IDDL AssociateItem(IAggregatedStats aggregatedStat)
        {
            this._aggregatedStats.Add(aggregatedStat);
            return this;
        }

        public bool? IsActive { get; private set; }
        public bool? SetAsActive(bool isActive = true)
        {
            if (!this.IsActive.HasValue || isActive != this.IsActive.Value)
            {
                if (isActive)
                {
                    ++this.Keyspace.Stats.NbrActive;
                }
                else if(this.IsActive.HasValue)
                {
                    --this.Keyspace.Stats.NbrActive;
                }
                this.IsActive = isActive;
            }
            return this.IsActive;
        }

        public virtual object ToDump()
        {
            return new { Table = this.FullName, Cluster = this.Cluster.Name, DataCenter = this.DataCenter.Name, Me = this };
        }
        public bool Equals(string other)
        {
            var kstblpair = StringHelpers.SplitTableName(other);

            if(kstblpair.Item1 != null)
            {
                if(!this.Keyspace.Equals(kstblpair.Item1))
                {
                    return false;
                }
            }

            return this.Name == kstblpair.Item2;
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

        [JsonProperty(PropertyName="HashCode")]
        private int _hashcode = 0;
        public override int GetHashCode()
        {
            unchecked
            {
                if (this._hashcode != 0) return this._hashcode;
                if (this.Keyspace.Cluster.IsMaster) return this.FullName.GetHashCode();

                return this._hashcode = this.Keyspace.GetHashCode() * 31 + (this.GetType().Name + '.' + this.Name).GetHashCode();
            }
        }

        public override string ToString()
        {
            return string.Format("CQLTable{{{0}}}", this.DDL);
        }
        #endregion

        public UnitOfMeasure AddToStorage(UnitOfMeasure size)
        {
            return this.Stats.Storage = this.Stats.Storage.Add(size);
        }
        public UnitOfMeasure AddToStorage(decimal size)
        {
            return this.Stats.Storage = this.Stats.Storage.Add(size);
        }
        public long AddToReadCount(long readCount)
        {
            return this.Stats.ReadCount += readCount;
        }
        public long AddToWriteCount(long writeCount)
        {
            return this.Stats.WriteCount += writeCount;
        }
        public long AddToKeyCount(long keyCount)
        {
            return this.Stats.KeyCount += keyCount;
        }
        public long AddToSSTableCount(long sstableCount)
        {
            return this.Stats.SSTableCount += sstableCount;
        }

        public static ICQLTable TryGet(IKeyspace ksInstance, string name)
        {
            return ksInstance.TryGetTable(name);
        }

        #region private members

        private void UpdateColumnStats(IEnumerable<ICQLColumn> columns)
        {
            this.Stats.NbrColumns += (uint) columns.Count();

            foreach (var column in columns)
            {
                
                if (column.IsStatic)
                {
                    ++this.Stats.Statics;
                }

                if (column.CQLType.IsFrozen)
                {
                    ++this.Stats.Frozens;
                }
                else if (column.CQLType.IsCollection)
                {
                    ++this.Stats.Collections;
                    this.UpdateColumnStats(column.CQLType.CQLSubType);
                }               
                else if (column.CQLType.IsTuple)
                {
                    ++this.Stats.Tuples;
                    this.UpdateColumnStats(column.CQLType.CQLSubType);
                }
                else if (column.CQLType.IsCounter)
                {
                    ++this.Stats.Counters;
                }
                else if (column.CQLType.IsBlob)
                {
                    ++this.Stats.Blobs;
                }
                else if (column.CQLType.IsUDT)
                {
                    ++this.Stats.UDTs;
                    if(column.UDT == null)
                    {
                        var udtInstance = CQLUserDefinedType.TryGet(column.CQLType.Name, this.Keyspace);

                        if (udtInstance == null)
                            this.UpdateColumnStats(column.CQLType.CQLSubType);
                        else
                            this.UpdateColumnStats(udtInstance.Columns);
                    }                        
                    else
                        this.UpdateColumnStats(column.UDT.Columns);
                }
                
            }
        }

        private void UpdateColumnStats(IEnumerable<CQLColumnType> columnTypes)
        {           
            foreach (var columnType in columnTypes)
            {
                if (columnType.IsFrozen)
                {
                    ++this.Stats.Frozens;
                }
                else if (columnType.IsCollection)
                {
                    ++this.Stats.Collections;
                    this.UpdateColumnStats(columnType.CQLSubType);
                }                
                else if (columnType.IsTuple)
                {
                    ++this.Stats.Tuples;
                    this.UpdateColumnStats(columnType.CQLSubType);
                }
                else if (columnType.IsCounter)
                {
                    ++this.Stats.Counters;
                }
                else if (columnType.IsBlob)
                {
                    ++this.Stats.Blobs;
                }
                else if (columnType.IsUDT)
                {
                    ++this.Stats.UDTs;
                    this.UpdateColumnStats(columnType.CQLSubType);
                }                
            }
        }

        #endregion
    }
}
