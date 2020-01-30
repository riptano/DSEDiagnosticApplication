using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using Common;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;

namespace DSEDiagnosticToDataTable
{    
     public sealed class PFTombstonesDataTable : DataTableLoad
     {
        public PFTombstonesDataTable(DSEDiagnosticLibrary.Cluster cluster,
                                            DataTable taggedDCDataTable,
                                            CancellationTokenSource cancellationSource = null,
                                            Guid? sessionId = null)
            : base(cluster, cancellationSource, taggedDCDataTable, sessionId)
        {            
        }
        
        public override DataTable CreateInitializationTable()
        {
            var dtPF = new DataTable(TableNames.PFTombstonesTable, this.SourceTable.Namespace);

            if (this.SessionId.HasValue)
                dtPF.Columns.Add(ColumnNames.SessionId, typeof(Guid));

            dtPF.Columns.Add(TaggedDCDataTable.Columns.KSCSType, typeof(string)).AllowDBNull = true;
            dtPF.Columns.Add(ColumnNames.Table, typeof(string)).AllowDBNull = true;

            TaggedDCDataTable.AddDCColumns(dtPF, this.SourceTable);

            dtPF.Columns.Add(TaggedItemsDataTable.Columns.TombstoneRatioMax, typeof(decimal))
                                .AllowDBNull();
            dtPF.Columns.Add(TaggedItemsDataTable.Columns.TombstoneRatioMin, typeof(decimal))
                                .AllowDBNull();
            dtPF.Columns.Add(TaggedItemsDataTable.Columns.TombstoneRatioAvg, typeof(decimal))
                                .AllowDBNull();
            dtPF.Columns.Add(TaggedItemsDataTable.Columns.TombstonesRead, typeof(long))
                                .AllowDBNull();
            dtPF.Columns.Add(TaggedItemsDataTable.Columns.LiveRead, typeof(long))
                                .AllowDBNull();

            dtPF.Columns.Add(TaggedItemsDataTable.Columns.KeysPercent, typeof(decimal))
                                .AllowDBNull();
            dtPF.Columns.Add(TaggedItemsDataTable.Columns.StoragePercent, typeof(decimal))
                                .AllowDBNull();

            dtPF.Columns.Add(TaggedItemsDataTable.Columns.ReadFactor, typeof(decimal))
                                .AllowDBNull();
            dtPF.Columns.Add(TaggedItemsDataTable.Columns.ReadPercent, typeof(decimal))
                                .AllowDBNull();


            dtPF.Columns.Add(TaggedItemsDataTable.Columns.WriteFactor, typeof(decimal))
                                .AllowDBNull();
            dtPF.Columns.Add(TaggedItemsDataTable.Columns.WritePercent, typeof(decimal))
                                .AllowDBNull();
            dtPF.Columns.Add(TaggedItemsDataTable.Columns.SSTablesFactor, typeof(decimal))
                                .AllowDBNull();

            dtPF.Columns.Add(TaggedItemsDataTable.Columns.PartitionSizeFactor, typeof(decimal))
                                .AllowDBNull();

            dtPF.Columns.Add(TaggedItemsDataTable.Columns.SecondaryIndexes, typeof(int))
                                .AllowDBNull();
            dtPF.Columns.Add(TaggedItemsDataTable.Columns.MaterializedViews, typeof(int))
                                .AllowDBNull();

            dtPF.Columns.Add(TaggedItemsDataTable.Columns.CommonKey, typeof(string))
                                .AllowDBNull();

            dtPF.Columns.Add(TaggedItemsDataTable.Columns.CommonKeyFactor, typeof(decimal))
                                .AllowDBNull();

            dtPF.Columns.Add(TaggedDCDataTable.Columns.BaseTableFactor, typeof(decimal))
                                .AllowDBNull();
            
            return dtPF;
        }

        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        /// <exception cref="Exception">Should re-thrown any exception except for OperationCanceledException</exception>
        public override DataTable LoadTable()
        {

            this.Table.BeginLoadData();
            try
            {
                int nbrItems = 0;
                Logger.Instance.InfoFormat("Loading {0}", this.Table.TableName);

                this.CancellationToken.ThrowIfCancellationRequested();

                decimal? WeightAvg(IEnumerable<DataRow> dataRows, string columnName)
                {
                    var factorValues = dataRows.Where(r => !r.IsNull(columnName)).Select(r => r.Field<decimal>(columnName)).ToArray();

                    if (factorValues.Length == 0) return null;

                    var aggregate = factorValues.Sum();
                    var maxFactor = factorValues.Max();

                    return (aggregate + maxFactor) / ((decimal)factorValues.Length + 1m);
                }

                decimal? Sum(IEnumerable<DataRow> dataRows, string columnName)
                {
                    var factorValue = dataRows.Where(r => !r.IsNull(columnName))
                                                .Select(r => r.Field<decimal>(columnName))
                                                .DefaultIfEmpty()
                                                .Sum();

                    if (factorValue == 0) return null;

                    return factorValue;
                }

                long? SumL(IEnumerable<DataRow> dataRows, string columnName)
                {
                    var factorValue = dataRows.Where(r => !r.IsNull(columnName))
                                                .Select(r => r.Field<long>(columnName))
                                                .DefaultIfEmpty()
                                                .Sum();

                    if (factorValue == 0) return null;

                    return factorValue;
                }

                var taggedItems = from dataRow in this.SourceTable.AsEnumerable()
                                  where !dataRow.IsNull(TaggedItemsDataTable.Columns.TombstoneRatioFactor)
                                  let keySpace = dataRow.Field<string>(ColumnNames.KeySpace)
                                  let compactionType = dataRow.Field<string>(TaggedItemsDataTable.Columns.CompactionStrategy)
                                  let cqlType = dataRow.Field<string>(TaggedItemsDataTable.Columns.CQLType)
                                  let tableName = dataRow.Field<string>(ColumnNames.Table)
                                  group dataRow by new { keySpace, compactionType, cqlType, tableName } into grp
                                  let dcfactors = grp.Select(i => new
                                  {
                                      DC = i.Field<string>(ColumnNames.DataCenter),
                                      Factor = i.Field<decimal>(TaggedItemsDataTable.Columns.TombstoneRatioFactor)
                                  })
                                  where dcfactors.Any(i => i.Factor >= Properties.Settings.Default.TriggerTombstoneRatioFactor)
                                  orderby grp.Key.keySpace ascending, grp.Key.compactionType ascending, grp.Key.cqlType ascending, grp.Key.tableName ascending
                                  select new
                                  {
                                      Keyspace = grp.Key.keySpace,
                                      Compaction = grp.Key.compactionType,
                                      CQLType = grp.Key.cqlType,
                                      Table = grp.Key.tableName,
                                      DCFactor = dcfactors,
                                      TRMax = grp.Max(r => r.Field<decimal>(TaggedItemsDataTable.Columns.TombstoneRatioMax)),
                                      TRMin = grp.Where(r => !r.IsNull(TaggedItemsDataTable.Columns.TombstoneRatioMin))
                                                    .ActionIfNotEmpty(i => i.Min(r => r.Field<decimal>(TaggedItemsDataTable.Columns.TombstoneRatioMin)), default(decimal?)),
                                      TRAvg = grp.Where(r => !r.IsNull(TaggedItemsDataTable.Columns.TombstoneRatioAvg))
                                                    .ActionIfNotEmpty(i => i.Min(r => r.Field<decimal>(TaggedItemsDataTable.Columns.TombstoneRatioAvg)), default(decimal?)),
                                      Tombstones = SumL(grp, TaggedItemsDataTable.Columns.TombstonesRead),
                                      LiveRead = SumL(grp, TaggedItemsDataTable.Columns.LiveRead),
                                      ReadFactor = WeightAvg(grp, TaggedItemsDataTable.Columns.ReadFactor),
                                      ReadPercent = Sum(grp, TaggedItemsDataTable.Columns.ReadPercent),
                                      KeyPercent = Sum(grp, TaggedItemsDataTable.Columns.KeysPercent),
                                      WriteFactor = WeightAvg(grp, TaggedItemsDataTable.Columns.WriteFactor),
                                      WritePercent = Sum(grp, TaggedItemsDataTable.Columns.WritePercent),
                                      StoragePercent = Sum(grp, TaggedItemsDataTable.Columns.StoragePercent),
                                      SSTableFactor = WeightAvg(grp, TaggedItemsDataTable.Columns.SSTablesFactor),
                                      PartitionFactor = WeightAvg(grp, TaggedItemsDataTable.Columns.PartitionSizeFactor),
                                      SecondaryIndexes = grp.First().Field<int?>(TaggedItemsDataTable.Columns.SecondaryIndexes),
                                      Views = grp.First().Field<int?>(TaggedItemsDataTable.Columns.MaterializedViews),
                                      CommonKey = grp.First().Field<string>(TaggedItemsDataTable.Columns.CommonKey),
                                      CommonKeyFactor = WeightAvg(grp, TaggedItemsDataTable.Columns.CommonKeyFactor),
                                      BaseTableFactor = WeightAvg(grp, TaggedItemsDataTable.Columns.BaseTableWeightedFactorMax)
                                  };

                DataRow newDataRow;
                string lstKeySpace = null;
                string lstCompaction = null;
                string lstType = null;

                foreach (var factors in taggedItems)
                {
                    this.CancellationToken.ThrowIfCancellationRequested();

                    newDataRow = this.Table.NewRow();

                    if (lstKeySpace != factors.Keyspace)
                    {
                        var ksRow = this.Table.NewRow();
                        ksRow.SetField(TaggedDCDataTable.Columns.KSCSType, lstKeySpace = factors.Keyspace);
                        this.Table.Rows.Add(ksRow);
                        lstCompaction = lstType = null;
                    }
                    if (lstCompaction != factors.Compaction)
                    {
                        var csRow = this.Table.NewRow();
                        csRow.SetField(TaggedDCDataTable.Columns.KSCSType, "   " + (lstCompaction = factors.Compaction));
                        this.Table.Rows.Add(csRow);
                        lstType = null;
                    }
                    if (lstType != factors.CQLType)
                    {
                        var csRow = this.Table.NewRow();
                        csRow.SetField(TaggedDCDataTable.Columns.KSCSType, "      " + (lstType = factors.CQLType));
                        this.Table.Rows.Add(csRow);
                    }

                    newDataRow.SetField(DSEDiagnosticToDataTable.ColumnNames.Table, factors.Table);

                    newDataRow.SetField(TaggedItemsDataTable.Columns.TombstoneRatioMax, factors.TRMax);
                    newDataRow.SetField(TaggedItemsDataTable.Columns.TombstoneRatioMin, factors.TRMin);
                    newDataRow.SetField(TaggedItemsDataTable.Columns.TombstoneRatioAvg, factors.TRAvg);
                    newDataRow.SetField(TaggedItemsDataTable.Columns.TombstonesRead, factors.Tombstones);
                    newDataRow.SetField(TaggedItemsDataTable.Columns.LiveRead, factors.LiveRead);

                    foreach (var dcFactor in factors.DCFactor)
                    {
                        newDataRow.SetField(dcFactor.DC, dcFactor.Factor);
                    }

                    newDataRow.SetField(TaggedItemsDataTable.Columns.ReadFactor, factors.ReadFactor);
                    newDataRow.SetField(TaggedItemsDataTable.Columns.ReadPercent, factors.ReadPercent);

                    newDataRow.SetField(TaggedItemsDataTable.Columns.KeysPercent, factors.KeyPercent);

                    newDataRow.SetField(TaggedItemsDataTable.Columns.WriteFactor, factors.WriteFactor);
                    newDataRow.SetField(TaggedItemsDataTable.Columns.WritePercent, factors.WritePercent);

                    newDataRow.SetField(TaggedItemsDataTable.Columns.StoragePercent, factors.StoragePercent);

                    newDataRow.SetField(TaggedItemsDataTable.Columns.SSTablesFactor, factors.SSTableFactor);

                    newDataRow.SetField(TaggedItemsDataTable.Columns.PartitionSizeFactor, factors.PartitionFactor);

                    newDataRow.SetField(TaggedItemsDataTable.Columns.SecondaryIndexes, factors.SecondaryIndexes);
                    newDataRow.SetField(TaggedItemsDataTable.Columns.MaterializedViews, factors.Views);

                    newDataRow.SetField(TaggedItemsDataTable.Columns.CommonKey, factors.CommonKey);
                    newDataRow.SetField(TaggedItemsDataTable.Columns.CommonKeyFactor, factors.CommonKeyFactor);

                    newDataRow.SetField(TaggedDCDataTable.Columns.BaseTableFactor, factors.BaseTableFactor);

                    ++nbrItems;
                    this.Table.Rows.Add(newDataRow);
                }                

                Logger.Instance.InfoFormat("Loaded {0}, Total Nbr Items {0:###,###,##0}", nbrItems, this.Table.TableName);
            }
            catch (OperationCanceledException)
            {
                Logger.Instance.Warn(string.Format("Loading {0} Canceled", this.Table.TableName));
            }
            finally
            {
                this.Table.AcceptChanges();
                this.Table.EndLoadData();
            }

            return this.Table;
        }
    }
}
