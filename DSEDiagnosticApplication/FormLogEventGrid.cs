using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using IMMLogValue = Common.Patterns.Collections.MemoryMapped.IMMValue<DSEDiagnosticLibrary.ILogEvent>;

namespace DSEDiagnosticApplication
{
    public partial class FormLogEventGrid : Form
    {
        readonly IEnumerable<IMMLogValue> _logMMValues;

        public FormLogEventGrid()
        {
            InitializeComponent();
        }

        public FormLogEventGrid(Task<IEnumerable<IMMLogValue>> logEvents)
        {
            InitializeComponent();
            this._logMMValues = logEvents.Result;

            this.ultraDataSourceLogEvents.Rows.SetCount(this._logMMValues.Count());
        }

        #region Datasource

        int _cachedRowIndex = -1;
        DSEDiagnosticLibrary.ILogEvent _cachedLogEvent = null;

        private DSEDiagnosticLibrary.ILogEvent SetLogEventCache(DSEDiagnosticLibrary.ILogEvent logEvent, int rowIndex)
        {
            this._cachedLogEvent = logEvent;
            this._cachedRowIndex = rowIndex;
            return logEvent;
        }

        private DSEDiagnosticLibrary.ILogEvent GetLogEventCache(int rowIndex)
        {
           return this._cachedRowIndex == rowIndex && this._cachedLogEvent != null
                                    ? this._cachedLogEvent
                                    : this.SetLogEventCache(this._logMMValues.ElementAtOrDefault(rowIndex)?.GetValue(), rowIndex);
        }

        private void ultraDataSourceLogEvents_CellDataRequested(object sender, Infragistics.Win.UltraWinDataSource.CellDataRequestedEventArgs e)
        {
            e.CacheData = false;

            if (e.Row.Band.ParentBand == null)
            {
                #region Top Band
                var logValue = this.GetLogEventCache(e.Row.Index);

                if (logValue != null)
                {
                    switch (e.Column.Key)
                    {
                        case "Source":
                            e.Data = logValue.Source.ToString();
                            break;
                        case "Path":
                            e.Data = logValue.Path?.PathResolved;
                            break;
                        case "Cluster":
                            e.Data = logValue.Cluster?.Name;
                            break;
                        case "DataCenter":
                            e.Data = logValue.DataCenter?.Name;
                            break;
                        case "Node":
                            e.Data = logValue.Node?.Id.NodeName();
                            break;
                        case "Items":
                            e.Data = logValue.Items;
                            break;
                        case "LineNbr":
                            e.Data = logValue.LineNbr;
                            break;
                        case "Class":
                            e.Data = logValue.Class.ToString();
                            break;
                        case "SubClass":
                            e.Data = logValue.SubClass;
                            break;
                        case "Id":
                            e.Data = logValue.Id;
                            break;
                        case "Timezone":
                            e.Data = logValue.TimeZone?.Name;
                            break;
                        case "EventTime":
                            e.Data = logValue.EventTime.DateTime;
                            break;
                        case "EventTimeOffset":
                            e.Data = (decimal)logValue.EventTime.Offset.TotalHours;
                            break;
                        case "LocalEventTime":
                            e.Data = logValue.EventTimeLocal;
                            break;
                        case "SessionTieOutId":
                            e.Data = logValue.SessionTieOutId;
                            break;
                        case "BeginSessionTime":
                            e.Data = logValue.EventTimeBegin.HasValue ? (object)logValue.EventTimeBegin.Value.DateTime : null;
                            break;
                        case "EndSessionTime":
                            e.Data = logValue.EventTimeEnd.HasValue ? (object)logValue.EventTimeEnd.Value.DateTime : null;
                            break;
                        case "SessionDuration":
                            e.Data = logValue.Duration.HasValue ? (object)((decimal)logValue.Duration.Value.TotalSeconds) : null;
                            break;
                        case "DSEProduct":
                            e.Data = logValue.Product.ToString();
                            break;
                        case "Keyspace":
                            e.Data = logValue.Keyspace?.Name;
                            break;
                        case "TableViewIndex":
                            e.Data = logValue.TableViewIndex?.FullName;
                            break;
                        case "Exception":
                            e.Data = logValue.Exception;
                            break;
                        case "LogMessage":
                            e.Data = logValue.LogMessage;
                            break;
                        case "Type":
                            e.Data = logValue.Type;
                            break;
                        default:
                            break;
                    }
                }
                #endregion
            }
            else if (e.Row.Band.Key == "BandSSTables")
            {
                var logValue = this.GetLogEventCache(e.Row.ParentRow.Index);

                e.Data = logValue?.SSTables.ElementAtOrDefault(e.Row.Index);
            }
            else if (e.Row.Band.Key == "BandDDLItems")
            {
                var logValue = this.GetLogEventCache(e.Row.ParentRow.Index);

                e.Data = logValue?.DDLItems.ElementAtOrDefault(e.Row.Index)?.FullName;
            }
            else if (e.Row.Band.Key == "BandNodes")
            {
                var logValue = this.GetLogEventCache(e.Row.ParentRow.Index);

                e.Data = logValue?.AssociatedNodes.ElementAtOrDefault(e.Row.Index)?.Id.NodeName();
            }
            else if (e.Row.Band.Key == "BandTokenRanges")
            {
                var logValue = this.GetLogEventCache(e.Row.ParentRow.Index);
                var tokenRange = logValue?.TokenRanges.ElementAtOrDefault(e.Row.Index);

                if (tokenRange != null)
                {
                    switch (e.Column.Key)
                    {
                        case "Start":
                            e.Data = tokenRange.StartRange;
                            break;
                        case "End":
                            e.Data = tokenRange.EndRange;
                            break;
                        case "Load":
                            e.Data = tokenRange.Load.NormalizedValue.Value;
                            break;
                        case "Wraps":
                            e.Data = tokenRange.WrapsRange;
                            break;
                        case "Slots":
                            e.Data = tokenRange.Slots;
                            break;
                        default:
                            break;
                    }
                }
            }
            else if (e.Row.Band.Key == "BandExceptionPaths")
            {
                var logValue = this.GetLogEventCache(e.Row.ParentRow.Index);

                e.Data = logValue?.ExceptionPath.ElementAtOrDefault(e.Row.Index);
            }
        }


        #endregion

        #region Grid Events

        #endregion

        private void FormLogEventGrid_Load(object sender, EventArgs e)
        {
            if (this.ultraDataSourceLogEvents.Rows.Count > 0)
            {
                var firstEvent = this._logMMValues.First().GetValue();
                var lastEvent = this._logMMValues.Last().GetValue();

                #region text Editors
                this.ultraTextEditorClusterName.Value = firstEvent.Cluster?.Name;
                this.ultraTextEditorDCName.Value = firstEvent.DataCenter?.Name;
                this.ultraTextEditorFilePath.Value = firstEvent.Path?.Path;
                this.ultraTextEditorNode.Value = firstEvent.Node?.Id.NodeName();
                this.ultraTextEditorTimeZone.Value = firstEvent.TimeZone?.Name;
                this.ultraTextEditorTZOffset.Value = string.Format("{0:zzz}", firstEvent.EventTime);
                #endregion

                #region Grantt View
                // Create a new Project other than the Unassigned Project
                var logeventProject = this.ultraCalendarInfoLogEvents.Projects.Add("Log Events", firstEvent.EventTime.DateTime);
                logeventProject.Key = "LogEventsProject";

                this.ultraGanttView1.GridSettings.ColumnSettings[Infragistics.Win.UltraWinSchedule.TaskField.Name].Visible = Infragistics.Win.DefaultableBoolean.False;
                this.ultraGanttView1.GridSettings.ColumnSettings[Infragistics.Win.UltraWinSchedule.TaskField.Dependencies].Visible = Infragistics.Win.DefaultableBoolean.False;
                this.ultraGanttView1.GridSettings.ColumnSettings[Infragistics.Win.UltraWinSchedule.TaskField.Resources].Visible = Infragistics.Win.DefaultableBoolean.False;
                this.ultraGanttView1.GridSettings.ColumnSettings[Infragistics.Win.UltraWinSchedule.TaskField.StartDateTime].Format = "yyyy-MM-dd HH:mm:ss.fff";
                this.ultraGanttView1.GridSettings.ColumnSettings[Infragistics.Win.UltraWinSchedule.TaskField.EndDateTime].Format = "yyyy-MM-dd HH:mm:ss.fff";

                var logEventTask = this.ultraCalendarInfoLogEvents.Tasks.Add(firstEvent.EventTime.DateTime,
                                                                                lastEvent.EventTime - firstEvent.EventTime,
                                                                                string.Format("{0:MM-dd} to {1:MM-dd}", firstEvent.EventTime.Date, lastEvent.EventTime.Date),
                                                                                "LogEventsProject");

                this.ultraGanttView1.Project = this.ultraGanttView1.CalendarInfo.Projects[1];

                this.ultraGanttView1.PerformAutoSizeAllGridColumns();
                #endregion

                this.Text = string.Format("{0} Log Events from \"{1:MM-dd HH:mm zzz}\" to \"{2:MM-dd HH:mm zzz}\"",
                                            firstEvent.Node.Id.NodeName(),
                                            firstEvent.EventTime,
                                            lastEvent.EventTime);
            }
        }

        private void ultraGrid1_InitializeRowsCollection(object sender, Infragistics.Win.UltraWinGrid.InitializeRowsCollectionEventArgs e)
        {
            if (e.Rows.Count == 0 && e.Rows.ParentRow != null)
            {
                var parentDSRow = (Infragistics.Win.UltraWinDataSource.UltraDataRow)e.Rows.ParentRow.ListObject;
                var childCollection = parentDSRow.GetChildRows(e.Rows.Band.Key);
                var logValue = this.GetLogEventCache(parentDSRow.Index);

                if (e.Rows.Band.Key == "BandSSTables")
                {
                    childCollection.SetCount(logValue.SSTables?.Count() ?? 0);
                }
                else if (e.Rows.Band.Key == "BandDDLItems")
                {
                    childCollection.SetCount(logValue.DDLItems?.Count() ?? 0);
                }
                else if (e.Rows.Band.Key == "BandNodes")
                {
                    childCollection.SetCount(logValue.AssociatedNodes?.Count() ?? 0);
                }
                else if (e.Rows.Band.Key == "BandTokenRanges")
                {
                    childCollection.SetCount(logValue.TokenRanges?.Count() ?? 0);
                }
                else if (e.Rows.Band.Key == "BandExceptionPaths")
                {
                    childCollection.SetCount(logValue.ExceptionPath?.Count() ?? 0);
                }
            }
        }
    }
}
