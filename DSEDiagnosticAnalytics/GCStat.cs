using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DSEDiagnosticLibrary;
using Common;
using CPC = Common.Patterns.Collections;
using CTS = Common.Patterns.Collections.ThreadSafe;

namespace DSEDiagnosticAnalytics
{
    public sealed class GCStat
    {
        public enum GCStatTypes
        {
            None = 0,
            BackToBack,
            TimeFrame
        }

        public GCStat(INode node, GCStatTypes type)
        {
            this.Node = node;
            this.StatType = type;
        }
        
        public INode Node { get; }
        public GCStatTypes StatType { get; }
        public DateTimeOffset GCTimeFrame { get; private set; } = DateTimeOffset.MaxValue;
        public DateTimeOffset GCTimeFrameEnd { get; private set; } = DateTimeOffset.MinValue;

        private List<ILogEvent> _logEvents = new List<ILogEvent>();
        public IEnumerable<ILogEvent> LogEvents
        {
            get { return this._logEvents; }
        }

        public bool TestAddEvent(ILogEvent logEvent)
        {
            bool bResult = false;

            if (this.StatType == GCStatTypes.BackToBack && LibrarySettings.GCBackToBackToleranceMS > 0)
                bResult = this.TestAddEventBackToBack(logEvent);
            else if (this.StatType == GCStatTypes.TimeFrame && LibrarySettings.GCTimeFrameDetection > TimeSpan.Zero)
                bResult |= this.TestAddEventTimeFrame(logEvent);

            return bResult;
        }

        public bool TestAddEventBackToBack(ILogEvent logEvent)
        {
            bool bResult = false;

            if(logEvent != null && this.GCTimeFrame < logEvent.EventTimeBegin.Value && logEvent.EventTimeBegin.Value <= this.GCTimeFrameEnd) 
            {
                this.GCTimeFrame = logEvent.EventTimeBegin.Value;
                this.GCTimeFrameEnd = logEvent.EventTimeEnd.Value.AddMilliseconds(LibrarySettings.GCBackToBackToleranceMS);
                this._logEvents.Add(logEvent);                
            }
            else
            {
                if(this._logEvents.Count > 1)
                {
                    var firstMsg = this._logEvents.First();
                    var logProperties = new Dictionary<string, object>();

                    logProperties.Add("occurrences", this._logEvents.Count);
                    logProperties.Add("durationmax", this._logEvents.Max(l => (long) l.Duration.Value.TotalMilliseconds));
                    logProperties.Add("durationmin", this._logEvents.Min(l => (long)l.Duration.Value.TotalMilliseconds));
                    logProperties.Add("durationmean", (decimal) this._logEvents.Average(l => (long)l.Duration.Value.TotalMilliseconds));
                    logProperties.Add("durationstddev", (decimal)this._logEvents.Select(l => (long)l.Duration.Value.TotalMilliseconds).StandardDeviationP());

                    var statLogEvent = new LogCassandraEvent((IFilePath) firstMsg.Path,
                                                                this.Node,                                                                
                                                                EventClasses.Node | EventClasses.GCStats,
                                                                firstMsg.EventTimeLocal,
                                                                firstMsg.EventTimeBegin.Value,
                                                                this._logEvents.Sum(l => l.Duration.Value.TotalMilliseconds),                                                                                                                              
                                                                EventTypes.SessionSpan | EventTypes.AggregateDataDerived,
                                                                string.Format("{0} Back-To-Back GCs Detected", this._logEvents.Count),
                                                                logEndTimewOffset: this._logEvents.Last().EventTimeEnd.Value,
                                                                logProperties: logProperties,
                                                                subClass: this.StatType.ToString(),
                                                                timeZone: firstMsg.TimeZone,
                                                                product: firstMsg.Product,
                                                                analyticsGroup: firstMsg.AnalyticsGroup
                                                                );
                    this.Node.AssociateItem(statLogEvent);
                    bResult = true;
                }

                this._logEvents.Clear();

                if (logEvent != null)
                {                    
                    this.GCTimeFrame = logEvent.EventTimeBegin.Value;
                    this.GCTimeFrameEnd = logEvent.EventTimeEnd.Value.AddMilliseconds(LibrarySettings.GCBackToBackToleranceMS);
                    this._logEvents.Add(logEvent);
                }
            }

            return bResult;
        }

        public bool TestAddEventTimeFrame(ILogEvent logEvent)
        {
            bool bResult = false;

            if (logEvent != null && this.GCTimeFrame < logEvent.EventTimeBegin.Value && logEvent.EventTimeBegin.Value <= this.GCTimeFrameEnd)
            {
                this._logEvents.Add(logEvent);                
            }
            else
            {
                if (this._logEvents.Count > 1)
                {
                    var totalDuration = this._logEvents.Sum(l => l.Duration.Value.TotalMinutes);

                    if(totalDuration >= LibrarySettings.GCTimeFrameDetectionThreholdMin)
                    {
                        var firstMsg = this._logEvents.First();
                        var logProperties = new Dictionary<string, object>();

                        logProperties.Add("occurrences", this._logEvents.Count);
                        logProperties.Add("durationmax", this._logEvents.Max(l => (long)l.Duration.Value.TotalMilliseconds));
                        logProperties.Add("durationmin", this._logEvents.Min(l => (long)l.Duration.Value.TotalMilliseconds));
                        logProperties.Add("durationmean", (decimal)this._logEvents.Average(l => (long)l.Duration.Value.TotalMilliseconds));
                        logProperties.Add("durationstddev", (decimal)this._logEvents.Select(l => (long)l.Duration.Value.TotalMilliseconds).StandardDeviationP());

                        var statLogEvent = new LogCassandraEvent((IFilePath)firstMsg.Path,
                                                                 this.Node,
                                                                 EventClasses.Node | EventClasses.GCStats,
                                                                 firstMsg.EventTimeLocal,
                                                                 firstMsg.EventTimeBegin.Value,
                                                                 this._logEvents.Sum(l => l.Duration.Value.TotalMilliseconds),
                                                                 EventTypes.SessionSpan | EventTypes.AggregateDataDerived,
                                                                 string.Format("{0} TimeFrame GCs Detected", this._logEvents.Count),
                                                                 logEndTimewOffset: this._logEvents.Last().EventTimeEnd.Value,
                                                                 logProperties: logProperties,
                                                                 subClass: this.StatType.ToString(),
                                                                 timeZone: firstMsg.TimeZone,
                                                                 product: firstMsg.Product,
                                                                 analyticsGroup: firstMsg.AnalyticsGroup
                                                                 );

                        this.Node.AssociateItem(statLogEvent);
                        bResult = true;
                    }                                        
                }

                this._logEvents.Clear();

               if (logEvent != null)
               {                    
                    this.GCTimeFrame = logEvent.EventTimeBegin.Value;
                    this.GCTimeFrameEnd = logEvent.EventTimeEnd.Value.Add(LibrarySettings.GCTimeFrameDetection);
                    this._logEvents.Add(logEvent);
                }
            }

            return bResult;
        }
    }
}
