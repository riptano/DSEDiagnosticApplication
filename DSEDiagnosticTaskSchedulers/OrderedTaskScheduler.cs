using System;
using System.Collections.ObjectModel;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

namespace Schedulers
{
    /// <summary> 
    /// Provides a task scheduler that ensures only one task is executing at a time, and that tasks 
    /// execute in the order that they were queued. 
    /// </summary> 
    public sealed class OrderedTaskScheduler : LimitedConcurrencyLevelTaskScheduler
    {
        /// <summary>Initializes an instance of the OrderedTaskScheduler class.</summary> 
        public OrderedTaskScheduler() : base(1) { }
    }

}
