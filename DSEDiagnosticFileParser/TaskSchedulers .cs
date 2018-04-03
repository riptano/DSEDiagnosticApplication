//-------------------------------------------------------------------------- 
//  
//  Copyright (c) Microsoft Corporation.  All rights reserved.  
//  
//  File: LimitedConcurrencyTaskScheduler.cs 
// 
//-------------------------------------------------------------------------- 

using System;
using System.Collections.ObjectModel;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

namespace DSEDiagnosticFileParser.Tasks.Schedulers
{
    public static class LazyExtensions
    {
        /// <summary>Forces value creation of a Lazy instance.</summary> 
        /// <typeparam name="T">Specifies the type of the value being lazily initialized.</typeparam> 
        /// <param name="lazy">The Lazy instance.</param> 
        /// <returns>The initialized Lazy instance.</returns> 
        public static Lazy<T> Force<T>(this Lazy<T> lazy)
        {
            var ignored = lazy.Value;
            return lazy;
        }

        /// <summary>Retrieves the value of a Lazy asynchronously.</summary> 
        /// <typeparam name="T">Specifies the type of the value being lazily initialized.</typeparam> 
        /// <param name="lazy">The Lazy instance.</param> 
        /// <returns>A Task representing the Lazy's value.</returns> 
        public static Task<T> GetValueAsync<T>(this Lazy<T> lazy)
        {
            return Task.Factory.StartNew(() => lazy.Value);
        }

        /// <summary>Creates a Lazy that's already been initialized to a specified value.</summary> 
        /// <typeparam name="T">The type of the data to be initialized.</typeparam> 
        /// <param name="value">The value with which to initialize the Lazy instance.</param> 
        /// <returns>The initialized Lazy.</returns> 
        public static Lazy<T> Create<T>(T value)
        {
            return new Lazy<T>(() => value, false).Force();
        }
    }

    /// <summary> 
    /// Provides a task scheduler that ensures a maximum concurrency level while 
    /// running on top of the ThreadPool. 
    /// </summary> 
    public class LimitedConcurrencyLevelTaskScheduler : TaskScheduler
    {
        /// <summary>Whether the current thread is processing work items.</summary> 
        [ThreadStatic]
        private static bool _currentThreadIsProcessingItems;
        /// <summary>The list of tasks to be executed.</summary> 
        private readonly LinkedList<Task> _tasks = new LinkedList<Task>(); // protected by lock(_tasks) 
        /// <summary>The maximum concurrency level allowed by this scheduler.</summary> 
        private readonly int _maxDegreeOfParallelism;
        /// <summary>Whether the scheduler is currently processing work items.</summary> 
        private int _delegatesQueuedOrRunning = 0; // protected by lock(_tasks) 

        /// <summary> 
        /// Initializes an instance of the LimitedConcurrencyLevelTaskScheduler class with the 
        /// specified degree of parallelism. 
        /// </summary> 
        /// <param name="maxDegreeOfParallelism">The maximum degree of parallelism provided by this scheduler.</param> 
        public LimitedConcurrencyLevelTaskScheduler(int maxDegreeOfParallelism)
        {
            if (maxDegreeOfParallelism < 1) throw new ArgumentOutOfRangeException("maxDegreeOfParallelism");
            _maxDegreeOfParallelism = maxDegreeOfParallelism;
        }

        /// <summary>Queues a task to the scheduler.</summary> 
        /// <param name="task">The task to be queued.</param> 
        protected sealed override void QueueTask(Task task)
        {
            // Add the task to the list of tasks to be processed.  If there aren't enough 
            // delegates currently queued or running to process tasks, schedule another. 
            lock (_tasks)
            {
                _tasks.AddLast(task);
                if (_delegatesQueuedOrRunning < _maxDegreeOfParallelism)
                {
                    ++_delegatesQueuedOrRunning;
                    NotifyThreadPoolOfPendingWork();
                }
            }
        }

        /// <summary> 
        /// Informs the ThreadPool that there's work to be executed for this scheduler. 
        /// </summary> 
        private void NotifyThreadPoolOfPendingWork()
        {
            ThreadPool.UnsafeQueueUserWorkItem(_ =>
            {
                // Note that the current thread is now processing work items. 
                // This is necessary to enable inlining of tasks into this thread. 
                _currentThreadIsProcessingItems = true;
                try
                {
                    // Process all available items in the queue. 
                    while (true)
                    {
                        Task item;
                        lock (_tasks)
                        {
                            // When there are no more items to be processed, 
                            // note that we're done processing, and get out. 
                            if (_tasks.Count == 0)
                            {
                                --_delegatesQueuedOrRunning;
                                break;
                            }

                            // Get the next item from the queue 
                            item = _tasks.First.Value;
                            _tasks.RemoveFirst();
                        }

                        // Execute the task we pulled out of the queue 
                        base.TryExecuteTask(item);
                    }
                }
                // We're done processing items on the current thread 
                finally { _currentThreadIsProcessingItems = false; }
            }, null);
        }

        /// <summary>Attempts to execute the specified task on the current thread.</summary> 
        /// <param name="task">The task to be executed.</param> 
        /// <param name="taskWasPreviouslyQueued"></param> 
        /// <returns>Whether the task could be executed on the current thread.</returns> 
        protected sealed override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            // If this thread isn't already processing a task, we don't support inlining 
            if (!_currentThreadIsProcessingItems) return false;

            // If the task was previously queued, remove it from the queue 
            if (taskWasPreviouslyQueued) TryDequeue(task);

            // Try to run the task. 
            return base.TryExecuteTask(task);
        }

        /// <summary>Attempts to remove a previously scheduled task from the scheduler.</summary> 
        /// <param name="task">The task to be removed.</param> 
        /// <returns>Whether the task could be found and removed.</returns> 
        protected sealed override bool TryDequeue(Task task)
        {
            lock (_tasks) return _tasks.Remove(task);
        }

        /// <summary>Gets the maximum concurrency level supported by this scheduler.</summary> 
        public sealed override int MaximumConcurrencyLevel { get { return _maxDegreeOfParallelism; } }

        /// <summary>Gets an enumerable of the tasks currently scheduled on this scheduler.</summary> 
        /// <returns>An enumerable of the tasks currently scheduled.</returns> 
        protected sealed override IEnumerable<Task> GetScheduledTasks()
        {
            bool lockTaken = false;
            try
            {
                Monitor.TryEnter(_tasks, ref lockTaken);
                if (lockTaken) return _tasks.ToArray();
                else throw new NotSupportedException();
            }
            finally
            {
                if (lockTaken) Monitor.Exit(_tasks);
            }
        }
    }

    /// <summary>Provides a task scheduler that dedicates a thread per task.</summary> 
    public sealed class ThreadPerTaskScheduler : TaskScheduler
    {       
        /// <summary>Gets the tasks currently scheduled to this scheduler.</summary> 
        /// <remarks>This will always return an empty enumerable, as tasks are launched as soon as they're queued.</remarks> 
        protected override IEnumerable<Task> GetScheduledTasks() { return Enumerable.Empty<Task>(); }

        /// <summary>Starts a new thread to process the provided task.</summary> 
        /// <param name="task">The task to be executed.</param> 
        protected override void QueueTask(Task task)
        {
            new Thread(() => TryExecuteTask(task)) { IsBackground = true }.Start();
        }

        /// <summary>Runs the provided task on the current thread.</summary> 
        /// <param name="task">The task to be executed.</param> 
        /// <param name="taskWasPreviouslyQueued">Ignored.</param> 
        /// <returns>Whether the task could be executed on the current thread.</returns> 
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            return TryExecuteTask(task);
        }
    }

    /// <summary>Provides a work-stealing scheduler.</summary> 
    public sealed class WorkStealingTaskScheduler : TaskScheduler, IDisposable
    {
        private readonly int m_concurrencyLevel;
        private readonly Queue<Task> m_queue = new Queue<Task>();
        private WorkStealingQueue<Task>[] m_wsQueues = new WorkStealingQueue<Task>[Environment.ProcessorCount];
        private Lazy<Thread[]> m_threads;
        private int m_threadsWaiting;
        private bool m_shutdown;
        [ThreadStatic]
        private static WorkStealingQueue<Task> m_wsq;

        /// <summary>Initializes a new instance of the WorkStealingTaskScheduler class.</summary> 
        /// <remarks>This constructors defaults to using twice as many threads as there are processors.</remarks> 
        public WorkStealingTaskScheduler() : this(Environment.ProcessorCount * 2) { }

        /// <summary>Initializes a new instance of the WorkStealingTaskScheduler class.</summary> 
        /// <param name="concurrencyLevel">The number of threads to use in the scheduler.</param> 
        public WorkStealingTaskScheduler(int concurrencyLevel)
        {
            // Store the concurrency level 
            if (concurrencyLevel <= 0) throw new ArgumentOutOfRangeException("concurrencyLevel");
            m_concurrencyLevel = concurrencyLevel;

            // Set up threads 
            m_threads = new Lazy<Thread[]>(() =>
            {
                var threads = new Thread[m_concurrencyLevel];
                for (int i = 0; i < threads.Length; i++)
                {
                    threads[i] = new Thread(DispatchLoop) { IsBackground = true };
                    threads[i].Start();
                }
                return threads;
            });
        }
       
        /// <summary>Queues a task to the scheduler.</summary> 
        /// <param name="task">The task to be scheduled.</param> 
        protected override void QueueTask(Task task)
        {            
            // Make sure the pool is started, e.g. that all threads have been created. 
            m_threads.Force();

            // If the task is marked as long-running, give it its own dedicated thread 
            // rather than queueing it. 
            if ((task.CreationOptions & TaskCreationOptions.LongRunning) != 0)
            {
                new Thread(state => base.TryExecuteTask((Task)state)) { IsBackground = true }.Start(task);
            }
            else
            {
                // Otherwise, insert the work item into a queue, possibly waking a thread. 
                // If there's a local queue and the task does not prefer to be in the global queue, 
                // add it to the local queue. 
                WorkStealingQueue<Task> wsq = m_wsq;
                if (wsq != null && ((task.CreationOptions & TaskCreationOptions.PreferFairness) == 0))
                {
                    // Add to the local queue and notify any waiting threads that work is available. 
                    // Races may occur which result in missed event notifications, but they're benign in that 
                    // this thread will eventually pick up the work item anyway, as will other threads when another 
                    // work item notification is received. 
                    wsq.LocalPush(task);
                    if (m_threadsWaiting > 0) // OK to read lock-free. 
                    {
                        lock (m_queue) { Monitor.Pulse(m_queue); }
                    }
                }
                // Otherwise, add the work item to the global queue 
                else
                {
                    lock (m_queue)
                    {
                        m_queue.Enqueue(task);
                        if (m_threadsWaiting > 0) Monitor.Pulse(m_queue);
                    }
                }
            }
        }

        /// <summary>Executes a task on the current thread.</summary> 
        /// <param name="task">The task to be executed.</param> 
        /// <param name="taskWasPreviouslyQueued">Ignored.</param> 
        /// <returns>Whether the task could be executed.</returns> 
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            return TryExecuteTask(task);

            // // Optional replacement: Instead of always trying to execute the task (which could 
            // // benignly leave a task in the queue that's already been executed), we 
            // // can search the current work-stealing queue and remove the task, 
            // // executing it inline only if it's found. 
            // WorkStealingQueue<Task> wsq = m_wsq; 
            // return wsq != null && wsq.TryFindAndPop(task) && TryExecuteTask(task); 
        }

        /// <summary>Gets the maximum concurrency level supported by this scheduler.</summary> 
        public override int MaximumConcurrencyLevel
        {
            get { return m_concurrencyLevel; }
        }

        /// <summary>Gets all of the tasks currently scheduled to this scheduler.</summary> 
        /// <returns>An enumerable containing all of the scheduled tasks.</returns> 
        protected override IEnumerable<Task> GetScheduledTasks()
        {
            // Keep track of all of the tasks we find 
            List<Task> tasks = new List<Task>();

            // Get all of the global tasks.  We use TryEnter so as not to hang 
            // a debugger if the lock is held by a frozen thread. 
            bool lockTaken = false;
            try
            {
                Monitor.TryEnter(m_queue, ref lockTaken);
                if (lockTaken) tasks.AddRange(m_queue.ToArray());
                else throw new NotSupportedException();
            }
            finally
            {
                if (lockTaken) Monitor.Exit(m_queue);
            }

            // Now get all of the tasks from the work-stealing queues 
            WorkStealingQueue<Task>[] queues = m_wsQueues;
            for (int i = 0; i < queues.Length; i++)
            {
                WorkStealingQueue<Task> wsq = queues[i];
                if (wsq != null) tasks.AddRange(wsq.ToArray());
            }

            // Return to the debugger all of the collected task instances 
            return tasks;
        }

        /// <summary>Adds a work-stealing queue to the set of queues.</summary> 
        /// <param name="wsq">The queue to be added.</param> 
        private void AddWsq(WorkStealingQueue<Task> wsq)
        {
            lock (m_wsQueues)
            {
                // Find the next open slot in the array. If we find one, 
                // store the queue and we're done. 
                int i;
                for (i = 0; i < m_wsQueues.Length; i++)
                {
                    if (m_wsQueues[i] == null)
                    {
                        m_wsQueues[i] = wsq;
                        return;
                    }
                }

                // We couldn't find an open slot, so double the length  
                // of the array by creating a new one, copying over, 
                // and storing the new one. Here, i == m_wsQueues.Length. 
                WorkStealingQueue<Task>[] queues = new WorkStealingQueue<Task>[i * 2];
                Array.Copy(m_wsQueues, queues, i);
                queues[i] = wsq;
                m_wsQueues = queues;
            }
        }

        /// <summary>Remove a work-stealing queue from the set of queues.</summary> 
        /// <param name="wsq">The work-stealing queue to remove.</param> 
        private void RemoveWsq(WorkStealingQueue<Task> wsq)
        {
            lock (m_wsQueues)
            {
                // Find the queue, and if/when we find it, null out its array slot 
                for (int i = 0; i < m_wsQueues.Length; i++)
                {
                    if (m_wsQueues[i] == wsq)
                    {
                        m_wsQueues[i] = null;
                    }
                }
            }
        }

        /// <summary> 
        /// The dispatch loop run by each thread in the scheduler. 
        /// </summary> 
        private void DispatchLoop()
        {
            // Create a new queue for this thread, store it in TLS for later retrieval, 
            // and add it to the set of queues for this scheduler. 
            WorkStealingQueue<Task> wsq = new WorkStealingQueue<Task>();
            m_wsq = wsq;
            AddWsq(wsq);

            try
            {
                // Until there's no more work to do... 
                while (true)
                {
                    Task wi = null;

                    // Search order: (1) local WSQ, (2) global Q, (3) steals from other queues. 
                    if (!wsq.LocalPop(ref wi))
                    {
                        // We weren't able to get a task from the local WSQ 
                        bool searchedForSteals = false;
                        while (true)
                        {
                            lock (m_queue)
                            {
                                // If shutdown was requested, exit the thread. 
                                if (m_shutdown)
                                    return;

                                // (2) try the global queue. 
                                if (m_queue.Count != 0)
                                {
                                    // We found a work item! Grab it ... 
                                    wi = m_queue.Dequeue();
                                    break;
                                }
                                else if (searchedForSteals)
                                {
                                    // Note that we're not waiting for work, and then wait 
                                    m_threadsWaiting++;
                                    try { Monitor.Wait(m_queue); }
                                    finally { m_threadsWaiting--; }

                                    // If we were signaled due to shutdown, exit the thread. 
                                    if (m_shutdown)
                                        return;

                                    searchedForSteals = false;
                                    continue;
                                }
                            }

                            // (3) try to steal. 
                            WorkStealingQueue<Task>[] wsQueues = m_wsQueues;
                            int i;
                            for (i = 0; i < wsQueues.Length; i++)
                            {
                                WorkStealingQueue<Task> q = wsQueues[i];
                                if (q != null && q != wsq && q.TrySteal(ref wi)) break;
                            }

                            if (i != wsQueues.Length) break;

                            searchedForSteals = true;
                        }
                    }

                    // ...and Invoke it. 
                    TryExecuteTask(wi);
                }
            }
            finally
            {
                RemoveWsq(wsq);
            }
        }

        /// <summary>Signal the scheduler to shutdown and wait for all threads to finish.</summary> 
        public void Dispose()
        {
            m_shutdown = true;
            if (m_queue != null && m_threads.IsValueCreated)
            {
                var threads = m_threads.Value;
                lock (m_queue) Monitor.PulseAll(m_queue);
                for (int i = 0; i < threads.Length; i++) threads[i].Join();
            }
        }
    }

    /// <summary>A work-stealing queue.</summary> 
    /// <typeparam name="T">Specifies the type of data stored in the queue.</typeparam> 
    internal sealed class WorkStealingQueue<T> where T : class
    {
        private const int INITIAL_SIZE = 32;
        private T[] m_array = new T[INITIAL_SIZE];
        private int m_mask = INITIAL_SIZE - 1;
        private volatile int m_headIndex = 0;
        private volatile int m_tailIndex = 0;

        private object m_foreignLock = new object();

        internal void LocalPush(T obj)
        {
            int tail = m_tailIndex;

            // When there are at least 2 elements' worth of space, we can take the fast path. 
            if (tail < m_headIndex + m_mask)
            {
                m_array[tail & m_mask] = obj;
                m_tailIndex = tail + 1;
            }
            else
            {
                // We need to contend with foreign pops, so we lock. 
                lock (m_foreignLock)
                {
                    int head = m_headIndex;
                    int count = m_tailIndex - m_headIndex;

                    // If there is still space (one left), just add the element. 
                    if (count >= m_mask)
                    {
                        // We're full; expand the queue by doubling its size. 
                        T[] newArray = new T[m_array.Length << 1];
                        for (int i = 0; i < m_array.Length; i++)
                            newArray[i] = m_array[(i + head) & m_mask];

                        // Reset the field values, incl. the mask. 
                        m_array = newArray;
                        m_headIndex = 0;
                        m_tailIndex = tail = count;
                        m_mask = (m_mask << 1) | 1;
                    }

                    m_array[tail & m_mask] = obj;
                    m_tailIndex = tail + 1;
                }
            }
        }

        internal bool LocalPop(ref T obj)
        {
            while (true)
            {
                // Decrement the tail using a fence to ensure subsequent read doesn't come before. 
                int tail = m_tailIndex;
                if (m_headIndex >= tail)
                {
                    obj = null;
                    return false;
                }

                tail -= 1;
#pragma warning disable 0420
                Interlocked.Exchange(ref m_tailIndex, tail);
#pragma warning restore 0420

                // If there is no interaction with a take, we can head down the fast path. 
                if (m_headIndex <= tail)
                {
                    int idx = tail & m_mask;
                    obj = m_array[idx];

                    // Check for nulls in the array. 
                    if (obj == null) continue;

                    m_array[idx] = null;
                    return true;
                }
                else
                {
                    // Interaction with takes: 0 or 1 elements left. 
                    lock (m_foreignLock)
                    {
                        if (m_headIndex <= tail)
                        {
                            // Element still available. Take it. 
                            int idx = tail & m_mask;
                            obj = m_array[idx];

                            // Check for nulls in the array. 
                            if (obj == null) continue;

                            m_array[idx] = null;
                            return true;
                        }
                        else
                        {
                            // We lost the race, element was stolen, restore the tail. 
                            m_tailIndex = tail + 1;
                            obj = null;
                            return false;
                        }
                    }
                }
            }
        }

        internal bool TrySteal(ref T obj)
        {
            obj = null;

            while (true)
            {
                if (m_headIndex >= m_tailIndex)
                    return false;

                lock (m_foreignLock)
                {
                    // Increment head, and ensure read of tail doesn't move before it (fence). 
                    int head = m_headIndex;
#pragma warning disable 0420
                    Interlocked.Exchange(ref m_headIndex, head + 1);
#pragma warning restore 0420

                    if (head < m_tailIndex)
                    {
                        int idx = head & m_mask;
                        obj = m_array[idx];

                        // Check for nulls in the array. 
                        if (obj == null) continue;

                        m_array[idx] = null;
                        return true;
                    }
                    else
                    {
                        // Failed, restore head. 
                        m_headIndex = head;
                        obj = null;
                    }
                }

                return false;
            }
        }

        internal bool TryFindAndPop(T obj)
        {
            // We do an O(N) search for the work item. The theory of work stealing and our 
            // inlining logic is that most waits will happen on recently queued work.  And 
            // since recently queued work will be close to the tail end (which is where we 
            // begin our search), we will likely find it quickly.  In the worst case, we 
            // will traverse the whole local queue; this is typically not going to be a 
            // problem (although degenerate cases are clearly an issue) because local work 
            // queues tend to be somewhat shallow in length, and because if we fail to find 
            // the work item, we are about to block anyway (which is very expensive). 

            for (int i = m_tailIndex - 1; i >= m_headIndex; i--)
            {
                if (m_array[i & m_mask] == obj)
                {
                    // If we found the element, block out steals to avoid interference. 
                    lock (m_foreignLock)
                    {
                        // If we lost the race, bail. 
                        if (m_array[i & m_mask] == null)
                        {
                            return false;
                        }

                        // Otherwise, null out the element. 
                        m_array[i & m_mask] = null;

                        // And then check to see if we can fix up the indexes (if we're at 
                        // the edge).  If we can't, we just leave nulls in the array and they'll 
                        // get filtered out eventually (but may lead to superflous resizing). 
                        if (i == m_tailIndex)
                            m_tailIndex -= 1;
                        else if (i == m_headIndex)
                            m_headIndex += 1;

                        return true;
                    }
                }
            }

            return false;
        }

        internal T[] ToArray()
        {
            List<T> list = new List<T>();
            for (int i = m_tailIndex - 1; i >= m_headIndex; i--)
            {
                T obj = m_array[i & m_mask];
                if (obj != null) list.Add(obj);
            }
            return list.ToArray();
        }
    }

    /// <summary>Provides a task scheduler that runs tasks on the current thread.</summary> 
    public sealed class CurrentThreadTaskScheduler : TaskScheduler
    {
        /// <summary>Runs the provided Task synchronously on the current thread.</summary> 
        /// <param name="task">The task to be executed.</param> 
        protected override void QueueTask(Task task)
        {
            TryExecuteTask(task);
        }

        /// <summary>Runs the provided Task synchronously on the current thread.</summary> 
        /// <param name="task">The task to be executed.</param> 
        /// <param name="taskWasPreviouslyQueued">Whether the Task was previously queued to the scheduler.</param> 
        /// <returns>True if the Task was successfully executed; otherwise, false.</returns> 
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            return TryExecuteTask(task);
        }

        /// <summary>Gets the Tasks currently scheduled to this scheduler.</summary> 
        /// <returns>An empty enumerable, as Tasks are never queued, only executed.</returns> 
        protected override IEnumerable<Task> GetScheduledTasks()
        {
            return Enumerable.Empty<Task>();
        }

        /// <summary>Gets the maximum degree of parallelism for this scheduler.</summary> 
        public override int MaximumConcurrencyLevel { get { return 1; } }
    }

    /// <summary> 
    /// Provides a task scheduler that ensures only one task is executing at a time, and that tasks 
    /// execute in the order that they were queued. 
    /// </summary> 
    public sealed class OrderedTaskScheduler : LimitedConcurrencyLevelTaskScheduler
    {
        /// <summary>Initializes an instance of the OrderedTaskScheduler class.</summary> 
        public OrderedTaskScheduler() : base(1) { }
    }

    /// <summary>Enables the creation of a group of schedulers that support round-robin scheduling for fairness.</summary> 
    public sealed class RoundRobinSchedulerGroup
    {
        private readonly List<RoundRobinTaskSchedulerQueue> _queues = new List<RoundRobinTaskSchedulerQueue>();
        private int _nextQueue = 0;

        /// <summary>Creates a new scheduler as part of this group.</summary> 
        /// <returns>The new scheduler.</returns> 
        public TaskScheduler CreateScheduler()
        {
            var createdQueue = new RoundRobinTaskSchedulerQueue(this);
            lock (_queues) _queues.Add(createdQueue);
            return createdQueue;
        }

        /// <summary>Gets a collection of all schedulers in this group.</summary> 
        public ReadOnlyCollection<TaskScheduler> Schedulers
        {
            get { lock (_queues) return new ReadOnlyCollection<TaskScheduler>(_queues.Cast<TaskScheduler>().ToArray()); }
        }

        /// <summary>Removes a scheduler from the group.</summary> 
        /// <param name="queue">The scheduler to be removed.</param> 
        private void RemoveQueue_NeedsLock(RoundRobinTaskSchedulerQueue queue)
        {
            int index = _queues.IndexOf(queue);
            if (_nextQueue >= index) _nextQueue--;
            _queues.RemoveAt(index);
        }

        /// <summary>Notifies the ThreadPool that there's a new item to be executed.</summary> 
        private void NotifyNewWorkItem()
        {
            // Queue a processing delegate to the ThreadPool 
            ThreadPool.UnsafeQueueUserWorkItem(_ =>
            {
                Task targetTask = null;
                RoundRobinTaskSchedulerQueue queueForTargetTask = null;
                lock (_queues)
                {
                    // Determine the order in which we'll search the schedulers for work 
                    var searchOrder = Enumerable.Range(_nextQueue, _queues.Count - _nextQueue).Concat(Enumerable.Range(0, _nextQueue));

                    // Look for the next item to process 
                    foreach (int i in searchOrder)
                    {
                        queueForTargetTask = _queues[i];
                        var items = queueForTargetTask._workItems;
                        if (items.Count > 0)
                        {
                            targetTask = items.Dequeue();
                            _nextQueue = i;
                            if (queueForTargetTask._disposed && items.Count == 0)
                            {
                                RemoveQueue_NeedsLock(queueForTargetTask);
                            }
                            break;
                        }
                    }
                    _nextQueue = (_nextQueue + 1) % _queues.Count;
                }

                // If we found an item, run it 
                if (targetTask != null) queueForTargetTask.RunQueuedTask(targetTask);
            }, null);
        }

        /// <summary>A scheduler that participates in round-robin scheduling.</summary> 
        private sealed class RoundRobinTaskSchedulerQueue : TaskScheduler, IDisposable
        {
            internal RoundRobinTaskSchedulerQueue(RoundRobinSchedulerGroup pool) { _pool = pool; }

            private RoundRobinSchedulerGroup _pool;
            internal Queue<Task> _workItems = new Queue<Task>();
            internal bool _disposed;

            protected override IEnumerable<Task> GetScheduledTasks()
            {
                object obj = _pool._queues;
                bool lockTaken = false;
                try
                {
                    Monitor.TryEnter(obj, ref lockTaken);
                    if (lockTaken) return _workItems.ToArray();
                    else throw new NotSupportedException();
                }
                finally
                {
                    if (lockTaken) Monitor.Exit(obj);
                }
            }

            protected override void QueueTask(Task task)
            {
                if (_disposed) throw new ObjectDisposedException(GetType().Name);
                lock (_pool._queues) _workItems.Enqueue(task);
                _pool.NotifyNewWorkItem();
            }

            protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
            {
                return base.TryExecuteTask(task);
            }

            internal void RunQueuedTask(Task task) { TryExecuteTask(task); }

            void IDisposable.Dispose()
            {
                if (!_disposed)
                {
                    lock (_pool._queues)
                    {
                        if (_workItems.Count == 0) _pool.RemoveQueue_NeedsLock(this);
                        _disposed = true;
                    }
                }
            }
        }
    }

    /// <summary> 
    /// Provides a TaskScheduler that provides control over priorities, fairness, and the underlying threads utilized. 
    /// </summary> 
    [DebuggerTypeProxy(typeof(QueuedTaskSchedulerDebugView))]
    [DebuggerDisplay("Id={Id}, Queues={DebugQueueCount}, ScheduledTasks = {DebugTaskCount}")]
    public sealed class QueuedTaskScheduler : TaskScheduler, IDisposable
    {
        /// <summary>Debug view for the QueuedTaskScheduler.</summary> 
        private class QueuedTaskSchedulerDebugView
        {
            /// <summary>The scheduler.</summary> 
            private QueuedTaskScheduler _scheduler;

            /// <summary>Initializes the debug view.</summary> 
            /// <param name="scheduler">The scheduler.</param> 
            public QueuedTaskSchedulerDebugView(QueuedTaskScheduler scheduler)
            {
                if (scheduler == null) throw new ArgumentNullException("scheduler");
                _scheduler = scheduler;
            }

            /// <summary>Gets all of the Tasks queued to the scheduler directly.</summary> 
            public IEnumerable<Task> ScheduledTasks
            {
                get
                {
                    var tasks = (_scheduler._targetScheduler != null) ?
                        (IEnumerable<Task>)_scheduler._nonthreadsafeTaskQueue :
                        (IEnumerable<Task>)_scheduler._blockingTaskQueue;
                    return tasks.Where(t => t != null).ToList();
                }
            }

            /// <summary>Gets the prioritized and fair queues.</summary> 
            public IEnumerable<TaskScheduler> Queues
            {
                get
                {
                    List<TaskScheduler> queues = new List<TaskScheduler>();
                    foreach (var group in _scheduler._queueGroups) queues.AddRange(group.Value);
                    return queues;
                }
            }
        }

        /// <summary> 
        /// A sorted list of round-robin queue lists.  Tasks with the smallest priority value 
        /// are preferred.  Priority groups are round-robin'd through in order of priority. 
        /// </summary> 
        private readonly SortedList<int, QueueGroup> _queueGroups = new SortedList<int, QueueGroup>();
        /// <summary>Cancellation token used for disposal.</summary> 
        private readonly CancellationTokenSource _disposeCancellation = new CancellationTokenSource();
        /// <summary> 
        /// The maximum allowed concurrency level of this scheduler.  If custom threads are 
        /// used, this represents the number of created threads. 
        /// </summary> 
        private readonly int _concurrencyLevel;
        /// <summary>Whether we're processing tasks on the current thread.</summary> 
        private static ThreadLocal<bool> _taskProcessingThread = new ThreadLocal<bool>();

        // *** 
        // *** For when using a target scheduler 
        // *** 

        /// <summary>The scheduler onto which actual work is scheduled.</summary> 
        private readonly TaskScheduler _targetScheduler;
        /// <summary>The queue of tasks to process when using an underlying target scheduler.</summary> 
        private readonly Queue<Task> _nonthreadsafeTaskQueue;
        /// <summary>The number of Tasks that have been queued or that are running whiel using an underlying scheduler.</summary> 
        private int _delegatesQueuedOrRunning = 0;

        // *** 
        // *** For when using our own threads 
        // *** 

        /// <summary>The threads used by the scheduler to process work.</summary> 
        private readonly Thread[] _threads;
        /// <summary>The collection of tasks to be executed on our custom threads.</summary> 
        private readonly BlockingCollection<Task> _blockingTaskQueue;

        // *** 

        /// <summary>Initializes the scheduler.</summary> 
        public QueuedTaskScheduler() : this(TaskScheduler.Default, 0) { }

        /// <summary>Initializes the scheduler.</summary> 
        /// <param name="targetScheduler">The target underlying scheduler onto which this sceduler's work is queued.</param> 
        public QueuedTaskScheduler(TaskScheduler targetScheduler) : this(targetScheduler, 0) { }

        /// <summary>Initializes the scheduler.</summary> 
        /// <param name="targetScheduler">The target underlying scheduler onto which this sceduler's work is queued.</param> 
        /// <param name="maxConcurrencyLevel">The maximum degree of concurrency allowed for this scheduler's work.</param> 
        public QueuedTaskScheduler(
            TaskScheduler targetScheduler,
            int maxConcurrencyLevel)
        {
            // Validate arguments 
            if (targetScheduler == null) throw new ArgumentNullException("underlyingScheduler");
            if (maxConcurrencyLevel < 0) throw new ArgumentOutOfRangeException("concurrencyLevel");

            // Initialize only those fields relevant to use an underlying scheduler.  We don't 
            // initialize the fields relevant to using our own custom threads. 
            _targetScheduler = targetScheduler;
            _nonthreadsafeTaskQueue = new Queue<Task>();

            // If 0, use the number of logical processors.  But make sure whatever value we pick 
            // is not greater than the degree of parallelism allowed by the underlying scheduler. 
            _concurrencyLevel = maxConcurrencyLevel != 0 ? maxConcurrencyLevel : Environment.ProcessorCount;
            if (targetScheduler.MaximumConcurrencyLevel > 0 &&
                targetScheduler.MaximumConcurrencyLevel < _concurrencyLevel)
            {
                _concurrencyLevel = targetScheduler.MaximumConcurrencyLevel;
            }
        }

        /// <summary>Initializes the scheduler.</summary> 
        /// <param name="threadCount">The number of threads to create and use for processing work items.</param> 
        public QueuedTaskScheduler(int threadCount) : this(threadCount, string.Empty, false, ThreadPriority.Normal, ApartmentState.MTA, 0, null, null) { }

        /// <summary>Initializes the scheduler.</summary> 
        /// <param name="threadCount">The number of threads to create and use for processing work items.</param> 
        /// <param name="threadName">The name to use for each of the created threads.</param> 
        /// <param name="useForegroundThreads">A Boolean value that indicates whether to use foreground threads instead of background.</param> 
        /// <param name="threadPriority">The priority to assign to each thread.</param> 
        /// <param name="threadApartmentState">The apartment state to use for each thread.</param> 
        /// <param name="threadMaxStackSize">The stack size to use for each thread.</param> 
        /// <param name="threadInit">An initialization routine to run on each thread.</param> 
        /// <param name="threadFinally">A finalization routine to run on each thread.</param> 
        public QueuedTaskScheduler(
            int threadCount,
            string threadName = "",
            bool useForegroundThreads = false,
            ThreadPriority threadPriority = ThreadPriority.Normal,
            ApartmentState threadApartmentState = ApartmentState.MTA,
            int threadMaxStackSize = 0,
            Action threadInit = null,
            Action threadFinally = null)
        {
            // Validates arguments (some validation is left up to the Thread type itself). 
            // If the thread count is 0, default to the number of logical processors. 
            if (threadCount < 0) throw new ArgumentOutOfRangeException("concurrencyLevel");
            else if (threadCount == 0) _concurrencyLevel = Environment.ProcessorCount;
            else _concurrencyLevel = threadCount;

            // Initialize the queue used for storing tasks 
            _blockingTaskQueue = new BlockingCollection<Task>();

            // Create all of the threads 
            _threads = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                _threads[i] = new Thread(() => ThreadBasedDispatchLoop(threadInit, threadFinally), threadMaxStackSize)
                {
                    Priority = threadPriority,
                    IsBackground = !useForegroundThreads,
                };
                if (threadName != null) _threads[i].Name = threadName + " (" + i + ")";
                _threads[i].SetApartmentState(threadApartmentState);
            }

            // Start all of the threads 
            foreach (var thread in _threads) thread.Start();
        }

        /// <summary>The dispatch loop run by all threads in this scheduler.</summary> 
        /// <param name="threadInit">An initialization routine to run when the thread begins.</param> 
        /// <param name="threadFinally">A finalization routine to run before the thread ends.</param> 
        private void ThreadBasedDispatchLoop(Action threadInit, Action threadFinally)
        {
            _taskProcessingThread.Value = true;
            if (threadInit != null) threadInit();
            try
            {
                // If the scheduler is disposed, the cancellation token will be set and 
                // we'll receive an OperationCanceledException.  That OCE should not crash the process. 
                try
                {
                    // If a thread abort occurs, we'll try to reset it and continue running. 
                    while (true)
                    {
                        try
                        {
                            // For each task queued to the scheduler, try to execute it. 
                            foreach (var task in _blockingTaskQueue.GetConsumingEnumerable(_disposeCancellation.Token))
                            {
                                // If the task is not null, that means it was queued to this scheduler directly. 
                                // Run it. 
                                if (task != null)
                                {
                                    TryExecuteTask(task);
                                }
                                // If the task is null, that means it's just a placeholder for a task 
                                // queued to one of the subschedulers.  Find the next task based on 
                                // priority and fairness and run it. 
                                else
                                {
                                    // Find the next task based on our ordering rules... 
                                    Task targetTask;
                                    QueuedTaskSchedulerQueue queueForTargetTask;
                                    lock (_queueGroups) FindNextTask_NeedsLock(out targetTask, out queueForTargetTask);

                                    // ... and if we found one, run it 
                                    if (targetTask != null) queueForTargetTask.ExecuteTask(targetTask);
                                }
                            }
                        }
                        catch (ThreadAbortException)
                        {
                            // If we received a thread abort, and that thread abort was due to shutting down 
                            // or unloading, let it pass through.  Otherwise, reset the abort so we can 
                            // continue processing work items. 
                            if (!Environment.HasShutdownStarted && !AppDomain.CurrentDomain.IsFinalizingForUnload())
                            {
                                Thread.ResetAbort();
                            }
                        }
                    }
                }
                catch (OperationCanceledException) { }
            }
            finally
            {
                // Run a cleanup routine if there was one 
                if (threadFinally != null) threadFinally();
                _taskProcessingThread.Value = false;
            }
        }

        /// <summary>Gets the number of queues currently activated.</summary> 
        private int DebugQueueCount
        {
            get
            {
                int count = 0;
                foreach (var group in _queueGroups) count += group.Value.Count;
                return count;
            }
        }

        /// <summary>Gets the number of tasks currently scheduled.</summary> 
        private int DebugTaskCount
        {
            get
            {
                return (_targetScheduler != null ?
                    (IEnumerable<Task>)_nonthreadsafeTaskQueue : (IEnumerable<Task>)_blockingTaskQueue)
                    .Where(t => t != null).Count();
            }
        }

        /// <summary>Find the next task that should be executed, based on priorities and fairness and the like.</summary> 
        /// <param name="targetTask">The found task, or null if none was found.</param> 
        /// <param name="queueForTargetTask"> 
        /// The scheduler associated with the found task.  Due to security checks inside of TPL,   
        /// this scheduler needs to be used to execute that task. 
        /// </param> 
        private void FindNextTask_NeedsLock(out Task targetTask, out QueuedTaskSchedulerQueue queueForTargetTask)
        {
            targetTask = null;
            queueForTargetTask = null;

            // Look through each of our queue groups in sorted order. 
            // This ordering is based on the priority of the queues. 
            foreach (var queueGroup in _queueGroups)
            {
                var queues = queueGroup.Value;

                // Within each group, iterate through the queues in a round-robin 
                // fashion.  Every time we iterate again and successfully find a task,  
                // we'll start in the next location in the group. 
                foreach (int i in queues.CreateSearchOrder())
                {
                    queueForTargetTask = queues[i];
                    var items = queueForTargetTask._workItems;
                    if (items.Count > 0)
                    {
                        targetTask = items.Dequeue();
                        if (queueForTargetTask._disposed && items.Count == 0)
                        {
                            RemoveQueue_NeedsLock(queueForTargetTask);
                        }
                        queues.NextQueueIndex = (queues.NextQueueIndex + 1) % queueGroup.Value.Count;
                        return;
                    }
                }
            }
        }

        /// <summary>Queues a task to the scheduler.</summary> 
        /// <param name="task">The task to be queued.</param> 
        protected override void QueueTask(Task task)
        {
            // If we've been disposed, no one should be queueing 
            if (_disposeCancellation.IsCancellationRequested) throw new ObjectDisposedException(GetType().Name);

            // If the target scheduler is null (meaning we're using our own threads), 
            // add the task to the blocking queue 
            if (_targetScheduler == null)
            {
                _blockingTaskQueue.Add(task);
            }
            // Otherwise, add the task to the non-blocking queue, 
            // and if there isn't already an executing processing task, 
            // start one up 
            else
            {
                // Queue the task and check whether we should launch a processing 
                // task (noting it if we do, so that other threads don't result 
                // in queueing up too many). 
                bool launchTask = false;
                lock (_nonthreadsafeTaskQueue)
                {
                    _nonthreadsafeTaskQueue.Enqueue(task);
                    if (_delegatesQueuedOrRunning < _concurrencyLevel)
                    {
                        ++_delegatesQueuedOrRunning;
                        launchTask = true;
                    }
                }

                // If necessary, start processing asynchronously 
                if (launchTask)
                {
                    Task.Factory.StartNew(ProcessPrioritizedAndBatchedTasks,
                        CancellationToken.None, TaskCreationOptions.None, _targetScheduler);
                }
            }
        }

        /// <summary> 
        /// Process tasks one at a time in the best order.   
        /// This should be run in a Task generated by QueueTask. 
        /// It's been separated out into its own method to show up better in Parallel Tasks. 
        /// </summary> 
        private void ProcessPrioritizedAndBatchedTasks()
        {
            bool continueProcessing = true;
            while (!_disposeCancellation.IsCancellationRequested && continueProcessing)
            {
                try
                {
                    // Note that we're processing tasks on this thread 
                    _taskProcessingThread.Value = true;

                    // Until there are no more tasks to process 
                    while (!_disposeCancellation.IsCancellationRequested)
                    {
                        // Try to get the next task.  If there aren't any more, we're done. 
                        Task targetTask;
                        lock (_nonthreadsafeTaskQueue)
                        {
                            if (_nonthreadsafeTaskQueue.Count == 0) break;
                            targetTask = _nonthreadsafeTaskQueue.Dequeue();
                        }

                        // If the task is null, it's a placeholder for a task in the round-robin queues. 
                        // Find the next one that should be processed. 
                        QueuedTaskSchedulerQueue queueForTargetTask = null;
                        if (targetTask == null)
                        {
                            lock (_queueGroups) FindNextTask_NeedsLock(out targetTask, out queueForTargetTask);
                        }

                        // Now if we finally have a task, run it.  If the task 
                        // was associated with one of the round-robin schedulers, we need to use it 
                        // as a thunk to execute its task. 
                        if (targetTask != null)
                        {
                            if (queueForTargetTask != null) queueForTargetTask.ExecuteTask(targetTask);
                            else TryExecuteTask(targetTask);
                        }
                    }
                }
                finally
                {
                    // Now that we think we're done, verify that there really is 
                    // no more work to do.  If there's not, highlight 
                    // that we're now less parallel than we were a moment ago. 
                    lock (_nonthreadsafeTaskQueue)
                    {
                        if (_nonthreadsafeTaskQueue.Count == 0)
                        {
                            _delegatesQueuedOrRunning--;
                            continueProcessing = false;
                            _taskProcessingThread.Value = false;
                        }
                    }
                }
            }
        }

        /// <summary>Notifies the pool that there's a new item to be executed in one of the round-robin queues.</summary> 
        private void NotifyNewWorkItem() { QueueTask(null); }

        /// <summary>Tries to execute a task synchronously on the current thread.</summary> 
        /// <param name="task">The task to execute.</param> 
        /// <param name="taskWasPreviouslyQueued">Whether the task was previously queued.</param> 
        /// <returns>true if the task was executed; otherwise, false.</returns> 
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            // If we're already running tasks on this threads, enable inlining 
            return _taskProcessingThread.Value && TryExecuteTask(task);
        }

        /// <summary>Gets the tasks scheduled to this scheduler.</summary> 
        /// <returns>An enumerable of all tasks queued to this scheduler.</returns> 
        /// <remarks>This does not include the tasks on sub-schedulers.  Those will be retrieved by the debugger separately.</remarks> 
        protected override IEnumerable<Task> GetScheduledTasks()
        {
            // If we're running on our own threads, get the tasks from the blocking queue... 
            if (_targetScheduler == null)
            {
                // Get all of the tasks, filtering out nulls, which are just placeholders 
                // for tasks in other sub-schedulers 
                return _blockingTaskQueue.Where(t => t != null).ToList();
            }
            // otherwise get them from the non-blocking queue... 
            else
            {
                return _nonthreadsafeTaskQueue.Where(t => t != null).ToList();
            }
        }

        /// <summary>Gets the maximum concurrency level to use when processing tasks.</summary> 
        public override int MaximumConcurrencyLevel { get { return _concurrencyLevel; } }

        /// <summary>Initiates shutdown of the scheduler.</summary> 
        public void Dispose()
        {
            _disposeCancellation.Cancel();
        }

        /// <summary>Creates and activates a new scheduling queue for this scheduler.</summary> 
        /// <returns>The newly created and activated queue at priority 0.</returns> 
        public TaskScheduler ActivateNewQueue() { return ActivateNewQueue(0); }

        /// <summary>Creates and activates a new scheduling queue for this scheduler.</summary> 
        /// <param name="priority">The priority level for the new queue.</param> 
        /// <returns>The newly created and activated queue at the specified priority.</returns> 
        public TaskScheduler ActivateNewQueue(int priority)
        {
            // Create the queue 
            var createdQueue = new QueuedTaskSchedulerQueue(priority, this);

            // Add the queue to the appropriate queue group based on priority 
            lock (_queueGroups)
            {
                QueueGroup list;
                if (!_queueGroups.TryGetValue(priority, out list))
                {
                    list = new QueueGroup();
                    _queueGroups.Add(priority, list);
                }
                list.Add(createdQueue);
            }

            // Hand the new queue back 
            return createdQueue;
        }

        /// <summary>Removes a scheduler from the group.</summary> 
        /// <param name="queue">The scheduler to be removed.</param> 
        private void RemoveQueue_NeedsLock(QueuedTaskSchedulerQueue queue)
        {
            // Find the group that contains the queue and the queue's index within the group 
            var queueGroup = _queueGroups[queue._priority];
            int index = queueGroup.IndexOf(queue);

            // We're about to remove the queue, so adjust the index of the next 
            // round-robin starting location if it'll be affected by the removal 
            if (queueGroup.NextQueueIndex >= index) queueGroup.NextQueueIndex--;

            // Remove it 
            queueGroup.RemoveAt(index);
        }

        /// <summary>A group of queues a the same priority level.</summary> 
        private class QueueGroup : List<QueuedTaskSchedulerQueue>
        {
            /// <summary>The starting index for the next round-robin traversal.</summary> 
            public int NextQueueIndex = 0;

            /// <summary>Creates a search order through this group.</summary> 
            /// <returns>An enumerable of indices for this group.</returns> 
            public IEnumerable<int> CreateSearchOrder()
            {
                for (int i = NextQueueIndex; i < Count; i++) yield return i;
                for (int i = 0; i < NextQueueIndex; i++) yield return i;
            }
        }

        /// <summary>Provides a scheduling queue associatd with a QueuedTaskScheduler.</summary> 
        [DebuggerDisplay("QueuePriority = {_priority}, WaitingTasks = {WaitingTasks}")]
        [DebuggerTypeProxy(typeof(QueuedTaskSchedulerQueueDebugView))]
        private sealed class QueuedTaskSchedulerQueue : TaskScheduler, IDisposable
        {
            /// <summary>A debug view for the queue.</summary> 
            private sealed class QueuedTaskSchedulerQueueDebugView
            {
                /// <summary>The queue.</summary> 
                private readonly QueuedTaskSchedulerQueue _queue;

                /// <summary>Initializes the debug view.</summary> 
                /// <param name="queue">The queue to be debugged.</param> 
                public QueuedTaskSchedulerQueueDebugView(QueuedTaskSchedulerQueue queue)
                {
                    if (queue == null) throw new ArgumentNullException("queue");
                    _queue = queue;
                }

                /// <summary>Gets the priority of this queue in its associated scheduler.</summary> 
                public int Priority { get { return _queue._priority; } }
                /// <summary>Gets the ID of this scheduler.</summary> 
                public int Id { get { return _queue.Id; } }
                /// <summary>Gets all of the tasks scheduled to this queue.</summary> 
                public IEnumerable<Task> ScheduledTasks { get { return _queue.GetScheduledTasks(); } }
                /// <summary>Gets the QueuedTaskScheduler with which this queue is associated.</summary> 
                public QueuedTaskScheduler AssociatedScheduler { get { return _queue._pool; } }
            }

            /// <summary>The scheduler with which this pool is associated.</summary> 
            private readonly QueuedTaskScheduler _pool;
            /// <summary>The work items stored in this queue.</summary> 
            internal readonly Queue<Task> _workItems;
            /// <summary>Whether this queue has been disposed.</summary> 
            internal bool _disposed;
            /// <summary>Gets the priority for this queue.</summary> 
            internal int _priority;

            /// <summary>Initializes the queue.</summary> 
            /// <param name="priority">The priority associated with this queue.</param> 
            /// <param name="pool">The scheduler with which this queue is associated.</param> 
            internal QueuedTaskSchedulerQueue(int priority, QueuedTaskScheduler pool)
            {
                _priority = priority;
                _pool = pool;
                _workItems = new Queue<Task>();
            }

            /// <summary>Gets the number of tasks waiting in this scheduler.</summary> 
            internal int WaitingTasks { get { return _workItems.Count; } }

            /// <summary>Gets the tasks scheduled to this scheduler.</summary> 
            /// <returns>An enumerable of all tasks queued to this scheduler.</returns> 
            protected override IEnumerable<Task> GetScheduledTasks() { return _workItems.ToList(); }

            /// <summary>Queues a task to the scheduler.</summary> 
            /// <param name="task">The task to be queued.</param> 
            protected override void QueueTask(Task task)
            {
                if (_disposed) throw new ObjectDisposedException(GetType().Name);

                // Queue up the task locally to this queue, and then notify 
                // the parent scheduler that there's work available 
                lock (_pool._queueGroups) _workItems.Enqueue(task);
                _pool.NotifyNewWorkItem();
            }

            /// <summary>Tries to execute a task synchronously on the current thread.</summary> 
            /// <param name="task">The task to execute.</param> 
            /// <param name="taskWasPreviouslyQueued">Whether the task was previously queued.</param> 
            /// <returns>true if the task was executed; otherwise, false.</returns> 
            protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
            {
                // If we're using our own threads and if this is being called from one of them, 
                // or if we're currently processing another task on this thread, try running it inline. 
                return _taskProcessingThread.Value && TryExecuteTask(task);
            }

            /// <summary>Runs the specified ask.</summary> 
            /// <param name="task">The task to execute.</param> 
            internal void ExecuteTask(Task task) { TryExecuteTask(task); }

            /// <summary>Gets the maximum concurrency level to use when processing tasks.</summary> 
            public override int MaximumConcurrencyLevel { get { return _pool.MaximumConcurrencyLevel; } }

            /// <summary>Signals that the queue should be removed from the scheduler as soon as the queue is empty.</summary> 
            public void Dispose()
            {
                if (!_disposed)
                {
                    lock (_pool._queueGroups)
                    {
                        // We only remove the queue if it's empty.  If it's not empty, 
                        // we still mark it as disposed, and the associated QueuedTaskScheduler 
                        // will remove the queue when its count hits 0 and its _disposed is true. 
                        if (_workItems.Count == 0)
                        {
                            _pool.RemoveQueue_NeedsLock(this);
                        }
                    }
                    _disposed = true;
                }
            }
        }
    }

}
