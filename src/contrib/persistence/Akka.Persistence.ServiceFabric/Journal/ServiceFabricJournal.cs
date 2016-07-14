//-----------------------------------------------------------------------
// <copyright file="MemoryJournal.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2016 Typesafe Inc. <http://www.typesafe.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Persistence.Journal;
using Akka.Util.Internal;

namespace Akka.Persistence.ServiceFabric.Journal
{
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;
    using Messages = Microsoft.ServiceFabric.Data.Collections.IReliableDictionary<string, LinkedList<IPersistentRepresentation>>;

    /// <summary>
    /// Service Fabric journal.
    /// </summary>
    public class ServiceFabricJournal : AsyncWriteJournal
    {
       // private readonly IReliableDictionary<string, LinkedList<IPersistentRepresentation>> _messages;

        private readonly IReliableStateManager StateManager;

       // protected virtual IReliableDictionary<string, LinkedList<IPersistentRepresentation>> Messages { get { return _messages; } }


        public ServiceFabricJournal()
        {
            this.StateManager = ServiceFabricPersistence.Instance.Apply(Context.System).StateManager;
        }

        protected async override Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            foreach (var w in messages)
            {
                foreach (var p in (IEnumerable<IPersistentRepresentation>)w.Payload)
                {
                    await AddAsync(p);
                }
            }

            return (IImmutableList<Exception>) null; // all good
        }

        public async override Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            return await HighestSequenceNrAsync(persistenceId);
        }

        public async override Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            var highest = await HighestSequenceNrAsync(persistenceId);
            if (highest != 0L && max != 0L)
            {
                var result = await ReadAsync(persistenceId, fromSequenceNr, Math.Min(toSequenceNr, highest), max);
                result.ForEach(recoveryCallback);
            }

            return;
        }

        protected async override Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            var highestSequenceNumber = await HighestSequenceNrAsync(persistenceId);
            var toSeqNr = Math.Min(toSequenceNr, highestSequenceNumber);
            for (var snr = 1L; snr <= toSeqNr; snr++)
            {
                await DeleteAsync(persistenceId, snr);
            }

            return;
        }

        #region IMemoryMessages implementation

        public async Task<Messages> AddAsync(IPersistentRepresentation persistent)
        {
            using (var tx = this.StateManager.CreateTransaction())
            {
                var messages = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, LinkedList<IPersistentRepresentation>>>("Messages");
                var list = await messages.GetOrAddAsync(tx, persistent.PersistenceId, pid => new LinkedList<IPersistentRepresentation>());
                list.AddLast(persistent);
                await tx.CommitAsync();
                return messages;
            }
        }

        public async Task<Messages> UpdateAsync(string pid, long seqNr, Func<IPersistentRepresentation, IPersistentRepresentation> updater)
        {
            using (var tx = this.StateManager.CreateTransaction())
            {
                var messages = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, LinkedList<IPersistentRepresentation>>>("Messages");
                var result = await messages.TryGetValueAsync(tx, pid);

                if (result.HasValue)
                {
                    var node = result.Value.First;
                    while (node != null)
                    {
                        if (node.Value.SequenceNr == seqNr)
                            node.Value = updater(node.Value);

                        node = node.Next;
                    }
                }

                await tx.CommitAsync();
                return messages;
            }            
        }

        public async Task<Messages> DeleteAsync(string pid, long seqNr)
        {
            using (var tx = this.StateManager.CreateTransaction())
            {
                var messages = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, LinkedList<IPersistentRepresentation>>>("Messages");
                var result = await messages.TryGetValueAsync(tx, pid);
                if (result.HasValue)
                {
                    var node = result.Value.First;
                    while (node != null)
                    {
                        if (node.Value.SequenceNr == seqNr)
                            result.Value.Remove(node);

                        node = node.Next;
                    }
                }

                await tx.CommitAsync();
                return messages;
            }
        }

        public async Task<IEnumerable<IPersistentRepresentation>> ReadAsync(string pid, long fromSeqNr, long toSeqNr, long max)
        {
            var ret = Enumerable.Empty<IPersistentRepresentation>();
            using (var tx = this.StateManager.CreateTransaction())
            {
                var messages = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, LinkedList<IPersistentRepresentation>>>("Messages");
                var result = await messages.TryGetValueAsync(tx, pid);

                if (result.HasValue)
                {
                    ret = result.Value
                        .Where(x => x.SequenceNr >= fromSeqNr && x.SequenceNr <= toSeqNr)
                        .Take(max > int.MaxValue ? int.MaxValue : (int)max);
                }

                await tx.CommitAsync();
            }

            return ret;
        }

        public async Task<long> HighestSequenceNrAsync(string pid)
        {
            long returnLast = 0L;

            using (var tx = this.StateManager.CreateTransaction())
            {
                var messages = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, LinkedList<IPersistentRepresentation>>>("Messages");
                var result = await messages.TryGetValueAsync(tx, pid);

                if (result.HasValue)
                {
                    var last = result.Value.LastOrDefault();
                    returnLast = last != null ? last.SequenceNr : 0L;
                }

                await tx.CommitAsync();
                return returnLast;
            }
        }

        #endregion
    }
}

