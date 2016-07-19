//-----------------------------------------------------------------------
// <copyright file="ServiceFabricJournal.cs" company="Akka.NET Project">
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
    using System.Threading;
    using Messages = Microsoft.ServiceFabric.Data.Collections.IReliableDictionary<string, LinkedList<IPersistentRepresentation>>;

    /// <summary>
    /// Service Fabric journal.
    /// </summary>
    public class ServiceFabricJournal : AsyncWriteJournal
    {
        private readonly IReliableStateManager StateManager;
        public ServiceFabricJournal(IReliableStateManager stateManager)
        {
            StateManager = stateManager;
        }

        public ServiceFabricJournal()
        {
            this.StateManager = ServiceFabricPersistence.Instance.Apply(Context.System).StateManager;
        }

        /// <summary>
        /// Write all messages contained in a single transaction to the underlying reliable dictionary
        /// </summary>
        /// <param name="messages">list of messages</param>
        /// <returns></returns>
        protected async override Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            IReliableDictionary<string, LinkedList<IPersistentRepresentation>> messageLinkedList = null;
            IReliableDictionary<long, LinkedList<IPersistentRepresentation>> messageList = null;

            using (var tx = this.StateManager.CreateTransaction())
            {
                foreach (var message in messages)
                {
                    foreach (var payload in (IEnumerable<IPersistentRepresentation>)message.Payload)
                    {
                        if (messageLinkedList == null)
                        {
                            messageLinkedList = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, LinkedList<IPersistentRepresentation>>>($"Messages_{payload.PersistenceId}");
                            //messageList = await this.StateManager.GetOrAddAsync<IReliableDictionary<long, LinkedList<IPersistentRepresentation>>>($"Messages_{payload.PersistenceId}");
                        }

                        var list = await messageLinkedList.GetOrAddAsync(tx, payload.PersistenceId, pid => new LinkedList<IPersistentRepresentation>());
                        //var source = System.Threading.CancellationTokenSource.CreateLinkedTokenSource();
                        CancellationTokenSource cts = new CancellationTokenSource();
                        CancellationToken token = cts.Token;


                        list.AddLast(payload);
                        await messageLinkedList.AddOrUpdateAsync(tx, payload.PersistenceId,list, (ll, a) =>
                        {
                            var x = ll;
                            return list;
                        }, 
                        TimeSpan.FromSeconds(5), 
                        token);
                    }
                }

                await tx.CommitAsync();
            }

            return (IImmutableList<Exception>) null; // all good
        }

        /// <summary>
        /// Read the highest Sequence number for the Persistance Id provided. from Sequence number is not required in this implementation
        /// </summary>
        /// <param name="persistenceId"></param>
        /// <param name="fromSequenceNumber"></param>
        /// <returns></returns>
        public async override Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNumber)
        {
            return await HighestSequenceNumberAsync(persistenceId);
        }

        public async override Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNumber, long toSequenceNumber, long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            var highest = await HighestSequenceNumberAsync(persistenceId);
            if (highest != 0L && max != 0L)
            {
                var result = await ReadAsync(persistenceId, fromSequenceNumber, Math.Min(toSequenceNumber, highest), max);
                result.ForEach(recoveryCallback);
            }

            return;
        }

        /// <summary>
        /// Deletes all messages from begining to the Sequence number
        /// </summary>
        /// <param name="persistenceId"></param>
        /// <param name="toSequenceNumber"></param>
        /// <returns></returns>
        protected async override Task DeleteMessagesToAsync(string persistenceId, long toSequenceNumber)
        {
            var highestSequenceNumber = await HighestSequenceNumberAsync(persistenceId);
            var toSeqNr = Math.Min(toSequenceNumber, highestSequenceNumber);
            for (var snr = 1L; snr <= toSeqNr; snr++)
            {
                await DeleteAsync(persistenceId, snr);
            }

            return;
        }

        public async Task<Messages> AddAsync(IPersistentRepresentation persistent)
        {
            using (var tx = this.StateManager.CreateTransaction())
            {
                var messages = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, LinkedList<IPersistentRepresentation>>>($"Messages_{persistent.PersistenceId}");
                var list = await messages.GetOrAddAsync(tx, persistent.PersistenceId, pid => new LinkedList<IPersistentRepresentation>());
                list.AddLast(persistent);
                await tx.CommitAsync();
                return messages;
            }
        }

        public async Task<Messages> UpdateAsync(string pid, long sequenceNumber, Func<IPersistentRepresentation, IPersistentRepresentation> updater)
        {
            using (var tx = this.StateManager.CreateTransaction())
            {
                var messages = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, LinkedList<IPersistentRepresentation>>>($"Messages_{pid}");
                var result = await messages.TryGetValueAsync(tx, pid);

                if (result.HasValue)
                {
                    var node = result.Value.First;
                    while (node != null)
                    {
                        if (node.Value.SequenceNr == sequenceNumber)
                            node.Value = updater(node.Value);

                        node = node.Next;
                    }
                }

                await tx.CommitAsync();
                return messages;
            }            
        }

        public async Task<Messages> DeleteAsync(string pid, long sequenceNumber)
        {//combine with calling method to make more efficient, dont need to loop through collections continuously
            using (var tx = this.StateManager.CreateTransaction())
            {
                var messages = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, LinkedList<IPersistentRepresentation>>>($"Messages_{pid}");
                var result = await messages.TryGetValueAsync(tx, pid);
                if (result.HasValue)
                {
                    var node = result.Value.First;
                    while (node != null)
                    {
                        if (node.Value.SequenceNr == sequenceNumber) //if Sequence number above then jump ouiof loop
                            result.Value.Remove(node);

                        node = node.Next;
                    }
                }

                await tx.CommitAsync();
                return messages;
            }
        }

        public async Task<IEnumerable<IPersistentRepresentation>> ReadAsync(string pid, long fromSequenceNumber, long toSequenceNumber, long max)
        {
            var ret = Enumerable.Empty<IPersistentRepresentation>();
            using (var tx = this.StateManager.CreateTransaction())
            {
                var messages = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, LinkedList<IPersistentRepresentation>>>($"Messages_{pid}");
                var result = await messages.TryGetValueAsync(tx, pid);

                if (result.HasValue)
                {
                    ret = result.Value
                        .Where(x => x.SequenceNr >= fromSequenceNumber && x.SequenceNr <= toSequenceNumber)
                        .Take(max > int.MaxValue ? int.MaxValue : (int)max);
                }

                await tx.CommitAsync();
            }

            return ret;
        }

        public async Task<long> HighestSequenceNumberAsync(string pid)
        {
            long returnLast = 0L;

            using (var tx = this.StateManager.CreateTransaction())
            {
                var messages = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, LinkedList<IPersistentRepresentation>>>($"Messages_{pid}");
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
    }
}

