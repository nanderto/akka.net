using Akka.Persistence.Snapshot;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using System.Threading;

namespace Akka.Persistence.ServiceFabric.Snapshot
{
    public class ServiceFabricSnapshotStore : SnapshotStore
    {
        private readonly IReliableStateManager StateManager;

        public ServiceFabricSnapshotStore()
        {
            this.StateManager = ServiceFabricPersistence.Instance.Apply(Context.System).StateManager;
        }

        protected async override Task DeleteAsync(SnapshotMetadata metadata)
        {
            ServiceEventSource.Current.Message($"Entering ServiceFabricSnapshotStore.{nameof(DeleteAsync)} PersistenceId: {metadata.PersistenceId} SequencNumer: {metadata.SequenceNr}");

            using (var tx = this.StateManager.CreateTransaction())
            {
                var snapshots = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, SnapshotEntry>>(metadata.PersistenceId);

                var removed = await snapshots.TryRemoveAsync(tx, $"{metadata.PersistenceId}_{metadata.SequenceNr}");
                if(removed.HasValue)
                {
                    var result = removed.Value;
                }
                await tx.CommitAsync();
            }
        }

        protected async override Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            ServiceEventSource.Current.Message($"Entering ServiceFabricSnapshotStore.{nameof(DeleteAsync)} PersistenceId: {persistenceId} ");

            if ((criteria.MaxSequenceNr > 0 && criteria.MaxSequenceNr < long.MaxValue) &&
               (criteria.MaxTimeStamp != DateTime.MinValue && criteria.MaxTimeStamp != DateTime.MaxValue))
            {
                using (var tx = this.StateManager.CreateTransaction())
                {
                    var snapshots = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, SnapshotEntry>>(persistenceId);
                    long FirstSequenceNumber = 0;
                    for (long i = 0; i < criteria.MaxSequenceNr; i++)
                    {
                        var result = await snapshots.TryGetValueAsync(tx, $"{persistenceId}_{i}");
                        var snapShot = result.HasValue ? result.Value : null;
                        if (snapShot.Timestamp > criteria.MaxTimeStamp.Ticks)
                        {
                            FirstSequenceNumber = i;
                            await snapshots.TryRemoveAsync(tx, $"{persistenceId}_{i}");
                        }
                    }

                    await tx.CommitAsync();
                }
            }
        }

        /// <summary>
        /// Asynchronously loads snapshot with the highest sequence number for a persistent actor/view matching specified criteria.
        /// </summary>
        protected async override Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            ServiceEventSource.Current.Message($"Entering ServiceFabricSnapshotStore.{nameof(LoadAsync)} PersistenceId: {persistenceId} ");

            SnapshotEntry snapshot = null;
            if (criteria.MaxSequenceNr > 0 && criteria.MaxSequenceNr < long.MaxValue)
            {
                var MaxNumberkey = $"{persistenceId}_{criteria.MaxSequenceNr}";
                using (var tx = this.StateManager.CreateTransaction())
                {
                    ServiceEventSource.Current.Message($"{persistenceId} ");

                    var snapshots = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, SnapshotEntry>>(persistenceId);
                    var snapshotStorageCurrentHighSequenceNumber = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, long>>("SnapshotStorageCurrentHighSequenceNumber");

                    var maxSequenceNumberConditional = await snapshotStorageCurrentHighSequenceNumber.TryGetValueAsync(tx, persistenceId);
                    if(maxSequenceNumberConditional.HasValue)
                    {
                        var MaxSequenceNumber = maxSequenceNumberConditional.Value;
                        var ret = await snapshots.TryGetValueAsync(tx, $"{persistenceId}_{MaxSequenceNumber}");
                        snapshot = ret.HasValue ? ret.Value : null;
                        await tx.CommitAsync();
                        SelectedSnapshot ss = new SelectedSnapshot(new SnapshotMetadata(persistenceId, snapshot.SequenceNr), snapshot);
                        return ss;
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// Asynchronously stores a snapshot with metadata as object in the reliable dictionary, saves the highest Sequence number 
        /// separatly to allow the last one to be found by sequence number.
        /// </summary>
        protected async override Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            ServiceEventSource.Current.Message($"Entering ServiceFabricSnapshotStore.{nameof(SaveAsync)} PersistenceId: {metadata.PersistenceId} SequencNumer: {metadata.SequenceNr}");

            var snapshotEntry = new SnapshotEntry
            {
                Id = metadata.PersistenceId + "_" + metadata.SequenceNr,
                PersistenceId = metadata.PersistenceId,
                SequenceNr = metadata.SequenceNr,
                Snapshot = snapshot,
                Timestamp = metadata.Timestamp.Ticks
            };

            var snapshotEntryId = metadata.PersistenceId + "_" + metadata.SequenceNr;
            
            using (var tx = this.StateManager.CreateTransaction())
            {
                var snapshotStorageCurrentHighSequenceNumber = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, long>>("SnapshotStorageCurrentHighSequenceNumber");

                var resultCurrentHighSquenceNumber = await snapshotStorageCurrentHighSequenceNumber.AddOrUpdateAsync(tx, metadata.PersistenceId, metadata.SequenceNr, (ssschsn, lng) => metadata.SequenceNr);
                
                ServiceEventSource.Current.Message($"resultCurrentHighSquenceNumber: {resultCurrentHighSquenceNumber}");
   
                var snapshots = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, SnapshotEntry>>(metadata.PersistenceId);
                var resultSnapshotAdd = await snapshots.GetOrAddAsync(tx, snapshotEntry.Id, ssid => snapshotEntry);

                ServiceEventSource.Current.Message($"resultSnapshotAdd: {resultSnapshotAdd}");
               
                await tx.CommitAsync();
                ServiceEventSource.Current.Message($"Leaving {nameof(SaveAsync)} PersistenceId: {metadata.PersistenceId} SequencNumer: {metadata.SequenceNr}");
            }
           
            return;
        }
    }

}
