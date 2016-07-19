using Akka.Actor;
using Akka.Persistence.ServiceFabric.Journal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using System.Reflection;
using System.Fabric;

namespace Akka.Persistence.ServiceFabric
{
    public class ServiceFabricExtension : IExtension
    {
        /// <summary>
        /// The settings for the ServiceFabric journal.
        /// </summary>
        public ServiceFabricJournalSettings JournalSettings { get; private set; }

        public StatefulServiceContext StatefulServiceContext { get; set; }

        /// <summary>
        /// The settings for the Service Fabric Snapshot Store.
        /// </summary>
        public ServiceFabricSnapshotSettings SnapshotStoreSettings { get; private set; }

        public IReliableStateManager StateManager { get; internal set; }

        public ServiceFabricExtension(ExtendedActorSystem system)
        {
            if (system == null)
            {
                throw new ArgumentNullException("system");
            }

            // Initialize fallback configuration defaults
            system.Settings.InjectTopLevelFallback(ServiceFabricPersistence.DefaultConfig());

            // Read config
            var journalConfig = system.Settings.Config.GetConfig("akka.persistence.journal.servicefabric");
            JournalSettings = new ServiceFabricJournalSettings(journalConfig);

            var snapshotConfig = system.Settings.Config.GetConfig("akka.persistence.snapshot-store.servicefabric");
            SnapshotStoreSettings = new ServiceFabricSnapshotSettings(snapshotConfig);

            StatefulServiceContext = AkkaStatefulService.ServiceContext;
            StateManager = AkkaStatefulService.StatefulService.StateManager;
        }
    }
}
