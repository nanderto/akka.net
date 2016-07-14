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
        /// The collection for the ServiceFabric journal
        /// </summary>
        //public IMongoCollection<JournalEntry> JournalCollection { get; private set; }

        /// <summary>
        /// The settings for the MongoDB snapshot store.
        /// </summary>
        public ServiceFabricSnapshotSettings SnapshotStoreSettings { get; private set; }
        public IReliableStateManager StateManager { get; internal set; }

        /// <summary>
        /// The collection for the ServiceFabric snapshot store
        /// </summary>
        //public IServiceFabricCollection<SnapshotEntry> SnapshotCollection { get; private set; }

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

        //public StatefulServiceContext GetReference(String fieldName)
        //{
        //    var assemblies = AppDomain.CurrentDomain.GetAssemblies();
        //    assemblies[0].GetModule("").
        //    var r = AppDomain.CurrentDomain. .AssemblyResolve("System.Fabric.StatefulServiceContext");
        //    Type t = typeof(StatefulServiceContext);
        //    Assembly assemFromType = t.Assembly;
        //    Assembly currentAssem = Assembly.GetExecutingAssembly();
        //    var context = currentAssem.GetModule("Akka.Persistence.ServiceFabric");
           
        //    Type classType = this.GetType();
        //    FieldInfo info = classType.GetField(fieldName);
        //    // Use FieldInfo for fields, PropertyInfo for Properties (get/set accessor) and MethodInfo for methods
        //    return (StatefulServiceContext)info.GetValue(this); // use "this" assuming this method will be in the same class that contains the fields you're attempting to access.
        //}

    }
}
