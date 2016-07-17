using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.ServiceFabric.Data;
using Akka.Persistence.ServiceFabric.Snapshot;

namespace Akka.Persistence.ServiceFabric
{
    public class AkkaStatefulService : StatefulService
    {
        internal static StatefulServiceContext ServiceContext;

        internal static StatefulService StatefulService;

        //private StatefulServiceContext context;

        //private IReliableStateManager reliableStateManager;

        public AkkaStatefulService(StatefulServiceContext context)
            : this(context, new InitializationCallbackAdapter())
        {
                        
        }

        public AkkaStatefulService(StatefulServiceContext context, InitializationCallbackAdapter adapter)
            : base(context, new ReliableStateManager(context, new ReliableStateManagerConfiguration(onInitializeStateSerializersEvent: adapter.OnInitialize)))
        {
            adapter.StateManager = this.StateManager;
            ServiceContext = context;
            StatefulService = this;
        }
    }

    public class InitializationCallbackAdapter
    {
        public Task OnInitialize()
        {
            this.StateManager.TryAddStateSerializer(new SnapshotEntrySerializer());
            return Task.FromResult(true);
        }

        public IReliableStateManager StateManager { get; set; }
    }
}
