using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Akka.Configuration;
using Akka.Actor;
using AkkaPersistence.Actors;
using Akka.Persistence.ServiceFabric;
using Akka.Persistence.ServiceFabric.Snapshot;
using Microsoft.ServiceFabric.Data;

namespace AkkaPersistence
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class AkkaPersistence : AkkaStatefulService
    {
        public AkkaPersistence(StatefulServiceContext context)
            : base(context)
        { }
      
        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see http://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        //protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        //{
        //    return new ServiceReplicaListener[0];
        //} 

    /// <summary>
    /// This is the main entry point for your service replica.
    /// This method executes when this replica of your service becomes primary and has write status.
    /// </summary>
    /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
    protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following sample code with your own logic 
            //       or remove this RunAsync override if it's not needed in your service.
            var config = ConfigurationFactory.ParseString(@"
                akka {
                    persistence {
                        journal {
                            # Path to the journal plugin to be used
                            plugin = ""akka.persistence.journal.servicefabricjournal""

                            # In-memory journal plugin.
                            servicefabricjournal {
                                # Class name of the plugin.
                                class = ""Akka.Persistence.ServiceFabric.Journal.ServiceFabricJournal, Akka.Persistence.ServiceFabric""
                                # Dispatcher for the plugin actor.
                                plugin-dispatcher = ""akka.actor.default-dispatcher""
                            }
                        }
                        snapshot-store {
                            plugin = ""akka.persistence.snapshot-store.servicefabric""
                            servicefabric {
                                class = ""Akka.Persistence.ServiceFabric.Snapshot.ServiceFabricSnapshotStore, Akka.Persistence.ServiceFabric""
                                # Dispatcher for the plugin actor.
                                plugin-dispatcher = ""akka.actor.default-dispatcher""
                            }
                        }
                    }  
                }");


            var myDictionary = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, long>>("myDictionary");

            // Create a new actor system (a container for your actors)
            var system = Akka.Actor.ActorSystem.Create("MySystem", config);

            // Create your actor and get a reference to it.
            // This will be an "ActorRef", which is not a
            // reference to the actual actor instance
            // but rather a client or proxy to it.
            var logger = system.ActorOf<LoggerActor>("Startup");
            // var logger2 = system.ActorOf<LoggerActor>("Startup2");
            // Send a message to the actor
            logger.Tell("First Message");
            //  logger2.Tell("First Message to Logger 2");

            // var myDictionary = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, long>>("myDictionary");

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (var tx = this.StateManager.CreateTransaction())
                {
                    var result = await myDictionary.TryGetValueAsync(tx, "Counter");

                    ServiceEventSource.Current.ServiceMessage(this, "Current Counter Value: {0}",
                        result.HasValue ? result.Value.ToString() : "Value does not exist.");

                    await myDictionary.AddOrUpdateAsync(tx, "Counter", 0, (key, value) => ++value);


                    var counter = result.HasValue ? result.Value.ToString() : "Value does not exist.";
                    logger.Tell($"Sending Logger a message, current counter is: {counter} ");
                   // logger2.Tell($"Sending Logger2 a message: {message} ");

                    //if (result.Value > 10)
                    //{
                    //    logger.Tell(PoisonPill.Instance);
                    //}

                    //if (result.Value > 20)
                    //{
                    //    logger = system.ActorOf<LoggerActor>("Second Startup");
                    //}
                    // If an exception is thrown before calling CommitAsync, the transaction aborts, all changes are 
                    // discarded, and nothing is saved to the secondary replicas.
                    await tx.CommitAsync();
                }

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }
    }
}
