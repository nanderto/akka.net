using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Akka.Persistence.ServiceFabric;
using Akka.Configuration;
using Akka.Actor;
using PersistenceExample;

namespace AkkaPersistenceExample
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class AkkaPersistenceExample : AkkaStatefulService
    {
        public AkkaPersistenceExample(StatefulServiceContext context)
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
                        

            //SqlServerPersistence.Init(system);
            //BasicUsage(system);

            FailingActorExample(system);

           // SnapshotedActor(system);

            //ViewExample(system);

            //AtLeastOnceDelivery(system);
            Console.ReadLine();

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (var tx = this.StateManager.CreateTransaction())
                {
                    var result = await myDictionary.TryGetValueAsync(tx, "Counter");

                    ServiceEventSource.Current.ServiceMessage(this, "Current Counter Value: {0}",
                        result.HasValue ? result.Value.ToString() : "Value does not exist.");

                    await myDictionary.AddOrUpdateAsync(tx, "Counter", 0, (key, value) => ++value);

                    // If an exception is thrown before calling CommitAsync, the transaction aborts, all changes are 
                    // discarded, and nothing is saved to the secondary replicas.
                    await tx.CommitAsync();
                }

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }

        private static void AtLeastOnceDelivery(ActorSystem system)
        {
            ServiceEventSource.Current.Message("\n--- AT LEAST ONCE DELIVERY EXAMPLE ---\n");
            var delivery = system.ActorOf(Props.Create(() => new DeliveryActor()), "delivery");

            var deliverer = system.ActorOf(Props.Create(() => new AtLeastOnceDeliveryExampleActor(delivery.Path)));
            delivery.Tell("start");
            deliverer.Tell(new Message("foo"));


            System.Threading.Thread.Sleep(1000); //making sure delivery stops before send other commands
            delivery.Tell("stop");

            deliverer.Tell(new Message("bar"));

            ServiceEventSource.Current.Message("\nSYSTEM: Throwing exception in Deliverer\n");
            deliverer.Tell("boom");
            System.Threading.Thread.Sleep(1000);

            deliverer.Tell(new Message("bar1"));
            ServiceEventSource.Current.Message("\nSYSTEM: Enabling confirmations in 3 seconds\n");

            System.Threading.Thread.Sleep(3000);
            ServiceEventSource.Current.Message("\nSYSTEM: Enabled confirmations\n");
            delivery.Tell("start");

        }

        private static void ViewExample(ActorSystem system)
        {
            ServiceEventSource.Current.Message("\n--- PERSISTENT VIEW EXAMPLE ---\n");
            var pref = system.ActorOf(Props.Create<ViewExampleActor>());
            var view = system.ActorOf(Props.Create<ExampleView>());

            system.Scheduler.ScheduleTellRepeatedly(TimeSpan.Zero, TimeSpan.FromSeconds(2), pref, "scheduled", ActorRefs.NoSender);
            system.Scheduler.ScheduleTellRepeatedly(TimeSpan.Zero, TimeSpan.FromSeconds(5), view, "snap", ActorRefs.NoSender);
        }

        private static void SnapshotedActor(ActorSystem system)
        {
            ServiceEventSource.Current.Message("\n--- SNAPSHOTED ACTOR EXAMPLE ---\n");
            var pref = system.ActorOf(Props.Create<SnapshotedExampleActor>(), "snapshoted-actor");

            // send two messages (a, b) and persist them
            pref.Tell("a");
            pref.Tell("b");

            // make a snapshot: a, b will be stored in durable memory
            pref.Tell("snap");

            // send next two messages - those will be cleared, since MemoryJournal is not "persistent"
            pref.Tell("c");
            pref.Tell("d");

            // print internal actor's state
            pref.Tell("print");

            // result after first run should be like:

            // Current actor's state: d, c, b, a

            // after second run:

            // Offered state (from snapshot): b, a      - taken from the snapshot
            // Current actor's state: d, c, b, a, b, a  - 2 last messages loaded from the snapshot, rest send in this run

            // after third run:

            // Offered state (from snapshot): b, a, b, a        - taken from the snapshot
            // Current actor's state: d, c, b, a, b, a, b, a    - 4 last messages loaded from the snapshot, rest send in this run

            // etc...
        }

        private static void FailingActorExample(ActorSystem system)
        {
            ServiceEventSource.Current.Message("\n--- FAILING ACTOR EXAMPLE ---\n");
            var pref = system.ActorOf(Props.Create<ExamplePersistentFailingActor>(), "failing-actor");

            pref.Tell("a");
            pref.Tell("print");
            // restart and recovery
            pref.Tell("boom");
            pref.Tell("print");
            pref.Tell("b");
            pref.Tell("print");
            pref.Tell("c");
            pref.Tell("print");

            // Will print in a first run (i.e. with empty journal):

            // Received: a
            // Received: a, b
            // Received: a, b, c
        }

        private static void BasicUsage(ActorSystem system)
        {
            ServiceEventSource.Current.Message("\n--- BASIC EXAMPLE ---\n");
            ServiceEventSource.Current.Message("--- BASIC EXAMPLE ---");
            // create a persistent actor, using LocalSnapshotStore and MemoryJournal
            var aref = system.ActorOf(Props.Create<ExamplePersistentActor>(), "basic-actor");

            // all commands are stacked in internal actor's state as a list
            aref.Tell(new Command("foo"));
            aref.Tell(new Command("baz"));
            aref.Tell(new Command("bar"));

            // save current actor state using LocalSnapshotStore (it will be serialized and stored inside file on example bin/snapshots folder)
            aref.Tell("snap");

            // add one more message, this one is not snapshoted and won't be persisted (because of MemoryJournal characteristics)
            aref.Tell(new Command("buzz"));

            // print current actor state
            aref.Tell("print");

            aref.GracefulStop(TimeSpan.FromSeconds(10));
            // on first run displayed state should be: 

            // buzz-3, bar-2, baz-1, foo-0 
            // (numbers denotes current actor's sequence numbers for each stored event)

            // on the second run: 

            // buzz-6, bar-5, baz-4, foo-3, bar-2, baz-1, foo-0
            // (sequence numbers are continuously increasing taken from last snapshot, 
            // also buzz-3 event isn't present since it's has been called after snapshot request,
            // and MemoryJournal will destroy stored events on program stop)

            // on next run's etc...
        }
    }
}
