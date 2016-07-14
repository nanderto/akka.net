using Akka.Actor;
using Akka.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AkkaPersistence.Actors
{
    public class LoggerActor :  ReceivePersistentActor
    {
        public class GetMessages { }

        private List<string> messages = new List<string>(); //INTERNAL STATE

        private int _msgsSinceLastSnapshot = 0;

        public LoggerActor()
        {
           
            Recover<string>(str => messages.Add(str));
            Recover<SnapshotOffer>(offer => {
                var messages = offer.Snapshot as List<string>;
                if (messages != null) // null check
                    messages = (List<string>) messages.Concat(messages);
            });

            Command<string>(str => Persist(str, s => 
            {
                messages.Add(str); //add msg to in-memory event store after persisting
                if (++_msgsSinceLastSnapshot % 10 == 0)
                {
                    //time to save a snapshot
                    SaveSnapshot(messages);
                }

                ServiceEventSource.Current.Message($"Received in Logging actor {str}");

            }));

            Command<SaveSnapshotSuccess>(success => 
            {
                ServiceEventSource.Current.Message($"Saved snapshot");
                // soft-delete the journal up until the sequence # at
                // which the snapshot was taken
                DeleteMessages(success.Metadata.SequenceNr);
            });

            Command<SaveSnapshotFailure>(failure => {
                // handle snapshot save failure...
                ServiceEventSource.Current.Message($"Snapshot failure");
            });

            IReadOnlyList<string> readOnlyList = new List<string>(messages);
            Command<GetMessages>(get => Sender.Tell(readOnlyList));


            //Receive<string>(str => messages.Add(str));
            //Receive<GetMessages>(get => Sender.Tell(new IReadOnlyList<string>(messages));
        }

        public LoggerActor(string message)
        {
            this.message = message;
        }

        public string message { get; private set; }

        private string persistenceId = string.Empty;
        public override string PersistenceId
        {
            get
            {
                if(string.IsNullOrEmpty(persistenceId))
                {
                    persistenceId = Guid.NewGuid().ToString();
                }

                return persistenceId;
            }
        }    
    }
}
