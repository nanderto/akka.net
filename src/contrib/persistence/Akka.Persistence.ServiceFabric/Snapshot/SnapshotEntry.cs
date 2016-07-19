using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Akka.Persistence.ServiceFabric.Snapshot
{
    [DataContract]
    public class SnapshotEntry
    {
        [DataMember]
        public string Id { get; set; }

        [DataMember]
        public string PersistenceId { get; set; }

        [DataMember]
        public long SequenceNr { get; set; }

        [DataMember]
        public long Timestamp { get; set; }

        [DataMember]
        public object Snapshot { get; set; }
    }
}
