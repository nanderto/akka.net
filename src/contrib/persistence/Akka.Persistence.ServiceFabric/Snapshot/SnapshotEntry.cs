namespace Akka.Persistence.ServiceFabric.Snapshot
{
    using System.Runtime.Serialization;

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
