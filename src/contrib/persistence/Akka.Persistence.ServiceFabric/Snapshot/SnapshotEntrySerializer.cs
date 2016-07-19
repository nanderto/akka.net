using Akka.Serialization;
using Microsoft.ServiceFabric.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;

namespace Akka.Persistence.ServiceFabric.Snapshot
{
    public class SnapshotEntrySerializer : Wire.Serializer, IStateSerializer<SnapshotEntry>  
    {
        public Wire.Serializer Serializer { get; set; }
        public SnapshotEntrySerializer()
        {
            Serializer = new Wire.Serializer();
        }

        void IStateSerializer<SnapshotEntry>.Write(SnapshotEntry value, BinaryWriter writer)
        {
            Serializer.Serialize(value, writer.BaseStream);
        }

        SnapshotEntry IStateSerializer<SnapshotEntry>.Read(BinaryReader reader)
        {
            SnapshotEntry value = new SnapshotEntry();
            value = Serializer.Deserialize<SnapshotEntry>(reader.BaseStream);
            return value;
        }  
  
        void IStateSerializer<SnapshotEntry>.Write(SnapshotEntry currentValue, SnapshotEntry newValue, BinaryWriter writer)
        {  
            ((IStateSerializer<SnapshotEntry>)this).Write(newValue, writer);  
        }

        SnapshotEntry IStateSerializer<SnapshotEntry>.Read(SnapshotEntry baseValue, BinaryReader reader)
        {  
            return ((IStateSerializer<SnapshotEntry>)this).Read(reader);  
        }  
    }
}
