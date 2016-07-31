This implementation of Service Fabric persistence embeds the Service Fabric reliable collections in the actor (AsyncWriteJounrnal) responsible for persistence.

How to use:

1) reference the Akka.Persistence.ServiceFabric project / nuget (not yet available)

2) Create a stateful service in the standard service fabric way.

3) Find the class that inherits from the StatefulServiuce and replace it with inheriting from AkkaStatefulService.

4) Create your Actor System in the RunAsync method. COnfiguration can be done in line orwith config files as is standard in Akka.Net
