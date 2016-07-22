using System;
using Microsoft.ServiceFabric.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;
using Akka.Persistence.ServiceFabric.Journal;

namespace Akka.Persistence.ServiceFabric.Tests
{
    [TestClass]
    public class UnitTest1
    {
        private ITransaction tx = null;

        [TestMethod]
        public async Task TestMethod1()
        {
            var mockDictionary = new Mocks.MockReliableDictionary<int, int>();
            await mockDictionary.AddAsync(tx, 1, 33);
            var result = await mockDictionary.TryGetValueAsync(tx, 1);
            Assert.IsTrue(result.HasValue);
            Assert.AreEqual(33, result.Value);
        }

        [TestMethod]
        public async Task TestMethod2()
        {
            var system = Akka.Actor.ActorSystem.Create("MySystem");
            var Journal = system.ActorOf<ServiceFabricJournal>("Startup");
            
            var journal = new ServiceFabricJournal();
        }
    }
}
