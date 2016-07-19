//-----------------------------------------------------------------------
// <copyright file="OutputStreamSourceSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.IO;
using Akka.Streams.Dsl;
using Akka.Streams.Implementation;
using Akka.Streams.Implementation.IO;
using Akka.Streams.TestKit;
using Akka.Streams.TestKit.Tests;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Tests.IO
{
    public class OutputStreamSourceSpec : AkkaSpec
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromMilliseconds(300);

        private readonly ActorMaterializer _materializer;
        private readonly byte[] _bytesArray;
        private readonly ByteString _byteString;

        public OutputStreamSourceSpec(ITestOutputHelper helper) : base(Utils.UnboundedMailboxConfig, helper)
        {
            Sys.Settings.InjectTopLevelFallback(ActorMaterializer.DefaultConfig());
            var settings = ActorMaterializerSettings.Create(Sys).WithDispatcher("akka.actor.default-dispatcher");
            _materializer = Sys.Materializer(settings);

            _bytesArray = new[]
            {
                Convert.ToByte(new Random().Next(256)),
                Convert.ToByte(new Random().Next(256)),
                Convert.ToByte(new Random().Next(256))
            };

            _byteString = ByteString.Create(_bytesArray);
        }

        private void ExpectTimeout(Task f, TimeSpan duration) => f.Wait(duration).Should().BeFalse();

        private void ExpectSuccess<T>(Task<T> f, T value)
        {
            f.Wait(Timeout).Should().BeTrue();
            f.Result.Should().Be(value);
        }

        [Fact]
        public void OutputStreamSource_must_read_bytes_from_OutputStream()
        {
            this.AssertAllStagesStopped(() =>
            {
                var t = StreamConverters.AsOutputStream()
                        .ToMaterialized(this.SinkProbe<ByteString>(), Keep.Both)
                        .Run(_materializer);
                var outputStream = t.Item1;
                var probe = t.Item2;
                var s = probe.ExpectSubscription();

                outputStream.Write(_bytesArray, 0, _bytesArray.Length);
                s.Request(1);
                probe.ExpectNext(_byteString);
                outputStream.Close();
                probe.ExpectComplete();
            }, _materializer);
        }

        [Fact]
        public void OutputStreamSource_must_block_flush_call_until_send_all_buffer_to_downstream()
        {
            this.AssertAllStagesStopped(() =>
            {
                var t = StreamConverters.AsOutputStream()
                        .ToMaterialized(this.SinkProbe<ByteString>(), Keep.Both)
                        .Run(_materializer);
                var outputStream = t.Item1;
                var probe = t.Item2;
                var s = probe.ExpectSubscription();

                outputStream.Write(_bytesArray, 0, _bytesArray.Length);
                var f = Task.Run(() =>
                {
                    outputStream.Flush();
                    return NotUsed.Instance;
                });

                ExpectTimeout(f, Timeout);
                probe.ExpectNoMsg(TimeSpan.MinValue);

                s.Request(1);
                ExpectSuccess(f, NotUsed.Instance);
                probe.ExpectNext(_byteString);

                outputStream.Close();
                probe.ExpectComplete();
            }, _materializer);
        }

        [Fact]
        public void OutputStreamSource_must_not_block_flushes_when_buffer_is_empty()
        {
            this.AssertAllStagesStopped(() =>
            {
                var t = StreamConverters.AsOutputStream()
                        .ToMaterialized(this.SinkProbe<ByteString>(), Keep.Both)
                        .Run(_materializer);
                var outputStream = t.Item1;
                var probe = t.Item2;
                var s = probe.ExpectSubscription();

                outputStream.Write(_bytesArray, 0, _byteString.Count);
                var f = Task.Run(() =>
                {
                    outputStream.Flush();
                    return NotUsed.Instance;
                });
                s.Request(1);
                ExpectSuccess(f, NotUsed.Instance);
                probe.ExpectNext(_byteString);

                var f2 = Task.Run(() =>
                {
                    outputStream.Flush();
                    return NotUsed.Instance;
                });
                ExpectSuccess(f2, NotUsed.Instance);

                outputStream.Close();
                probe.ExpectComplete();

            }, _materializer);
        }
        
        [Fact]
        public void OutputStreamSource_must_block_writes_when_buffer_is_full()
        {
            this.AssertAllStagesStopped(() =>
            {
                var t = StreamConverters.AsOutputStream()
                    .WithAttributes(Attributes.CreateInputBuffer(16, 16))
                    .ToMaterialized(this.SinkProbe<ByteString>(), Keep.Both)
                    .Run(_materializer);
                var outputStream = t.Item1;
                var probe = t.Item2;
                var s = probe.ExpectSubscription();

                for (var i = 1; i <= 16; i++)
                    outputStream.Write(_bytesArray, 0, _byteString.Count);

                //blocked call
                var f = Task.Run(() =>
                {
                    outputStream.Write(_bytesArray, 0, _byteString.Count);
                    return NotUsed.Instance;
                });
                ExpectTimeout(f, Timeout);
                probe.ExpectNoMsg(TimeSpan.MinValue);

                s.Request(17);
                ExpectSuccess(f, NotUsed.Instance);
                probe.ExpectNextN(Enumerable.Repeat(_byteString, 17).ToList());

                outputStream.Close();
                probe.ExpectComplete();
            }, _materializer);
        }

        [Fact]
        public void OutputStreamSource_must_throw_error_when_writer_after_stream_is_closed()
        {
            this.AssertAllStagesStopped(() =>
            {
                var t = StreamConverters.AsOutputStream()
                        .ToMaterialized(this.SinkProbe<ByteString>(), Keep.Both)
                        .Run(_materializer);
                var outputStream = t.Item1;
                var probe = t.Item2;

                probe.ExpectSubscription();
                outputStream.Close();
                probe.ExpectComplete();

                outputStream.Invoking(s => s.Write(_bytesArray, 0, _byteString.Count)).ShouldThrow<IOException>();
            }, _materializer);
        }

        [Fact]
        public void OutputStreamSource_must_use_dedicated_default_blocking_io_dispatcher_by_default()
        {
            this.AssertAllStagesStopped(() =>
            {
                var sys = ActorSystem.Create("dispatcher-testing", Utils.UnboundedMailboxConfig);
                var materializer = sys.Materializer();

                try
                {
                    StreamConverters.AsOutputStream().RunWith(this.SinkProbe<ByteString>(), materializer);
                    ((ActorMaterializerImpl) materializer).Supervisor.Tell(StreamSupervisor.GetChildren.Instance,
                        TestActor);
                    var actorRef = ExpectMsg<StreamSupervisor.Children>()
                            .Refs.First(c => c.Path.ToString().Contains("outputStreamSource"));
                    Utils.AssertDispatcher(actorRef, "akka.stream.default-blocking-io-dispatcher");
                }
                finally
                {
                    Shutdown(sys);
                }

            }, _materializer);
        }

        [Fact]
        public void OutputStreamSource_must_throw_IOException_when_writing_to_the_stream_after_the_subscriber_has_cancelled_the_reactive_stream()
        {
            this.AssertAllStagesStopped(() =>
            {
                var sourceProbe = CreateTestProbe();
                var t =
                    TestSourceStage<ByteString, Stream>.Create(new OutputStreamSourceStage(Timeout), sourceProbe)
                        .ToMaterialized(this.SinkProbe<ByteString>(), Keep.Both)
                        .Run(_materializer);
                var outputStream = t.Item1;
                var probe = t.Item2;

                var s = probe.ExpectSubscription();

                outputStream.Write(_bytesArray, 0, _bytesArray.Length);
                s.Request(1);
                sourceProbe.ExpectMsg<GraphStageMessages.Pull>();

                probe.ExpectNext(_byteString);

                s.Cancel();
                sourceProbe.ExpectMsg<GraphStageMessages.DownstreamFinish>();

                Thread.Sleep(500);
                outputStream.Invoking(os => os.Write(_bytesArray, 0, _bytesArray.Length)).ShouldThrow<IOException>();
            }, _materializer);
        }

        [Fact]
        public void OutputStreamSource_must_fail_to_materialize_with_zero_sized_input_buffer()
        {
            new Action(
                () =>
                    StreamConverters.AsOutputStream(Timeout)
                        .WithAttributes(Attributes.CreateInputBuffer(0, 0))
                        .RunWith(Sink.First<ByteString>(), _materializer)).ShouldThrow<ArgumentException>();
            /*
             With Sink.First we test the code path in which the source
             itself throws an exception when being materialized. If
             Sink.Ignore is used, the same exception is thrown by
             Materializer.
             */
        }
    }
}
