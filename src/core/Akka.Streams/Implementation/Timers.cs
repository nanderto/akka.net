//-----------------------------------------------------------------------
// <copyright file="Timers.cs" company="Akka.NET Project">
//     Copyright (C) 2015-2016 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2016 Akka.NET project <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Streams.Implementation.Fusing;
using Akka.Streams.Stage;

namespace Akka.Streams.Implementation
{
    /// <summary>
    /// INTERNAL API
    /// 
    /// Various stages for controlling timeouts on IO related streams (although not necessarily).
    /// 
    /// The common theme among the processing stages here that
    ///  - they wait for certain event or events to happen
    ///  - they have a timer that may fire before these events
    ///  - if the timer fires before the event happens, these stages all fail the stream
    ///  - otherwise, these streams do not interfere with the element flow, ordinary completion or failure
    /// </summary>
    internal static class Timers
    {
        public static TimeSpan IdleTimeoutCheckInterval(TimeSpan timeout)
            => new TimeSpan(Math.Min(Math.Max(timeout.Ticks/8, 100*TimeSpan.TicksPerMillisecond), timeout.Ticks/2));
    }

    internal sealed class Initial<T> : SimpleLinearGraphStage<T>
    {
        #region InitialStageLogic
        private sealed class Logic : TimerGraphStageLogic
        {
            private readonly Initial<T> _stage;
            private bool _initialHasPassed;

            public Logic(Initial<T> stage) : base(stage.Shape)
            {
                _stage = stage;

                SetHandler(stage.Inlet, onPush: () =>
                {
                    _initialHasPassed = true;
                    Push(stage.Outlet, Grab(stage.Inlet));
                });
                SetHandler(stage.Outlet, onPull: () => Pull(stage.Inlet));
            }

            protected internal override void OnTimer(object timerKey)
            {
                if (!_initialHasPassed)
                    FailStage(new TimeoutException($"The first element has not yet passed through in {_stage.Timeout}."));
            }

            public override void PreStart() => ScheduleOnce("InitialTimeoutTimer", _stage.Timeout);
        }
        #endregion

        public readonly TimeSpan Timeout;

        public Initial(TimeSpan timeout)
        {
            Timeout = timeout;
        }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);
    }

    internal sealed class Completion<T> : SimpleLinearGraphStage<T>
    {
        #region stage logic
        private sealed class Logic : TimerGraphStageLogic
        {
            private readonly Completion<T> _stage;

            public Logic(Completion<T> stage) : base(stage.Shape)
            {
                _stage = stage;
                SetHandler(stage.Inlet, onPush: () => Push(stage.Outlet, Grab(stage.Inlet)));
                SetHandler(stage.Outlet, onPull: () => Pull(stage.Inlet));
            }

            protected internal override void OnTimer(object timerKey)
                => FailStage(new TimeoutException($"The stream has not been completed in {_stage.Timeout}."));

            public override void PreStart() => ScheduleOnce("CompletionTimeoutTimer", _stage.Timeout);
        }
        #endregion

        public readonly TimeSpan Timeout;
        public Completion(TimeSpan timeout)
        {
            Timeout = timeout;
        }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);
    }

    internal sealed class Idle<T> : SimpleLinearGraphStage<T>
    {
        #region stage logic
        private sealed class Logic : TimerGraphStageLogic
        {
            private readonly Idle<T> _stage;
            private DateTime _nextDeadline;

            public Logic(Idle<T> stage) : base(stage.Shape)
            {
                _stage = stage;
                _nextDeadline = DateTime.UtcNow + stage.Timeout;

                SetHandler(stage.Inlet, onPush: () =>
                {
                    _nextDeadline = DateTime.UtcNow + stage.Timeout;
                    Push(stage.Outlet, Grab(stage.Inlet));
                });
                SetHandler(stage.Outlet, onPull: () => Pull(stage.Inlet));
            }

            protected internal override void OnTimer(object timerKey)
            {
                if (_nextDeadline <= DateTime.UtcNow)
                    FailStage(new TimeoutException($"No elements passed in the last {_stage.Timeout}."));
            }

            public override void PreStart()
                => ScheduleRepeatedly("IdleTimeoutCheckTimer", Timers.IdleTimeoutCheckInterval(_stage.Timeout));
        }
        #endregion

        public readonly TimeSpan Timeout;

        public Idle(TimeSpan timeout)
        {
            Timeout = timeout;
        }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);
    }

    internal sealed class IdleTimeoutBidi<TIn, TOut> : GraphStage<BidiShape<TIn, TIn, TOut, TOut>>
    {
        #region stage logic
        private sealed class Logic : TimerGraphStageLogic
        {
            private readonly IdleTimeoutBidi<TIn, TOut> _stage;
            private DateTime _nextDeadline;

            public Logic(IdleTimeoutBidi<TIn, TOut> stage) : base(stage.Shape)
            {
                _stage = stage;
                _nextDeadline = DateTime.UtcNow + _stage.Timeout;

                SetHandler(_stage.In1, onPush: () =>
                {
                    OnActivity();
                    Push(_stage.Out1, Grab(_stage.In1));
                },
                onUpstreamFinish: () => Complete(_stage.Out1));

                SetHandler(_stage.In2, onPush: () =>
                {
                    OnActivity();
                    Push(_stage.Out2, Grab(_stage.In2));
                },
                onUpstreamFinish: () => Complete(_stage.Out2));

                SetHandler(_stage.Out1,
                    onPull: () => Pull(_stage.In1),
                    onDownstreamFinish: () => Cancel(_stage.In1));

                SetHandler(_stage.Out2,
                    onPull: () => Pull(_stage.In2),
                    onDownstreamFinish: () => Cancel(_stage.In2));
            }

            protected internal override void OnTimer(object timerKey)
            {
                if (_nextDeadline <= DateTime.UtcNow)
                    FailStage(new TimeoutException($"No elements passed in the last {_stage.Timeout}."));
            }

            public override void PreStart()
                => ScheduleRepeatedly("IdleTimeoutBidiCheckTimer", Timers.IdleTimeoutCheckInterval(_stage.Timeout));

            private void OnActivity() => _nextDeadline = DateTime.UtcNow + _stage.Timeout;
        }
        #endregion

        public readonly TimeSpan Timeout;

        public readonly Inlet<TIn> In1 = new Inlet<TIn>("in1");
        public readonly Inlet<TOut> In2 = new Inlet<TOut>("in2");
        public readonly Outlet<TIn> Out1 = new Outlet<TIn>("out1");
        public readonly Outlet<TOut> Out2 = new Outlet<TOut>("out2");

        public IdleTimeoutBidi(TimeSpan timeout)
        {
            Timeout = timeout;
            Shape = new BidiShape<TIn, TIn, TOut, TOut>(In1, Out1, In2, Out2);
        }

        protected override Attributes InitialAttributes { get; } = Attributes.CreateName("IdleTimeoutBidi");

        public override BidiShape<TIn, TIn, TOut, TOut> Shape { get; }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);
    }

    internal sealed class DelayInitial<T> : GraphStage<FlowShape<T, T>>
    {
        #region stage logic
        private sealed class Logic : TimerGraphStageLogic
        {
            private const string DelayTimer = "DelayTimer";
            private readonly DelayInitial<T> _stage;
            private bool _isOpen;

            public Logic(DelayInitial<T> stage) : base(stage.Shape)
            {
                _stage = stage;
                SetHandler(_stage.In, onPush: () => Push(_stage.Out, Grab(_stage.In)));
                SetHandler(_stage.Out, onPull: () =>
                {
                    if (_isOpen)
                        Pull(_stage.In);
                });
            }

            protected internal override void OnTimer(object timerKey)
            {
                _isOpen = true;
                if (IsAvailable(_stage.Out))
                    Pull(_stage.In);
            }

            public override void PreStart()
            {
                if (_stage.Delay == TimeSpan.Zero)
                    _isOpen = true;
                else
                    ScheduleOnce(DelayTimer, _stage.Delay);
            }
        }
        #endregion

        public readonly TimeSpan Delay;
        public readonly Inlet<T> In = new Inlet<T>("DelayInitial.in");
        public readonly Outlet<T> Out = new Outlet<T>("DelayInitial.out");

        public DelayInitial(TimeSpan delay)
        {
            Delay = delay;
            Shape = new FlowShape<T, T>(In, Out);
        }

        protected override Attributes InitialAttributes { get; } = Attributes.CreateName("DelayInitial");

        public override FlowShape<T, T> Shape { get; }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);
    }

    internal sealed class IdleInject<TIn, TOut> : GraphStage<FlowShape<TIn, TOut>> where TIn : TOut
    {
        #region stage logic
        private sealed class Logic : TimerGraphStageLogic
        {
            private const string IdleTimer = "IdleInjectTimer";
            private readonly IdleInject<TIn, TOut> _stage;
            private DateTime _nextDeadline;

            public Logic(IdleInject<TIn, TOut> stage) : base(stage.Shape)
            {
                _stage = stage;
                _nextDeadline = DateTime.UtcNow + _stage._timeout;

                SetHandler(_stage._in, onPush: () =>
                {
                    _nextDeadline = DateTime.UtcNow + _stage._timeout;
                    CancelTimer(IdleTimer);
                    if (IsAvailable(_stage._out))
                    {
                        Push(_stage._out, Grab(_stage._in));
                        Pull(_stage._in);
                    }
                },
                onUpstreamFinish: () =>
                {
                    if(!IsAvailable(_stage._in))
                        CompleteStage();
                });

                SetHandler(_stage._out, onPull: () =>
                {
                    if (IsAvailable(_stage._in))
                    {
                        Push(_stage._out, Grab(_stage._in));
                        if (IsClosed(_stage._in))
                            CompleteStage();
                        else
                            Pull(_stage._in);
                    }
                    else
                    {
                        var timeLeft = _nextDeadline - DateTime.UtcNow;
                        if (timeLeft <= TimeSpan.Zero)
                        {
                            _nextDeadline = DateTime.UtcNow + _stage._timeout;
                            Push(_stage._out, _stage._inject());
                        }
                        else
                            ScheduleOnce(IdleTimer, timeLeft);
                    }
                });
            }

            protected internal override void OnTimer(object timerKey)
            {
                if (_nextDeadline <= DateTime.UtcNow && IsAvailable(_stage._out))
                {
                    Push(_stage._out, _stage._inject());
                    _nextDeadline = DateTime.UtcNow + _stage._timeout;
                }
            }

            // Prefetching to ensure priority of actual upstream elements
            public override void PreStart() => Pull(_stage._in);
        }
        #endregion

        private readonly TimeSpan _timeout;
        private readonly Func<TOut> _inject;
        private readonly Inlet<TIn> _in = new Inlet<TIn>("IdleInject.in");
        private readonly Outlet<TOut> _out = new Outlet<TOut>("IdleInject.out");

        public IdleInject(TimeSpan timeout, Func<TOut> inject)
        {
            _timeout = timeout;
            _inject = inject;
            
            Shape = new FlowShape<TIn, TOut>(_in, _out);
        }

        protected override Attributes InitialAttributes { get; } = Attributes.CreateName("IdleInject");

        public override FlowShape<TIn, TOut> Shape { get; }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);
    }
}