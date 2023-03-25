import collections.abc
import dataclasses
import enum
import functools
import json
import os
import typing as ty

__all__ = ["TraceEventType", "TraceEvent", "TraceSink", "ProcessTracer"]

JsonEncodable: ty.TypeAlias = ty.Any


class TraceEventType(enum.StrEnum):
    """
    Type of a trace event

    See https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview#heading=h.puwqg050lyuy
    for details
    """

    # Duration trace events
    BEGIN = "B"
    END = "E"
    # Complete trace event
    COMPLETE = "X"
    # Instant trace event
    INSTANT = "I"
    # Counter trace event
    COUNTER = "C"
    # Async trace events
    NESTABLE_ASYNC_BEGIN = "b"
    NESTABLE_ASYNC_END = "e"
    NESTABLE_ASYNC_INSTANT = "n"
    # Flow trace events
    FLOW_BEGIN = "s"
    FLOW_STEP = "t"
    FLOW_END = "f"
    # Metadata trace events
    METADATA = "M"
    # Sample trace event
    SAMPLE = "P"
    # Object trace events
    CREATE_OBJECT = "N"
    SNAPSHOT_OBJECT = "O"
    DELETE_OBJECT = "D"
    # Memory dump trace events
    MEMORY_DUMP_GLOBAL = "V"
    MEMORY_DUMP = "v"
    # Mark trace event
    MARK = "R"
    # Clock sync event
    CLOCK_SYNC = "c"


@dataclasses.dataclass(frozen=True)
class TraceEvent:
    """
    Description of a trace event, modeled after the Chrome trace event format.

    Available at https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview#
    """

    # Name of the event
    name: str
    # Type of the trace event
    event_type: TraceEventType = dataclasses.field(metadata={"chrometrace_key": "ph"})
    # Timestamp of the trace event
    timestamp_us: float = dataclasses.field(metadata={"chrometrace_key": "ts"})
    # Duration of the event in us, only applicable for complete events
    duration_us: float = dataclasses.field(default=0, metadata={"chrometrace_key": "dur"})
    # Process ID for the process that created this event
    process_id: int = dataclasses.field(default=0, metadata={"chrometrace_key": "pid"})
    # Thread ID for the process that created this event, keep it optional
    thread_id: int | None = dataclasses.field(default=None, metadata={"chrometrace_key": "tid"})
    # Trace event categories
    categories: str = dataclasses.field(default="", metadata={"chrometrace_key": "cat"})
    # Arguments for the trace event, can be arbitrary JSON encodable data depending on the event
    args: dict[str, JsonEncodable] = dataclasses.field(default_factory=dict)
    # Scope of the event, only applicable for instant events
    scope: ty.Literal["g", "p", "t", None] = dataclasses.field(default=None, metadata={"chrometrace_key": "s"})

    @classmethod
    @functools.lru_cache(maxsize=None)
    def fields(cls) -> tuple[dataclasses.Field[ty.Any], ...]:
        """Get a tuple of the dataclass fields, wrapped in a lru_cache"""
        return dataclasses.fields(cls)

    @classmethod
    @functools.lru_cache(maxsize=None)
    def key_attribute_map(cls) -> dict[str, str]:
        key_attribute_map: dict[str, str] = {}
        for field in cls.fields():
            if field.metadata:
                key = field.metadata.get("chrometrace_key", field.name)
            else:
                key = field.name
            key_attribute_map[key] = field.name
        return key_attribute_map

    def to_dict(self) -> dict[str, JsonEncodable]:
        """Create a JSON encodable dictionary from the trace event"""
        event_dict: dict[str, JsonEncodable] = {}
        for key, attribute in self.key_attribute_map().items():
            value = getattr(self, attribute)
            if value is None:
                continue

            event_dict[key] = value
        return event_dict

    @classmethod
    def from_dict(cls, data: dict[str, ty.Any]) -> ty.Self:
        """Create a trace event object from an encoded dictionary"""
        key_attribute_map = cls.key_attribute_map()
        kwargs: dict[str, ty.Any] = {}
        for key, value in data.items():
            attribute = key_attribute_map[key]
            kwargs[attribute] = value

        return cls(**kwargs)

    @classmethod
    def duration_begin(
        cls,
        name: str,
        timestamp_us: float,
        process_id: int,
        thread_id: int | None,
        categories: list[str] | None = None,
        args: dict[str, JsonEncodable] | None = None,
    ) -> ty.Self:
        """
        Create a duration begin event

        Duration events provide a way to mark a duration of work on a given thread. The timestamps for the duration
        events must be in increasing order for a given thread. Timestamps in different threads do not have to be in
        increasing order, just the timestamps within a given thread.

        :param name: Name of the work on the thread
        :type name: str
        :param categories: Categories of the work on the thread
        :type categories: list[str] | None
        :param timestamp_us: Timestamp in us of the begin of the work
        :type timestamp_us: float
        :param process_id: ID of the process the work is happening in
        :type process_id: int
        :param thread_id: Thread ID the work is happening in
        :type thread_id: int | None
        :param args: Any arguments provided for the event
        :type args: dict[str, JsonEncodable] | None
        :return: Trace event marking the begin of a duration
        :rtype: ty.Self
        """

        return cls(
            name=name,
            categories=",".join(categories or []),
            timestamp_us=timestamp_us,
            process_id=process_id,
            thread_id=thread_id,
            args=args or {},
            event_type=TraceEventType.BEGIN,
        )

    @classmethod
    def duration_end(
        cls: ty.Type[ty.Self],
        name: str,
        timestamp_us: float,
        process_id: int,
        thread_id: int | None,
        categories: list[str] | None = None,
        args: dict[str, JsonEncodable] | None = None,
    ) -> ty.Self:
        """
        Create a duration end event

        Duration events provide a way to mark a duration of work on a given thread. The timestamps for the duration
        events must be in increasing order for a given thread. Timestamps in different threads do not have to be in
        increasing order, just the timestamps within a given thread.

        :param name: Name of the work on the thread
        :type name: str
        :param timestamp_us: Timestamp in us of the end of the work
        :type timestamp_us: float
        :param process_id: ID of the process the work is happening in
        :type process_id: int
        :param thread_id: Thread ID the work is happening in
        :type thread_id: int | None
        :param categories: Categories of the work on the thread
        :type categories: list[str] | None
        :param args: Any arguments provided for the event
        :type args: dict[str, JsonEncodable] | None
        :return: Trace event marking the end of a duration
        :rtype: ty.Self
        """
        return cls(
            name=name,
            categories=",".join(categories or []),
            timestamp_us=timestamp_us,
            process_id=process_id,
            thread_id=thread_id,
            args=args or {},
            event_type=TraceEventType.END,
        )

    @classmethod
    def complete(
        cls,
        name: str,
        timestamp_us: float,
        duration_us: float,
        process_id: int,
        thread_id: int | None,
        categories: list[str] | None = None,
        args: dict[str, JsonEncodable] | None = None,
    ) -> ty.Self:
        """
        Create a complete event

        Complete events are a combination of duration begin and end events. The usage of complete events can greatly
        reduce the trace size when a lot of duration events are used.

        :param name: Name of the work on the thread
        :type name: str
        :param timestamp_us: Timestamp in us of the begin of the work
        :type timestamp_us: float
        :param duration_us: Duration of the work in us
        :type duration_us: float
        :param process_id: ID of the process the work is happening in
        :type process_id: int
        :param thread_id: Thread ID the work is happening in
        :type thread_id: int | None
        :param categories: Categories of the work on the thread
        :type categories: list[str] | None
        :param args: Any arguments provided for the event
        :type args: dict[str, JsonEncodable] | None
        :return: Trace event marking  a duration
        :rtype: ty.Self
        """
        return cls(
            name=name,
            categories=",".join(categories or []),
            timestamp_us=timestamp_us,
            duration_us=duration_us,
            process_id=process_id,
            thread_id=thread_id,
            args=args or {},
            event_type=TraceEventType.COMPLETE,
        )

    @classmethod
    def instant_global_scope(cls, name: str, timestamp_us: float, categories: list[str] | None = None) -> ty.Self:
        """
        Create an instant event with global scope

        The instant events correspond to something that happens but has no duration associated with it.
        The global scope makes the event shown in the trace viewer through all processes and threads.

        :param name: Name of the event
        :type name: str
        :param timestamp_us: Timestamp of the event in us
        :type timestamp_us: float
        :param categories: Categories of the event
        :type categories: list[str] | None
        :return: Trace event marking a global instant event
        :rtype: ty.Self
        """
        categories = categories or []
        return cls(
            name=name,
            categories=",".join(categories or []),
            timestamp_us=timestamp_us,
            event_type=TraceEventType.INSTANT,
            scope="g",
        )

    @classmethod
    def instant_process_scope(
        cls, name: str, timestamp_us: float, process_id: int, categories: list[str] | None = None
    ) -> ty.Self:
        """
        Create an instant event with process scope

        The instant events correspond to something that happens but has no duration associated with it.
        The global scope makes the event shown in the trace viewer through all processes and threads.

        :param name: Name of the event
        :type name: str
        :param timestamp_us: Timestamp of the event in us
        :type timestamp_us: float
        :param process_id: ID of the process the event is happening in
        :type process_id: int
        :param categories: Categories of the event
        :type categories: list[str] | None
        :return: Trace event marking an instant event with process scope
        :rtype: ty.Self
        """
        return cls(
            name=name,
            categories=",".join(categories or []),
            timestamp_us=timestamp_us,
            process_id=process_id,
            event_type=TraceEventType.INSTANT,
            scope="p",
        )

    @classmethod
    def instant_thread_scope(
        cls, name: str, timestamp_us: float, process_id: int, thread_id: int, categories: list[str] | None = None
    ) -> ty.Self:
        """
        Create an instant event with process scope

        The instant events correspond to something that happens but has no duration associated with it.
        The global scope makes the event shown in the trace viewer through all processes and threads.

        :param name: Name of the event
        :type name: str
        :param timestamp_us: Timestamp of the event in us
        :type timestamp_us: float
        :param process_id: ID of the process the event is happening in
        :type process_id: int
        :param thread_id: ID of the thread the event is happening in
        :type thread_id: int
        :param categories: Categories of the event
        :type categories: list[str] | Non
        :return: Trace event marking an instant event with process scope
        :rtype: ty.Self
        """
        return cls(
            name=name,
            categories=",".join(categories or []),
            timestamp_us=timestamp_us,
            process_id=process_id,
            thread_id=thread_id,
            event_type=TraceEventType.INSTANT,
            scope="t",
        )

    @classmethod
    def counter(
        cls,
        name: str,
        timestamp_us: float,
        process_id: int,
        thread_id: int | None,
        categories: list[str] | None = None,
        args: dict[str, JsonEncodable] | None = None,
    ) -> ty.Self:
        """
        Create a counter event for a thread on a process

        The counter events can track a value or multiple values as they change over time.
        Each counter can be provided with multiple series of data to display.
        When multiple series are provided they will be displayed as a stacked area chart in Trace Viewer.
        When an id field exists, the combination of the event name and id is used as the counter name.

        :param name: Name of the counter
        :type name: str
        :param timestamp_us: Timestamp the counter values change
        :type timestamp_us: float
        :param process_id: ID of the process the counter belongs to
        :type process_id: int
        :param thread_id: ID of the thread the counter belongs to
        :type thread_id: int | None
        :param categories: List of categories
        :type categories: list[str] | None
        :param args: Dictionary with series to track as keys and their value at timestamp as values
        :type args: dict[str, JsonEncodable] | None
        :return: Trace event marking a change in counter values
        :rtype: ty.Self
        """
        return cls(
            name=name,
            categories=",".join(categories or []),
            timestamp_us=timestamp_us,
            process_id=process_id,
            thread_id=thread_id,
            args=args or {},
            event_type=TraceEventType.COUNTER,
        )

    @classmethod
    def process_name(
        cls,
        process_id: int,
        process_name: str,
    ) -> ty.Self:
        """
        Create a metadata event to set the name of a process

        :param process_id: ID of the process
        :type process_id: int
        :param process_name: Name of the process
        :type process_name: str
        :return: Metadata event setting the name of a process
        :rtype: ty.Self
        """
        return cls(
            name="process_name",
            process_id=process_id,
            args={"name": process_name},
            event_type=TraceEventType.METADATA,
            timestamp_us=0,  # Do not care
        )

    @classmethod
    def process_sort_index(
        cls,
        process_id: int,
        sort_index: int,
    ) -> ty.Self:
        """
        Create a metadata event to set the sort index of a process

        The `sort_index` argument value specifies the relative position you'd like the item to be displayed.
        Lower numbers are displayed higher in Trace Viewer. If multiple items all have the same sort index
        then they are displayed sorted by name and, given duplicate names, by id.

        :param process_id: ID of the process
        :type process_id: int
        :param sort_index: Process sort order position
        :type sort_index: int
        :return: Metadata event setting the sort index of a process
        :rtype: ty.Self
        """
        return cls(
            name="process_sort_index",
            process_id=process_id,
            args={"sort_index": sort_index},
            event_type=TraceEventType.METADATA,
            timestamp_us=0,  # Do not care
        )

    @classmethod
    def process_labels(
        cls,
        process_id: int,
        labels: list[str],
    ) -> ty.Self:
        """
        Create a metadata event to set the labels of a process

        :param process_id: ID of the process
        :type process_id: int
        :param labels: Labels to set for the process
        :type labels: list[str]
        :return: Metadata event setting the labels of a process
        :rtype: ty.Self
        """
        return cls(
            name="process_name",
            process_id=process_id,
            args={"labels": ",".join(labels)},
            event_type=TraceEventType.METADATA,
            timestamp_us=0,  # Do not care
        )

    @classmethod
    def thread_name(cls, process_id: int, thread_id: int, thread_name: str) -> ty.Self:
        """
        Create a metadata event to set the name for a thread

        :param process_id: ID of the process
        :type process_id: int
        :param thread_id: ID of the thread
        :type thread_id: int
        :param thread_name: Name of the thread
        :type thread_name: str
        :return: Metadata event setting the name of a thread
        :rtype: ty.Self
        """
        return cls(
            name="thread_name",
            process_id=process_id,
            thread_id=thread_id,
            args={"name": thread_name},
            event_type=TraceEventType.METADATA,
            timestamp_us=0,  # Do not care
        )

    @classmethod
    def thread_sort_index(
        cls,
        process_id: int,
        thread_id: int,
        sort_index: int,
    ) -> ty.Self:
        """
        Create a metadata event to set the sort index of a thread.

        The `sort_index` argument value specifies the relative position you'd like the item to be displayed.
        Lower numbers are displayed higher in Trace Viewer. If multiple items all have the same sort index
        then they are displayed sorted by name and, given duplicate names, by id.

        :param process_id: ID of the process
        :type process_id: int
        :param thread_id: ID of the thread
        :type thread_id: int
        :param sort_index: Thread sort order position
        :type sort_index: int
        :return: Metadata event setting the sort index of a process
        :rtype: ty.Self
        """
        return cls(
            name="process_sort_index",
            process_id=process_id,
            thread_id=thread_id,
            args={"sort_index": sort_index},
            event_type=TraceEventType.METADATA,
            timestamp_us=0,  # Do not care
        )


class _TraceWriter:
    """Trace writer for the array trace format"""

    def __init__(self, path: os.PathLike[str] | str) -> None:
        self._path = path
        self._file_handle: ty.TextIO | None = None

    def open(self):
        """Open the chrometrace output file and write the opening square bracket"""
        if not self.opened():
            self._file_handle = open(self._path, "w")
            self._file_handle.write("[")

    def opened(self) -> bool:
        """Return whether the file is opened"""
        return not self.closed()

    def close(self) -> None:
        """
        Flush and close this trace sink. This method has no effect if the sink is already closed.
        Once the file is closed, any operation on the file (e.g. reading or writing) will raise a ValueError.

        As a convenience, it is allowed to call this method more than once; only the first call, however,
        will have an effect.
        """
        if self.opened():
            self._file_handle = ty.cast(ty.TextIO, self._file_handle)
            # Remove the last two characters (", ") from the file because JSON does not allow trailing
            # commas in arrays or objects
            self._file_handle.seek(self._file_handle.tell() - 2)
            self._file_handle.truncate()
            # Close the array, even though it would not be required according to the chrometrace specification
            self._file_handle.write("]")
            self._file_handle.close()
            self._file_handle = None

    def closed(self) -> bool:
        """Return whether the file is closed"""
        return self._file_handle is None

    def write(self, trace_events: collections.abc.Iterable[TraceEvent]) -> None:
        if self.closed():
            raise ValueError("I/O operation on closed file.")

        self._file_handle = ty.cast(ty.TextIO, self._file_handle)

        self._file_handle.write(json.dumps([event.to_dict() for event in trace_events])[1:-1])
        self._file_handle.write(", ")


class TraceSink:
    """
    Sink for collecting chrometrace events and writing them to a file in the JSON array format.

    The recommended way to use this trace sink is as a contextmanager. This ensures that all created traces are
    written when the contextmanager is exited, even if the program exits due to an exception.

    See the following example:

    .. code-block:: python

       with TraceSink("my-traces.json", stream=True) as trace_sink:
           trace_sink.add_event(TraceEvent.process_name(0, "my-process"))

    """

    def __init__(self, path: os.PathLike[str] | str, stream: bool = False, cache_size: int = 20) -> None:
        """
        Initialize a chrometrace sink

        :param path: File to write the data to
        :type path: os.PathLike[str] | str
        :param stream: Whether to write trace events to the output file if the cache size is >= `cache_size` ,
                       defaults to False. This will open the output file when the trace sink is opened.
        :type stream: bool, optional
        :param cache_size: Size of the internal cache, only applicable if `stream` is set to `True`, defaults to 20
        :type cache_size: int, optional
        """
        self._stream = stream
        self._cache_size = cache_size
        self._trace_events: list[TraceEvent] = list()
        self._trace_writer = _TraceWriter(path)

    def add_trace_event(self, trace_event: TraceEvent) -> None:
        """
        Add a event to the trace sink.

        If the following conditions are fulfilled, the trace cache is flushed:
            * The `TraceSink` is opened
            * More events than the cache size are in the cache
            * Streaming is enabled
        """
        self._trace_events.append(trace_event)
        if self._stream and len(self._trace_events) >= self._cache_size and self._trace_writer.opened():
            self.flush()

    def num_cached_items(self) -> int:
        """Returns the number of currently cached items"""
        return len(self._trace_events)

    def __enter__(self) -> ty.Self:
        if self._stream:
            # If we stream data to the trace writer, keep the file open
            self._trace_writer.open()
        return self

    def __exit__(self, exc_type: ty.Type[Exception], exc_value: Exception, tb: ty.Any):
        if self._trace_writer.closed():
            # If we are not streaming, the TraceWriter (alas the output file) was never opened
            self._trace_writer.open()
        self.close()

    def flush(self) -> None:
        """
        Flush the trace events to the trace writer.

        """
        if not self._trace_events:
            # Nothing to flush
            return

        self._trace_writer.write(self._trace_events)
        self._trace_events = []

    def close(self) -> None:
        """
        Flush and close this trace sink. This method has no effect if the sink is already closed.
        Once the file is closed, any operation on the file (e.g. reading or writing) will raise a ValueError.

        As a convenience, it is allowed to call this method more than once; only the first call, however,
        will have an effect.
        """
        if self._trace_writer.opened():
            self.flush()
        self._trace_writer.close()

    def process_tracer(self, process_name: str, process_id: int):
        """
        Create a process tracer object from this trace sink

        :param process_name: Name of the process
        :type process_name: str
        :param process_id: ID of the process
        :type process_id: int
        :return: Process ID instance connected to this trace sink
        :rtype: ProcessTracer
        """
        return ProcessTracer(process_name=process_name, process_id=process_id, trace_sink=self)


P = ty.ParamSpec("P")
ThreadIDType = ty.TypeVar("ThreadIDType", int, None)
ThreadNameType = ty.TypeVar("ThreadNameType", str, None)


class _Tracer(ty.Generic[ThreadNameType, ThreadIDType]):
    """Base class for tracers"""

    def __init__(
        self,
        process_name: str,
        process_id: int,
        thread_name: ThreadNameType,
        thread_id: ThreadIDType,
        trace_sink: TraceSink,
    ) -> None:
        self._process_name = process_name
        self._process_id = process_id
        self._thread_name = thread_name
        self._thread_id = thread_id

        self._trace_sink = trace_sink

        # Create trace methods on the tracer object
        self.duration_begin = self._get_trace_method(
            functools.partial(TraceEvent.duration_begin, process_id=self._process_id, thread_id=self._thread_id)
        )
        self.duration_end = self._get_trace_method(
            functools.partial(TraceEvent.duration_end, process_id=self._process_id, thread_id=self._thread_id)
        )
        self.complete = self._get_trace_method(
            functools.partial(TraceEvent.complete, process_id=self._process_id, thread_id=self._thread_id)
        )
        self.instant_global_scope = self._get_trace_method(functools.partial(TraceEvent.instant_global_scope))
        self.instant_process_scope = self._get_trace_method(
            functools.partial(TraceEvent.instant_process_scope, process_id=self._process_id)
        )
        self.counter = self._get_trace_method(
            functools.partial(TraceEvent.counter, process_id=self._process_id, thread_id=thread_id)
        )

    def _get_trace_method(self, trace_event_getter: ty.Callable[P, TraceEvent]) -> ty.Callable[P, TraceEvent]:
        """
        Return a function that creates a :py:class:`TraceEvent` based on the `trace_event_getter` and the arguments
        it gets passed, adds it to the internal :py:class:`TraceSink` and returns it.

        :param trace_event_getter: Callable that will return the TraceEvent when called with additional arguments
        :type trace_event_getter: ty.Callable[P, TraceEvent]
        :return: Function that calls `trace_event_getter` together with its `*args` and `**kwargs`
        :rtype: ty.Callable[P, TraceEvent]
        """

        @functools.wraps(trace_event_getter)
        def trace(*args: P.args, **kwargs: P.kwargs) -> TraceEvent:
            trace = trace_event_getter(*args, **kwargs)
            self._trace_sink.add_trace_event(trace)
            return trace

        return trace


class _ThreadTracer(_Tracer[str, int]):
    """Tracer for a thread inside a process

    Create this object from a :py:class:`ProcessTracer`.

    ..code-block:: python

       with TraceSink("my-traces.json") as trace_sink:
           process_tracer = trace_sink.process_tracer("my-process", 0)
           thread_tracer = process_tracer.thread_tracer("my-thread", 0)
    """

    def __init__(
        self, process_name: str, process_id: int, thread_name: str, thread_id: int, trace_sink: TraceSink
    ) -> None:
        super().__init__(process_name, process_id, thread_name, thread_id, trace_sink)

        # Name the thread in the trace file
        self._trace_sink.add_trace_event(
            TraceEvent.thread_name(
                process_id=self._process_id, thread_id=self._thread_id, thread_name=self._thread_name
            )
        )

        self.instant_thread_scope = self._get_trace_method(
            functools.partial(TraceEvent.instant_thread_scope, process_id=self._process_id, thread_id=thread_id)
        )
        self.sort_index = self._get_trace_method(
            functools.partial(TraceEvent.thread_sort_index, process_id=self._process_id, thread_id=thread_id)
        )


class ProcessTracer(_Tracer[None, None]):
    """
    Tracer for a process
    """

    def __init__(self, process_name: str, process_id: int, trace_sink: TraceSink) -> None:
        super().__init__(process_name, process_id, thread_name=None, thread_id=None, trace_sink=trace_sink)
        # Name the process in the trace file
        self._trace_sink.add_trace_event(
            TraceEvent.process_name(process_id=self._process_id, process_name=self._process_name)
        )

        self.sort_index = self._get_trace_method(
            functools.partial(TraceEvent.process_sort_index, process_id=self._process_id)
        )

    def thread_tracer(self, thread_name: str, thread_id: int) -> _ThreadTracer:
        """
        Create a thread tracer connected to this process.

        :param thread_name: Name of the thread
        :type thread_name: str
        :param thread_id: ID of the thread
        :type thread_id: int
        :return: Thread tracer instance
        :rtype: _ThreadTracer
        """
        return _ThreadTracer(
            self._process_name,
            self._process_id,
            thread_name=thread_name,
            thread_id=thread_id,
            trace_sink=self._trace_sink,
        )
