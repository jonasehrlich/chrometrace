import json
import tempfile

import pytest

import chrometrace.chrometrace as chrometrace


def read_trace_events(path: str) -> list[chrometrace.TraceEvent]:
    """Read a chrometrace file and return the created trace events"""
    with open(path) as f:
        return [chrometrace.TraceEvent.from_dict(event_dict) for event_dict in json.load(f)]


def test_process_tracer_basic() -> None:
    """Test basic process tracer trace creation"""
    sent_events: list[chrometrace.TraceEvent] = list()

    with tempfile.NamedTemporaryFile(suffix=".json", mode="w+") as file_like:
        with chrometrace.TraceSink(file_like.name) as trace_sink:
            process_tracer = trace_sink.process_tracer("kernel", 0)
            sent_events.append(process_tracer.complete("bootloader", 0, duration_us=1000))
        read_events = read_trace_events(file_like.name)
        # Drop the process naming event
        assert read_events[1:] == sent_events


def test_thread_tracer_basic() -> None:
    """Test basic thread tracer trace creation"""
    sent_events: list[chrometrace.TraceEvent] = list()

    with tempfile.NamedTemporaryFile(suffix=".json", mode="w+") as file_like:
        with chrometrace.TraceSink(file_like.name) as trace_sink:
            thread_tracer = trace_sink.process_tracer("kernel", 0).thread_tracer("MainThread", 0)
            sent_events.append(thread_tracer.complete("bootloader", 0, duration_us=1000))
        read_events = read_trace_events(file_like.name)
        # Drop the process and thread naming events
        assert read_events[2:] == sent_events


@pytest.mark.parametrize("num_traces", [1, 2])
def test_trace_sink_streaming(num_traces: int) -> None:
    """Test the streaming functionality of the trace sink"""
    sent_events: list[chrometrace.TraceEvent] = list()
    cache_size = 2
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w+") as file_like:
        with chrometrace.TraceSink(file_like.name, stream=True, cache_size=cache_size) as trace_sink:
            process_tracer = trace_sink.process_tracer("kernel", 0)
            # One trace event is cached
            assert trace_sink.num_cached_items() == 1
            for trace_idx in range(2, num_traces + 2):
                sent_events.append(
                    process_tracer.complete("bootloader", timestamp_us=trace_idx * 1000, duration_us=1000)
                )
                assert trace_sink.num_cached_items() == trace_idx % cache_size
        # Check that the cache is empty after the sink close
        assert trace_sink.num_cached_items() == 0
        read_events = read_trace_events(file_like.name)
        # Drop the process naming event
        assert read_events[1:] == sent_events
