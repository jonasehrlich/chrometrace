import importlib.metadata

from .chrometrace import ProcessTracer, TraceEvent, TraceEventType, TraceSink

__version__ = importlib.metadata.version(__name__)
__all__ = ["TraceEventType", "TraceEvent", "TraceSink", "ProcessTracer"]
