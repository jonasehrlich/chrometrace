# chrometrace

A Python library for creating Chrome Trace Viewer files. The Chrome Trace Viewer can be opened in
Chromium-based browsers (e.g. Google Chrome, Microsoft Edge, Chromium, ...) by entering _chrome://tracing_ into the
address bar.

The trace event format and types are defined in
[this document](https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU).

## Installation

Install the package from PyPi using the following command.

```sh
pip install chrometrace
```

## Usage

```python
import chrometrace

# Create the trace sink as a context manager
with chrometrace.TraceSink(file_like.name) as trace_sink:
    # Create a process tracer called myapp from the trace sink
    myapp_tracer = trace_sink.process_tracer("myapp", process_id=1337)
    # Create a thread tracer for the renderer thread from the process tracer
    renderer_thread_tracer = myapp_tracer.thread_tracer("RendererThread", 1)

    # Write a complete event at 10 us taking 1000 us with the name my_function
    renderer_thread_tracer.complete("my_function", timestamp_us=10, duration_us=1000)
```

## Supported trace formats

Currently only the _JSON Array Format_ is supported due to its simplicity. Support for the _JSON Object Format_ might be
added in the future.

## Supported trace events

* [x] Duration Events
  * Begin
  * End
* [x] Complete Events
* [x] Instant Events
  * With different scopes
* [x] Counter Events
* [ ] Async Events
* [ ] Flow Events
* [x] Metadata Events
  * Name process / thread
  * Define process / thread sort index
  * Process labels
* [ ] Sample Events
* [ ] Object Events
* [ ] Mark Events
* [ ] Clock Sync Events
* [ ] Context Events
