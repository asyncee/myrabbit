import sys
from dataclasses import dataclass

import myrabbit
from myrabbit.contrib.opentelemetry.instrumentor import MyrabbitInstrumentor

url = "amqp://"
eb = myrabbit.EventBus(myrabbit.ReconnectingPublisherFactory(url))
cb = myrabbit.CommandBus(myrabbit.ReconnectingPublisherFactory(url))

a = myrabbit.Service("a", eb, cb,)
b = myrabbit.Service("b", eb, cb,)


@dataclass
class Cmd:
    name: str


@b.on_event("a", Cmd)
def on_command(cmd):
    # with trace.get_tracer(__name__).start_as_current_span("on-command"):
    print(cmd)


if __name__ == "__main__":
    from opentelemetry import trace
    from opentelemetry.exporter import zipkin
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchExportSpanProcessor
    from opentelemetry.sdk.trace.export import (
        ConsoleSpanExporter,
        SimpleExportSpanProcessor,
    )

    trace.set_tracer_provider(TracerProvider())
    tracer = trace.get_tracer(__name__)

    if sys.argv[1] == "server":
        zipkin_exporter = zipkin.ZipkinSpanExporter(service_name="server",)
        span_processor = BatchExportSpanProcessor(zipkin_exporter)
        trace.get_tracer_provider().add_span_processor(span_processor)
        trace.get_tracer_provider().add_span_processor(
            SimpleExportSpanProcessor(ConsoleSpanExporter())
        )

        MyrabbitInstrumentor().instrument_myrabbit(service_name="server")
        myrabbit.run_services_threaded(url, b)
    else:
        zipkin_exporter = zipkin.ZipkinSpanExporter(service_name="client",)
        span_processor = BatchExportSpanProcessor(zipkin_exporter)
        trace.get_tracer_provider().add_span_processor(span_processor)
        trace.get_tracer_provider().add_span_processor(
            SimpleExportSpanProcessor(ConsoleSpanExporter())
        )

        MyrabbitInstrumentor().instrument_myrabbit(service_name="client")

        # with trace.get_tracer(__name__).start_as_current_span("send-command"):
        a.publish(Cmd(name="test"))
