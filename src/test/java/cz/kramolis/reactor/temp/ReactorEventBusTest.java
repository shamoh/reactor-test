package cz.kramolis.reactor.temp;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Processors;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.registry.Registration;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;
import reactor.rx.Streams;

import static org.junit.Assert.assertTrue;
import static reactor.bus.selector.Selectors.$;

/**
 * This is just manual test to walk through Reactor EventBus functionality.
 * @author Libor Kramolis
 */
public class ReactorEventBusTest {

    private static final String MODULE_A = "(A) ";
    private static final String MODULE_B = "(B)                                       ";
    private static final String MODULE_C = "(C)                                                                              ";
    private static final String MODULE_D = "(D)                                                                                                                     ";

    //
    // # Publish/Subscribe
    //

    @Test
    public void testHelloWorld() {
        // new default EventBus instance
        final EventBus bus = EventBus.create();

        // listen on "topic"
        bus.on($("topic"), (Event<String> ev) -> {
            String s = ev.getData();
            log(MODULE_A, "New topic event: " + s);
        });

        // publish new event into "topic"
        bus.notify("topic", Event.wrap("Hello World!"));
    }

    @Test
    public void testHelloWorldHotStream() {
        final EventBus bus = EventBus.create();

        // publish before any registered listeners
        bus.notify("topic", Event.wrap("Hello World!"));

        // does not receive any event yet
        bus.on($("topic"), (Event<String> ev) -> {
            String s = ev.getData();
            log(MODULE_A, "New topic event: " + s);
        });

        // this is first event to be received
        bus.notify("topic", Event.wrap("Cruel World!"));
    }

    @Test
    public void testHelloWorldMoreListeners() {
        final EventBus bus = EventBus.create();
        final String topic = "topic";

        // more listeners on same topic
        bus.on($(topic), (Event<String> ev) -> {
            String s = ev.getData();
            log(MODULE_A, "New topic event: " + s);
        });
        bus.on($(topic), (Event<String> ev) -> {
            String s = ev.getData();
            log(MODULE_B, "New topic event: " + s);
        });
        bus.on($(topic), (Event<String> ev) -> {
            String s = ev.getData();
            log(MODULE_C, "New topic event: " + s);
        });

        bus.notify(topic, Event.wrap("Hello World!"));
    }

    // ## Selectors

    @Test
    public void testHelloWorldSelectors() {
        final EventBus bus = EventBus.create();

        // Select on OBJECT == $
        bus.on(Selectors.object("topic"), (Event<String> ev) -> {
            String s = ev.getData();
            log(MODULE_A, "New topic event: " + s);
        });
        // Select by type
        bus.on(Selectors.type(String.class), (Event<String> ev) -> {
            String s = ev.getData();
            log(MODULE_B, "New topic event: " + s);
        });
        // Select ANY
        bus.on(Selectors.matchAll(), (Event<String> ev) -> {
            String s = ev.getData();
            log(MODULE_C, "New topic event: " + s);
        });
        // Select anonymous
        Selector anonymous = Selectors.anonymous();
        bus.on(anonymous, (Event<String> ev) -> {
            String s = ev.getData();
            log(MODULE_D, "New topic event: " + s);
        });

        // Publish to all my listeners
        bus.notify("topic", Event.wrap("Hello World!"));

        // Publish to ANY and String
        bus.notify("topic2", Event.wrap("Cruel World!"));

        // Publish to just ANY
        bus.notify(new Object(), Event.wrap("Hello Pink Floyd!"));

        // Publish to anonymous object
        bus.notify(anonymous.getObject(), Event.wrap("Hello NoName!"));

        // Other Selectors: regex, predicate, uri
    }

    //
    // # Request/Reply
    //

    @Test
    public void testRequestReply() {
        final EventBus bus = EventBus.create();

        { // module A
            // listen on REPLY event
            bus.on($("reply.sink"), (Event<String> ev) -> {
                String s = ev.getData();
                log(MODULE_A, "Got reply event: " + s);
            });
        }
        { // module B
            // listen on topic and RETURN response
            bus.receive($("job.sink"), (Event<String> ev) -> {
                String s = ev.getData();
                log(MODULE_B, "Received event: " + s);
                return s.toUpperCase();
            });
        }
        { // module A
            // publish new event and expect reply on "reply.sink"
            bus.send("job.sink", Event.wrap("Hello World!", "reply.sink"));
        }
    }

    @Test
    public void testRequestReplyMoreListeners() {
        final EventBus bus = EventBus.create();

        { // module A
            // listen on REPLY event
            bus.on($("reply.sink"), (Event<String> ev) -> {
                String s = ev.getData();
                log(MODULE_A, "Got reply event: " + s);
            });
        }
        { // module B
            // listen on topic and RETURN response
            bus.receive($("job.sink"), (Event<String> ev) -> {
                String s = ev.getData();
                log(MODULE_B, "Received event: " + s);
                return s.toUpperCase();
            });
        }
        { // module C
            // listen on topic and RETURN response
            bus.receive($("job.sink"), (Event<String> ev) -> {
                String s = ev.getData();
                log(MODULE_C, "Received event: " + s);
                return s.toLowerCase();
            });
        }
        { // module A
            // publish new event and expect reply on "reply.sink"
            bus.send("job.sink", Event.wrap("Hello World!", "reply.sink"));
        }
    }

    @Test
    public void testRequestReplyMoreListenersNoResponse() {
        final EventBus bus = EventBus.create();

        { // module A
            bus.on($("reply.sink"), (Event<String> ev) -> {
                String s = ev.getData();
                log(MODULE_A, "Got reply event: " + s);
            });
        }
        { // module B
            bus.receive($("job.sink"), (Event<String> ev) -> {
                String s = ev.getData();
                log(MODULE_B, "Received event: " + s);
                return s.toUpperCase();
            });
        }
        { // module C
            // listen on topic but NO response
            bus.on($("job.sink"), (Event<String> ev) -> {
                String s = ev.getData();
                log(MODULE_C, "Received event: " + s);
            });
        }
        { // module A
            bus.send("job.sink", Event.wrap("Hello World!", "reply.sink"));
        }
    }

    @Test
    public void testRequestReplyNoReplyTopic() {
        final EventBus bus = EventBus.create();

        { // module B
            // listen on topic and RETURN response
            bus.receive($("job.sink"), (Event<String> ev) -> {
                String s = ev.getData();
                log(MODULE_B, "Received event: " + s);
                return s.toUpperCase();
            });
        }
        { // module A
            // publish new event and register REPLY lambda
            bus.sendAndReceive(
                    "job.sink",
                    Event.wrap("Hello World!"),
                    (Event<String> ev) -> {
                        String s = ev.getData();
                        log(MODULE_A, "Got reply event: " + s);
                    });
        }
    }

    //
    // ## Cancel registration
    //

    @Test
    public void testPause() {
        final EventBus bus = EventBus.create();

        // store Registration
        Registration reg = bus.on($("topic"), (Event<String> ev) -> {
            String s = ev.getData();
            log(MODULE_A, "New topic event: " + s);
        });

        bus.notify("topic", Event.wrap("Hello World!"));

        // ...some time later...
        reg.pause();

        // ...some time later...
        bus.notify("topic", Event.wrap("Cruel World!"));
    }

    //
    // ## Cancel registration
    //

    @Test
    public void testCancel() {
        final EventBus bus = EventBus.create();

        // store Registration
        Registration reg = bus.on($("topic"), (Event<String> ev) -> {
            String s = ev.getData();
            log(MODULE_A, "New topic event: " + s);
        });

        bus.notify("topic", Event.wrap("Hello World!"));

        // ...some time later...
        reg.cancel();

        // ...some time later...
        bus.notify("topic", Event.wrap("Cruel World!"));
    }

    // # Init EventBus with Processor

    @Test
    public void testHelloWorldAsyncProcessor() throws InterruptedException {
        final EventBus bus = EventBus.create(Processors.topic());
        final String topic = "topic";
        CountDownLatch latch = new CountDownLatch(3);

        // more listeners on same topic
        bus.on($(topic), (Event<String> ev) -> {
            String s = ev.getData();
            log(MODULE_A, "New topic event: " + s);
            latch.countDown();
        });
        bus.on($(topic), (Event<String> ev) -> {
            String s = ev.getData();
            log(MODULE_B, "New topic event: " + s);
            latch.countDown();
        });
        bus.on($(topic), (Event<String> ev) -> {
            String s = ev.getData();
            log(MODULE_C, "New topic event: " + s);
            latch.countDown();
        });

        bus.notify(topic, Event.wrap("Hello World!"));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    //
    // ## listen on Publisher
    //

    @Test
    public void testHelloWorldPublisher() {
        final EventBus bus = EventBus.create();

        // subscribe on "topic" Publisher
        bus.on($("topic")).subscribe(new Subscriber<Event<?>>() {
            @Override
            public void onSubscribe(Subscription s) {
            }

            @Override
            public void onNext(Event<?> ev) {
                Object s = ev.getData();
                log(MODULE_A, "New topic event: " + s);
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
            }
        });

        // publish new event into "topic"
        bus.notify("topic", Event.wrap("Hello World!"));
    }

    @Test
    public void testHelloWorldPublisherAsStream() {
        final EventBus bus = EventBus.create();

        // subscribe on "topic" Publisher
        Streams.wrap(bus.on($("topic"))).consume(ev -> {
            Object s = ev.getData();
            log(MODULE_A, "New topic event: " + s);
        });

        // publish new event into "topic"
        bus.notify("topic", Event.wrap("Hello World!"));
    }

    @Test
    public void testHelloWorldPublisherFilter() {
        final EventBus bus = EventBus.create();

        // subscribe on "topic" Publisher and FILTER then
        Streams.wrap((Publisher<Event<String>>) bus.on($("topic")))
                .filter(ev -> ev.getData().contains("Cruel"))
                .consume(ev -> {
                    Object s = ev.getData();
                    log(MODULE_A, "New topic event: " + s);
                });

        // this event is NOT received
        bus.notify("topic", Event.wrap("Hello World!"));

        // this one IS
        bus.notify("topic", Event.wrap("Cruel World!"));
    }

    //
    // junit
    //

    @Before
    public void before() {
        System.out.println("++++++++++++++++++++++++++++++++++++++++++");
    }

    @After
    public void after() {
        System.out.println("==========================================");
    }

    //
    // utils
    //

    private static void log(String prefix, String msg) {
        System.out.printf("%s<[ %s ]> %s%n", prefix, Thread.currentThread().getName(), msg);
    }

}
