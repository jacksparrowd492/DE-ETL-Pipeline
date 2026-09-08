"""
test_consumer.py
-----------------
Mock tests for kafka1/consumer.py. consume() takes its Kafka consumer as a
parameter, so tests pass in a plain list of fake messages instead of ever
opening a real connection to a broker or a real Databricks connection
(load_to_staging/promote_to_warehouse/create_*_table are all patched).
"""

from kafka1.consumer import consume, get_consumer


class _FakeMessage:
    def __init__(self, value):
        self.value = value


class _FakeConsumer:
    """A minimal stand-in for kafka.KafkaConsumer: iterable once, closeable."""

    def __init__(self, messages, raise_after=None):
        self._messages = messages
        self._raise_after = raise_after
        self.closed = False

    def __iter__(self):
        for i, message in enumerate(self._messages):
            if self._raise_after is not None and i == self._raise_after:
                raise RuntimeError("simulated consumer failure")
            yield message

    def close(self):
        self.closed = True


def _patch_pipeline(mocker):
    """Patches every downstream call consume() makes, returning the mocks."""
    return {
        "create_staging_table": mocker.patch("kafka1.consumer.create_staging_table"),
        "create_products_table": mocker.patch("kafka1.consumer.create_products_table"),
        "load_to_staging": mocker.patch("kafka1.consumer.load_to_staging"),
        "promote_to_warehouse": mocker.patch("kafka1.consumer.promote_to_warehouse"),
    }


def test_consume_processes_each_message_through_the_full_chain(mocker, raw_product):
    mocks = _patch_pipeline(mocker)
    fake_consumer = _FakeConsumer([_FakeMessage(raw_product)])

    consume(consumer=fake_consumer)

    mocks["create_staging_table"].assert_called_once()
    mocks["create_products_table"].assert_called_once()
    mocks["load_to_staging"].assert_called_once()
    mocks["promote_to_warehouse"].assert_called_once()

    transformed = mocks["load_to_staging"].call_args.args[0]
    assert transformed["product_id"] == raw_product["id"]
    assert mocks["promote_to_warehouse"].call_args.args[0] is transformed


def test_consume_processes_multiple_messages_in_order(mocker, raw_product):
    mocks = _patch_pipeline(mocker)
    second = dict(raw_product, id=2, title="Second Product")
    fake_consumer = _FakeConsumer([_FakeMessage(raw_product), _FakeMessage(second)])

    consume(consumer=fake_consumer)

    assert mocks["load_to_staging"].call_count == 2
    processed_ids = [call.args[0]["product_id"] for call in mocks["load_to_staging"].call_args_list]
    assert processed_ids == [1, 2]


def test_consume_closes_consumer_even_on_error(mocker, raw_product):
    _patch_pipeline(mocker)
    fake_consumer = _FakeConsumer([_FakeMessage(raw_product)], raise_after=0)

    consume(consumer=fake_consumer)  # the broad except Exception logs and continues

    assert fake_consumer.closed is True


def test_consume_closes_consumer_on_keyboard_interrupt(mocker, raw_product):
    _patch_pipeline(mocker)

    class _InterruptingConsumer(_FakeConsumer):
        def __iter__(self):
            raise KeyboardInterrupt

    fake_consumer = _InterruptingConsumer([])

    consume(consumer=fake_consumer)  # must not propagate KeyboardInterrupt

    assert fake_consumer.closed is True


def test_consume_continues_when_a_single_product_fails_to_load(mocker, raw_product):
    """load_to_staging raising for one message is caught by the loop's broad
    except, so the consumer still closes cleanly rather than crashing."""
    mocks = _patch_pipeline(mocker)
    mocks["load_to_staging"].side_effect = RuntimeError("Databricks write failed")
    fake_consumer = _FakeConsumer([_FakeMessage(raw_product)])

    consume(consumer=fake_consumer)

    assert fake_consumer.closed is True


def test_get_consumer_is_lazy_and_cached(mocker):
    """Importing/calling into this module must never touch a real broker;
    get_consumer() should build the KafkaConsumer once and reuse it."""
    fake_kafka_consumer_cls = mocker.patch("kafka1.consumer.KafkaConsumer")
    mocker.patch("kafka1.consumer._consumer", None)

    first = get_consumer()
    second = get_consumer()

    fake_kafka_consumer_cls.assert_called_once()
    assert first is second
