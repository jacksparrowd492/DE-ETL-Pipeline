"""
test_producer.py
-----------------
Mock tests for kafka1/producer.py. get_producer() is patched so no test
ever opens a real connection to a Kafka broker.
"""

from kafka.errors import KafkaError

from kafka1.producer import enrich_product, send_product, send_products, validate_product


# --------------------------------------------------------
# validate_product
# --------------------------------------------------------

def test_validate_product_accepts_well_formed_product(raw_product):
    assert validate_product(raw_product) is True


def test_validate_product_rejects_missing_field(raw_product):
    del raw_product["price"]
    assert validate_product(raw_product) is False


# --------------------------------------------------------
# enrich_product
# --------------------------------------------------------

def test_enrich_product_adds_batch_id_and_ingestion_time(raw_product):
    enriched = enrich_product(raw_product)

    assert "batch_id" in enriched
    assert "ingestion_time" in enriched
    assert enriched["id"] == raw_product["id"]


def test_enrich_product_does_not_mutate_original(raw_product):
    original = dict(raw_product)
    enrich_product(raw_product)

    assert raw_product == original


# --------------------------------------------------------
# send_product (mocks the Kafka producer)
# --------------------------------------------------------

def test_send_product_happy_path_returns_true(mocker, raw_product):
    fake_future = mocker.Mock()
    fake_future.get.return_value = mocker.Mock(partition=0, offset=42)

    fake_producer = mocker.Mock()
    fake_producer.send.return_value = fake_future

    mocker.patch("kafka1.producer.get_producer", return_value=fake_producer)

    assert send_product(raw_product) is True
    fake_producer.send.assert_called_once()


def test_send_product_skips_kafka_when_invalid(mocker):
    get_producer_mock = mocker.patch("kafka1.producer.get_producer")

    result = send_product({"id": 1, "title": "No Price"})  # missing required fields

    assert result is False
    get_producer_mock.assert_not_called()


def test_send_product_returns_false_on_kafka_error(mocker, raw_product):
    fake_producer = mocker.Mock()
    fake_producer.send.side_effect = KafkaError("broker unavailable")

    mocker.patch("kafka1.producer.get_producer", return_value=fake_producer)

    assert send_product(raw_product) is False


# --------------------------------------------------------
# send_products (batch)
# --------------------------------------------------------

def test_send_products_counts_success_and_failure(mocker, raw_product):
    # kafka1.producer.validate_product only checks key *presence* (unlike
    # validation/validator.py, which also rejects empty strings), so the
    # "invalid" record here needs a field missing entirely.
    missing_category = {"id": 2, "title": "No Category", "price": 5.0}
    batch = [raw_product, missing_category]

    fake_future = mocker.Mock()
    fake_future.get.return_value = mocker.Mock(partition=0, offset=1)

    fake_producer = mocker.Mock()
    fake_producer.send.return_value = fake_future

    mocker.patch("kafka1.producer.get_producer", return_value=fake_producer)

    send_products(batch)

    # Only the valid product should have reached the (mocked) broker.
    assert fake_producer.send.call_count == 1
    fake_producer.flush.assert_called_once()
