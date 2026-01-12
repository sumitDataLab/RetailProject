import pytest
from lib.DataReader import read_customers, read_orders
from lib.Utils import get_spark_session
from lib.DataManipulation import filter_closed_orders,count_orders_state,count_generic_orders
from lib.ConfigReader import get_app_config

@pytest.mark.datareader()
def test_get_read_customers(spark):
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 12435

def test_get_read_orders(spark):
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68884

@pytest.mark.transformation()
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark,"LOCAL")
    closed_orders_count = filter_closed_orders(orders_df).count()
    assert closed_orders_count == 7556

@pytest.mark.skip("work in progress")
def test_app_config(spark):
    config = get_app_config("LOCAL")
    assert config['orders.file.path'] == 'data/orders.csv'

@pytest.mark.slow()
def test_count_agg_state(spark,expected_result):
    customers_df = read_customers(spark,"LOCAL")
    actual_results = count_orders_state(customers_df)
    assert actual_results.collect() == expected_result.collect()

@pytest.mark.skip()
def test_closed_orders_count(spark):
    orders_df = read_orders(spark,"LOCAL")
    closed_orders_count = count_generic_orders(orders_df,"CLOSED").count()
    assert closed_orders_count == 7556

@pytest.mark.skip()
def test_complete_orders_count(spark):
    orders_df = read_orders(spark,"LOCAL")
    closed_orders_count = count_generic_orders(orders_df,"COMPLETE").count()
    assert closed_orders_count == 22900

@pytest.mark.skip()
def test_pending_orders_count(spark):
    orders_df = read_orders(spark,"LOCAL")
    closed_orders_count = count_generic_orders(orders_df,"PENDING_PAYMENT").count()
    assert closed_orders_count == 15030

@pytest.mark.parametrize(
    "status,count",
    [("CLOSED",7556),
    ("COMPLETE",22900),
    ("PENDING_PAYMENT",15030)
    ]
)
def test_generic_orders_count(spark,status,count):
    orders_df = read_orders(spark,"LOCAL")
    generic_orders_count = count_generic_orders(orders_df,status).count()
    assert generic_orders_count == count