import pytest
from lib.Utils import get_spark_session
import data.test_result
@pytest.fixture
def spark():
    "Create spark session"
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()

@pytest.fixture
def expected_result(spark):
    "State Count Expected Result"
    state_schema = "state string, count int"
    return spark.read \
        .format("csv") \
        .schema(state_schema) \
        .load("data/test_result/state_aggregate.csv")