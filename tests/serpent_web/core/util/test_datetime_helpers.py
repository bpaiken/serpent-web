from datetime import datetime, timezone

import pytest

from serpent_web.core.util.datetime_helpers import utc_now_time_aware

@pytest.mark.unit
def test_utc_now_time_aware():
    result = utc_now_time_aware()
    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc