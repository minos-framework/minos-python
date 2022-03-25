import unittest
from datetime import (
    datetime,
    time,
    timedelta,
    timezone,
)
from math import (
    inf,
)
from unittest.mock import (
    MagicMock,
    call,
    patch,
)

from crontab import CronTab as CrontabImpl

from minos.common import (
    current_datetime,
)
from minos.networks import (
    CronTab,
)


class TestCronTab(unittest.IsolatedAsyncioTestCase):
    def test_constructor(self):
        crontab = CronTab("@daily")
        self.assertEqual(CrontabImpl("@daily").matchers, crontab.impl.matchers)

    def test_constructor_reboot(self):
        crontab = CronTab("@reboot")
        self.assertEqual(None, crontab.impl)

    def test_constructor_raises(self):
        with self.assertRaises(ValueError):
            CronTab("foo")

    def test_repetitions(self):
        crontab = CronTab("@daily")
        self.assertEqual(inf, crontab.repetitions)

    def test_repetitions_reboot(self):
        crontab = CronTab("@reboot")
        self.assertEqual(1, crontab.repetitions)

    def test_get_delay_until_next(self):
        crontab = CronTab("@daily")
        now = current_datetime()

        expected = (
            datetime.combine(now.date() + timedelta(days=1), time.min, tzinfo=timezone.utc) - now
        ).total_seconds()
        self.assertAlmostEqual(expected, crontab.get_delay_until_next(), places=1)

    def test_get_delay_until_next_reboot(self):
        crontab = CronTab("@reboot")
        self.assertEqual(0, crontab.get_delay_until_next())

    def test_hash(self):
        crontab = CronTab("@daily")
        self.assertIsInstance(hash(crontab), int)

    def test_hash_reboot(self):
        crontab = CronTab("@reboot")
        self.assertIsInstance(hash(crontab), int)

    def test_eq(self):
        base = CronTab("@daily")
        one = CronTab("@daily")
        self.assertEqual(base, one)

        two = CronTab("@hourly")
        self.assertNotEqual(base, two)

        three = CronTab("@reboot")
        self.assertNotEqual(base, three)

    async def test_sleep_until_next(self):
        crontab = CronTab("@reboot")

        mock = MagicMock(return_value=1234)
        crontab.get_delay_until_next = mock

        with patch("asyncio.sleep") as mock_sleep:
            await crontab.sleep_until_next()

        self.assertEqual([call(1234)], mock_sleep.call_args_list)

    async def test_aiter(self):
        crontab = CronTab("@reboot")

        with patch("asyncio.sleep") as mock_sleep:
            count = 0
            async for now in crontab:
                count += 1
                self.assertAlmostEqual(current_datetime(), now, delta=timedelta(seconds=1))
            self.assertEqual(1, count)

        self.assertEqual([call(0), call(inf)], mock_sleep.call_args_list)


if __name__ == "__main__":
    unittest.main()
