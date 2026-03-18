from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import List, Sequence, Tuple, Dict, Union, Optional, Iterator

from dateutil.relativedelta import relativedelta
from dsp.shared.common import parse_timestamp
from dsp.shared.constants import FileTypeTypes


class CalendarException(ValueError):
    pass


@dataclass(frozen=True)
class SubmissionWindowConfig:
    start_day: int = field(default=1)
    end_day: int = field(default=31)
    windows_per_rp: int = field(default=1)
    ytd: bool = field(default=False)
    end_add_months: int = field(default=0)
    mid_window: bool = field(default=False)


@dataclass(frozen=True)
class ReportingPeriod:
    start: date
    end: date = None
    unique_month_id: int = field(init=False)

    def __post_init__(self):

        if self.start.day != 1:
            raise CalendarException("reporting period start must be the first day of the month")

        if not self.end:
            # default the end date to the end of the month
            # note relativedelta(day=31) sets to the last day of the month .. regardless of how many days
            # have to use private setattr because the class is frozen
            object.__setattr__(self, 'end', self.start + relativedelta(day=31))

        object.__setattr__(self, 'unique_month_id', calculate_uniq_month_id_from_date(self.start))


@dataclass(frozen=True)
class SubmissionWindow:
    opens: date
    closes: date
    reporting_periods: Sequence[ReportingPeriod]
    closed_as_at: datetime = field(init=False)
    last_possible_submission_time: datetime = field(init=False)

    def __post_init__(self):
        rps = list(self.reporting_periods or [])
        rps.sort(key=lambda rp: rp.start)

        object.__setattr__(self, 'reporting_periods', tuple(rps))
        object.__setattr__(
            self, 'closed_as_at', datetime.combine(self.closes + relativedelta(days=1), datetime.min.time())
        )
        object.__setattr__(
            self, 'last_possible_submission_time', datetime.combine(self.closes, datetime.max.time())
        )

        assert len(set([rp.start for rp in rps])) == len(rps), "rp start dates are not unique"
        assert len(set([rp.end for rp in rps])) == len(rps), "rp end dates are not unique"

        # declaration guards
        if self.opens >= self.closes:
            raise CalendarException("submission window must open before it closes")

    @property
    def is_open(self) -> bool:
        now = datetime.today().date()
        return self.opens <= now <= self.closes

    @staticmethod
    def from_dates(opens: date, closes: date, rp_starts: Sequence[date]):
        assert opens
        assert closes
        rp_starts = list(rp_starts or [])
        rp_starts.sort()
        return SubmissionWindow(
            opens=opens,
            closes=closes,
            reporting_periods=[
                ReportingPeriod(start=sd) for sd in rp_starts
            ]
        )

    @property
    def is_primary_refresh(self) -> bool:
        return len(self.reporting_periods) > 1

    @property
    def earliest_rp(self):
        return self.reporting_periods[0]

    @property
    def last_rp(self):
        return self.reporting_periods[-1]


@dataclass(frozen=True)
class SubmissionCalendar:
    dataset_id: str
    submission_windows: Sequence[SubmissionWindow]
    window_config: SubmissionWindowConfig

    rp_start_maps: Dict[date, Sequence[SubmissionWindow]] = field(init=False)

    def __post_init__(self):

        submission_windows = list(self.submission_windows)  # type: List[SubmissionWindow]
        submission_windows.sort(key=lambda w: w.opens)

        rp_start_maps = defaultdict(list)

        last = None
        for sw in submission_windows:
            if last is not None and (sw.opens <= last.opens or sw.closes <= last.opens):
                raise CalendarException(
                    f'{self.dataset_id.upper()} submission window seem out of order {last} {sw}'
                )

            last = sw
            for rp in sw.reporting_periods:
                rp_start_maps[rp.start].append(sw)

        # For the ytd calendar any overlap between a currently active reporting period and
        # the submission window needs removing - assumes reporting periods end on month end
        if self.window_config.ytd:
            for reporting_period, rp_sws in rp_start_maps.items():
                if rp_sws[0].opens.month == reporting_period.month and rp_sws[0].opens.year == reporting_period.year:
                    rp_sws[0] = SubmissionWindow.from_dates(
                        opens=reporting_period + relativedelta(months=+1),
                        closes=rp_sws[0].closes,
                        rp_starts=[rp.start for rp in rp_sws[0].reporting_periods]
                    )

        rp_start_maps = {
            k: tuple(v) for k, v in rp_start_maps.items()
        }

        object.__setattr__(self, 'rp_start_maps', rp_start_maps)

    @classmethod
    def from_dates(cls, dataset_id: str, window_config: SubmissionWindowConfig,
                   dates: Sequence[Tuple[date, date, Optional[Sequence[date]]]]
                   ):
        submission_windows = []
        for opens, closes, rp_starts in dates:
            # if RP starts is not None (RP starts are hardcoded in the calendar module)
            # it allows us to override YTD generation if needed.
            if rp_starts is None and window_config.ytd:
                rp_starts = cls.get_ytd_rp_starts_from_sw_closes(closes)
            submission_windows.append(SubmissionWindow.from_dates(opens, closes, rp_starts))

        return cls(
            dataset_id=dataset_id,
            submission_windows=submission_windows,
            window_config=window_config
        )

    @staticmethod
    def get_ytd_rp_starts_from_sw_closes(sw_closes: date) -> Sequence[date]:
        rp_starts = []
        num_of_rps = SubmissionCalendar.ytd_num_rps_for_sw(sw_closes)
        # generating reporting periods (rp) in reverse, so rps are indexed correctly in their array
        # allowing latest and earliest properties to function correctly.
        for i in range(num_of_rps, 0, -1):
            rp_starts.append(round_down_to_beginning_of_month(sw_closes + relativedelta(months=-i)))

        return rp_starts

    def fake_windows_for_rp(self, rp_start_date: date) -> Sequence[SubmissionWindow]:
        cfg = self.window_config

        # Generate month offsets based on submission window and reporting period start dates, respectively
        def offsets_of_sws_relative_to_rp(start_date_rp: date) -> Iterator[int]:
            return range(self.num_sw_for_rp(start_date_rp))

        def offsets_of_rps_relative_to_sw(end_date_sw: date) -> Iterator[int]:
            return range(self.num_rps_for_sw(end_date_sw))

        # Generate submission window and reporting period start dates taking offsets as arguments
        def get_sw_start_date(sw_offset: int) -> date:
            sw_additional_month_offset = 0 if cfg.ytd else 1
            return rp_start_date + relativedelta(months=(sw_additional_month_offset + sw_offset), day=cfg.start_day)

        def get_sw_end_date(sw_offset: int) -> date:
            return rp_start_date + relativedelta(months=(1 + sw_offset + cfg.end_add_months), day=cfg.end_day)

        def get_rp_start_date(sw_offset: int, rp_offset: int) -> date:
            return rp_start_date + relativedelta(months=sw_offset - rp_offset)

        # Loop through each submission window for the reporting period, generate the set of valid reporting
        # periods for that submission window
        windows = [
            SubmissionWindow.from_dates(
                opens=get_sw_start_date(sw_offset),
                closes=get_sw_end_date(sw_offset),
                rp_starts=[
                    get_rp_start_date(sw_offset, rp_offset)
                    for rp_offset in offsets_of_rps_relative_to_sw(get_sw_end_date(sw_offset))
                ]
            ) for sw_offset in offsets_of_sws_relative_to_rp(rp_start_date)
        ]
        return tuple(windows)

    def num_rps_for_sw(self, sw_closes: date):
        if not self.window_config.ytd:
            return self.window_config.windows_per_rp

        return self.ytd_num_rps_for_sw(sw_closes)

    @staticmethod
    def ytd_num_rps_for_sw(sw_closes: date):
        april = date(sw_closes.year - (1 if sw_closes.month < 6 else 0), 4, 1)
        delta = relativedelta(date(sw_closes.year, sw_closes.month, 1), april)
        months = delta.months + 12 * delta.years

        return months

    def num_sw_for_rp(self, rp_start: date):
        if not self.window_config.ytd:
            return self.window_config.windows_per_rp

        april = financial_year_end_date_as_at(rp_start)
        delta = relativedelta(april, rp_start)
        months = 1 + delta.months + 12 * delta.years

        return months

    def fake_last_closed_window_as_at(self, as_at: date) -> SubmissionWindow:
        cfg = self.window_config

        closes = date(as_at.year, as_at.month, 1) + relativedelta(day=cfg.end_day)
        if as_at.day <= closes.day:
            closes = closes + relativedelta(months=-1, day=cfg.end_day)

        ytd_month_offset = 1 if cfg.ytd else 0

        opens = closes + relativedelta(months=-1 * (cfg.end_add_months + ytd_month_offset), day=cfg.start_day)
        primary_rp = opens + relativedelta(months=-1 + ytd_month_offset, day=1)

        return SubmissionWindow.from_dates(
            opens=opens,
            closes=closes,
            rp_starts=[
                (primary_rp + relativedelta(months=-1 * ix2, day=1))
                for ix2 in range(self.num_rps_for_sw(closes))
            ]
        )

    def fake_msds_earliest_active_window_as_at(self, as_at: date) -> SubmissionWindow:
        cfg = self.window_config

        closes = date(as_at.year, as_at.month, 1) + relativedelta(day=cfg.end_day)

        ytd_month_offset = 1 if cfg.ytd else 0

        opens = closes + relativedelta(months=-1 * (cfg.end_add_months + ytd_month_offset), day=cfg.start_day)
        primary_rp = opens + relativedelta(months=-1 + ytd_month_offset, day=1)

        return SubmissionWindow.from_dates(
            opens=opens,
            closes=closes,
            rp_starts=[
                (primary_rp + relativedelta(months=-1 * ix2, day=1))
                for ix2 in range(self.num_rps_for_sw(closes))
            ]
        )

    def submitted_within_submission_window(
            self, submission_datetime: datetime, reporting_period_start: date, fake_historic_windows: bool
    ) -> bool:
        submission_date = submission_datetime.date()

        rp_windows = self.get_sws_for_rp(reporting_period_start, fake_historic_windows)

        for sw in rp_windows:
            if sw and sw.opens <= submission_date <= sw.closes:
                return True

        return False

    def find_last_closed_submission_window_for_rp(
            self, reporting_period_start: date, as_at: Union[date, datetime], fake_historic_windows: bool
    ) -> SubmissionWindow:

        as_at_date = as_at.date() if isinstance(as_at, datetime) else as_at

        rp_windows = self.get_sws_for_rp(reporting_period_start, fake_historic_windows)

        for sw in reversed(rp_windows):
            if sw and sw.closes < as_at_date:
                return sw

        raise CalendarException(
            f"{self.dataset_id.upper()} no submission window for {reporting_period_start} as at {as_at_date}"
        )

    def find_last_closed_submission_window(
            self, as_at_date: Union[str, date, datetime], fake_historic_windows: bool
    ) -> SubmissionWindow:

        as_at_date = parse_timestamp(as_at_date).date()

        if self.submission_windows[-1].opens <= as_at_date:
            raise CalendarException(
                f"{self.dataset_id.upper()} no closed submission window found for as_at_date={as_at_date}"
            )

        for window in reversed(self.submission_windows):

            if window.closes >= as_at_date:
                continue

            return window

        if not fake_historic_windows:
            raise CalendarException(
                f"{self.dataset_id.upper()} no closed submission window found for as_at_date={as_at_date}"
            )

        return self.fake_last_closed_window_as_at(as_at_date)

    def find_msds_earliest_active_submission_window(
            self, as_at_date: Union[str, date, datetime], fake_historic_windows: bool
    ) -> SubmissionWindow:

        as_at_date = parse_timestamp(as_at_date).date()

        if self.submission_windows[-1].closes < as_at_date:
            raise CalendarException(
                f"{self.dataset_id.upper()} no open submission window found for as_at_date={as_at_date}"
            )

        for window in reversed(self.submission_windows):

            if window.opens >= as_at_date or window.opens + relativedelta(months=1) > as_at_date:
                continue

            return window

        if not fake_historic_windows:
            raise CalendarException(
                f"{self.dataset_id.upper()} no open submission window found for as_at_date={as_at_date}"
            )

        return self.fake_msds_earliest_active_window_as_at(as_at_date)

    def find_msds_submission_window_at_rp(
            self, reporting_period_start: date, as_at_date: Union[str, date, datetime], fake_historic_windows: bool
    ) -> SubmissionWindow:

        as_at_date = parse_timestamp(as_at_date).date()

        if self.submission_windows[-1].closes <= as_at_date:
            raise CalendarException(
                f"{self.dataset_id.upper()} no valid submission window found for {reporting_period_start} as_at={as_at_date}"
            )

        for window in reversed(self.submission_windows):
            if window.earliest_rp.start == reporting_period_start and window.closes < as_at_date:
                return window
            if window.earliest_rp.start == reporting_period_start and window.closes > as_at_date and window.opens < as_at_date:
                return window

        if not fake_historic_windows:
            raise CalendarException(
                f"{self.dataset_id.upper()} no valid submission window found for {reporting_period_start} as_at={as_at_date}"
            )

        return self.fake_msds_earliest_active_window_as_at(as_at_date)

    def get_sws_for_rp(
            self, rp_start_date: date, fake_historic_windows: bool
    ) -> Sequence[Union[None, SubmissionWindow]]:

        submission_windows = self.rp_start_maps.get(rp_start_date, [])
        expected_rps = self.num_sw_for_rp(rp_start_date)

        if len(submission_windows) < expected_rps:
            if not fake_historic_windows:
                return tuple(
                    [None for _x in range(expected_rps - len(submission_windows))] + list(submission_windows)
                )

            fake_windows = self.fake_windows_for_rp(rp_start_date)

            return tuple(
                list(fake_windows[:expected_rps - len(submission_windows)]) + list(submission_windows)
            )
        return submission_windows

    def find_submission_window(
            self, reporting_period_start: date, fake_historic_windows: bool,
            months_offset: int = 0
    ) -> SubmissionWindow:

        if reporting_period_start.day != 1:
            raise CalendarException(
                f"reporting periods start on the first for {self.dataset_id.upper()} submission calendar"
            )

        rp_windows = self.get_sws_for_rp(reporting_period_start, fake_historic_windows)

        if (months_offset + 1) > len(rp_windows):
            raise CalendarException(
                f"invalid offset for {self.dataset_id.upper()} submission calendar max: {len(rp_windows) - 1}"
            )

        sw = rp_windows[months_offset]

        if sw is None:
            raise CalendarException(f"the given month is not in the {self.dataset_id.upper()} submission calendar")

        return rp_windows[months_offset]

    def get_submission_window_type(
            self, reporting_period_start: date, submission_datetime: datetime,
            fake_historic_windows: bool = False, snap_to_last_closed: bool = False,
            allow_pre_sw: bool = False
    ) -> str:
        """ get the submission window type

        Args:
            reporting_period_start (date): reporting period start date
            fake_historic_windows (bool): fake historic windows if none found
            submission_datetime (datetime): the submission date
            snap_to_last_closed (bool):  if between primary and first refresh, snap to primary
            (should not be used for file type derivation)

        Returns:
            Name of submission window type which will be used, for example, in name of commissioner extract files (str)
        """

        rp_windows = self.get_sws_for_rp(reporting_period_start, fake_historic_windows)

        if rp_windows[0] is None:
            raise CalendarException(
                f"unable find submission windows for {self.dataset_id.upper()} {reporting_period_start}"
            )

        submission_date = submission_datetime.date()
        if submission_date < rp_windows[0].opens:
            if allow_pre_sw:
                return "undefined"

            raise CalendarException(
                "the as at date cannot precede the primary reporting "
                "period opens, as no submission windows can be selected."
            )

        if self.window_config.mid_window:
            if submission_date <= rp_windows[0].closes:
                return FileTypeTypes.MID_WINDOW
            else:
                return FileTypeTypes.POST_DEADLINE
        else:
            if len(rp_windows) == 1 or submission_date <= rp_windows[0].closes:
                return FileTypeTypes.PRIMARY

        if snap_to_last_closed and (
                submission_date < rp_windows[1].opens
                or submission_datetime == datetime.combine(rp_windows[1].opens, datetime.min.time())
        ):
            return FileTypeTypes.PRIMARY

        return FileTypeTypes.REFRESH

    def get_submission_num_sws_since_ytd_rp_start(
            self, reporting_period_start: date, submission_datetime: datetime,
            fake_historic_windows: bool = False
    ) -> int:
        """

        Gets the number of submission windows that have passed for the given submission since the start of the
        reporting period.
        Value returned is clipped to a minimum of 1 month, up to a maximum of 13 months.

        Args:
            reporting_period_start (date): reporting period start date
            fake_historic_windows (bool): fake historic windows if none found
            submission_datetime (datetime): the submission date

        Returns:
            int: The number of submission windows elapsed since the start of the reporting period
        """
        rp_windows = list(filter(None, self.get_sws_for_rp(reporting_period_start, fake_historic_windows)))
        if not rp_windows:
            raise CalendarException(
                f"unable to find submission windows for {self.dataset_id.upper()} {reporting_period_start}"
            )

        submission_date = submission_datetime.date()

        total_sws = sum([1 for window in rp_windows if window.opens <= submission_date])

        return min(max(total_sws, 1), 13)

    def find_submission_window_by_submission_date(
            self, submission_date: date, fake_historic_windows: bool = False,
            snap_to_last_open_window: bool = False
    ) -> SubmissionWindow:
        if submission_date < self.submission_windows[0].opens:

            if not fake_historic_windows:
                raise CalendarException(
                    f"the given month is not in the {self.dataset_id.upper()} submission calendar "
                    "or the submission date falls outside a submission window"
                )

            return self.fake_last_closed_window_as_at(submission_date + relativedelta(months=1, day=15))

        last_window = None
        for window in self.submission_windows:

            if window.opens <= submission_date <= window.closes:
                return window

            if snap_to_last_open_window and last_window and submission_date < window.opens:
                return last_window

            last_window = window

        raise CalendarException(
            f"the given month is not in the {self.dataset_id.upper()} submission calendar "
            "or the submission date falls outside a submission window"
        )

    def get_earliest_unique_month_id_and_rp_start_as_at(self,
                                                        as_at: Union[datetime, date, str, int],
                                                        fake_historic_windows: bool
                                                        ) -> Tuple[int, date]:

        as_at = parse_timestamp(as_at)

        sw = self.find_last_closed_submission_window(as_at,
                                                     fake_historic_windows=fake_historic_windows)

        rp = sw.earliest_rp
        return rp.unique_month_id, rp.start

    def get_latest_unique_month_id_and_rp_start_as_at(self, as_at: Union[datetime, date, str, int],
                                                      fake_historic_windows: bool) -> Tuple[int, date]:

        as_at = parse_timestamp(as_at)

        sw = self.find_last_closed_submission_window(
            as_at, fake_historic_windows=fake_historic_windows
        )

        rp = sw.last_rp
        return rp.unique_month_id, rp.start

    def get_primary_unique_month_id_and_rp_start_as_at(self, as_at: Union[datetime, date, str, int],
                                                       fake_historic_windows: bool) -> Tuple[int, date]:

        as_at = parse_timestamp(as_at)

        sw = self.find_last_closed_submission_window(
            as_at, fake_historic_windows=fake_historic_windows
        )

        if not sw.is_primary_refresh:
            raise CalendarException(f"Calendar for {self.dataset_id.upper()} does not seem to be primary refresh")

        rp = sw.last_rp
        return rp.unique_month_id, rp.start

    def get_refresh_unique_month_id_and_rp_start_as_at(self, as_at: Union[datetime, date, str, int],
                                                       fake_historic_windows: bool) -> Tuple[int, date]:

        if self.window_config.ytd:
            raise CalendarException(
                f'Calendar for {self.dataset_id.upper()} is YTD, multiple refresh windows available'
            )

        as_at = parse_timestamp(as_at)

        sw = self.find_last_closed_submission_window(
            as_at, fake_historic_windows=fake_historic_windows
        )
        if not sw.is_primary_refresh:
            raise CalendarException(f"Calendar for {self.dataset_id.upper()} does not seem to be primary refresh")

        rp = sw.earliest_rp
        return rp.unique_month_id, rp.start

    def find_submission_windows(self, reporting_period_start: date) -> Sequence[SubmissionWindow]:
        """
            get all submission windows for a reporting period
        Args:
            reporting_period_start (date): reporting period start date

        Returns:
            Sequence[SubmissionWindow]
        """

        return self.rp_start_maps.get(reporting_period_start) or []


def calculate_uniq_month_id_from_date(month_date: date) -> int:
    """
    Calculate the uniq month ID - number of months since April 1900 +1

    Args:
        month_date:

    Returns: the month ID of the date parameter (ignores the day of month
    """

    start_date = datetime(1900, 4, 1)
    time_diff = relativedelta(month_date, start_date)
    unique_month_id = time_diff.years * 12 + time_diff.months + 1

    return unique_month_id


def round_down_to_beginning_of_month(from_date: date) -> date:
    """
    Round a date to the beginning of the month so that it represents a valid reporting period start date

    Args:
        from_date: the date to round down

    Returns: a new date based on from_date but with the day set to 1
    """
    return from_date.replace(day=1)


def financial_year_end_date_as_at(at_date: date):
    return date(at_date.year + (1 if at_date.month > 3 else 0), 4, 1)


def financial_year_start_date_as_at(at_date: date):
    return date(at_date.year - (0 if at_date.month > 3 else 1), 4, 1)
