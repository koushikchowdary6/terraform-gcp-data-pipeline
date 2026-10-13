"""Source definitions.

Each source is a real, openly licensed public CSV feed. A source declares how
its columns map onto the canonical long-format observation schema, which lets
one ingestion code path handle feeds with different shapes.

Both feeds are published by the Frictionless Data "datasets" project:
  - s-and-p-500 : monthly S&P 500 composite + fundamentals (Shiller dataset)
  - finance-vix : daily CBOE VIX index OHLC

Licensing: the datasets project publishes these under the Open Data Commons
Public Domain Dedication and Licence (PDDL). See docs/DATASETS.md.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType


@dataclass(frozen=True)
class SourceSpec:
    """Declarative description of one upstream CSV feed."""

    name: str
    series_id: str
    description: str
    url: str
    date_column: str
    # csv column name -> canonical metric_name
    metric_columns: Mapping[str, str]
    # Metrics where the feed writes 0.0 to mean "not reported yet" rather than
    # a true zero. Those are treated as missing instead of being loaded as 0.
    zero_means_missing: frozenset[str] = field(default_factory=frozenset)
    # Accepted date layouts, tried in order.
    date_formats: tuple[str, ...] = ("%Y-%m-%d", "%Y-%m")
    # Inclusive plausibility bounds applied to every metric of this source.
    min_value: float = -100.0
    max_value: float = 1_000_000.0

    @property
    def raw_filename(self) -> str:
        return f"{self.name}.csv"


SP500 = SourceSpec(
    name="sp500_monthly",
    series_id="SP500_COMPOSITE",
    description="S&P 500 composite monthly price and fundamentals (Shiller)",
    url="https://raw.githubusercontent.com/datasets/s-and-p-500/main/data/data.csv",
    date_column="Date",
    metric_columns=MappingProxyType(
        {
            "SP500": "index_level",
            "Dividend": "dividend",
            "Earnings": "earnings",
            "Consumer Price Index": "cpi",
            "Long Interest Rate": "long_interest_rate",
            "Real Price": "real_price",
            "Real Dividend": "real_dividend",
            "Real Earnings": "real_earnings",
            "PE10": "pe10",
        }
    ),
    # The upstream feed pads not-yet-reported fundamentals with 0.0 for recent
    # months. An index level or CPI of exactly 0 is not a real observation.
    zero_means_missing=frozenset(
        {
            "dividend",
            "earnings",
            "cpi",
            "real_price",
            "real_dividend",
            "real_earnings",
            "pe10",
            "index_level",
            # A 0.00% long-term rate has never been observed in this series;
            # the feed pads unreported recent months with 0.0 here too.
            "long_interest_rate",
        }
    ),
)

VIX = SourceSpec(
    name="vix_daily",
    series_id="CBOE_VIX",
    description="CBOE Volatility Index daily open/high/low/close",
    url="https://raw.githubusercontent.com/datasets/finance-vix/main/data/vix-daily.csv",
    date_column="DATE",
    metric_columns=MappingProxyType(
        {"OPEN": "open", "HIGH": "high", "LOW": "low", "CLOSE": "close"},
    ),
    # A VIX print of 0 has never occurred; treat it as missing if it appears.
    zero_means_missing=frozenset({"open", "high", "low", "close"}),
    min_value=0.0,
    max_value=500.0,
)

SOURCES: Mapping[str, SourceSpec] = MappingProxyType({s.name: s for s in (SP500, VIX)})


def get_source(name: str) -> SourceSpec:
    try:
        return SOURCES[name]
    except KeyError:
        raise KeyError(f"Unknown source {name!r}. Available: {', '.join(sorted(SOURCES))}") from None
