from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


@dataclass
class OlindaEndpoint:
    service: str
    version: str
    entity: str        # entity set name, or "Func()" for no-param function imports
    gcs_name: str
    frequency: str     # "monthly" | "quarterly"
    func_date_param: str | None = None  # function import param name that receives the date
    func_date_format: str = "%Y-%m-%d"  # strftime format for the function date value
    func_date_quoted: bool = True        # True: wrap in OData single quotes; False: bare int
    query_date_param: str | None = None  # query-string date filter param (entity sets only)
    odata_filter_param: str | None = None  # field name for OData $filter expression (e.g. "anoMes")
    odata_filter_format: str = "%Y-%m"     # strftime format for the $filter value
    split_filters: list[str] | None = None  # OData $filter expressions to run as separate slices, combined into one blob
    # When set, loop the function import from this date to api_date (one call per month) into one blob
    func_range_start: date | None = None
