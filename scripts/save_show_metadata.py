import polars as pl
from datetime import date

# TODO - Query the TodayTix API for this information!

show_list = pl.DataFrame({
    "show_id": ["42462", "41707"],
    "show_name": ["A Christmas Carol (ish)", "Oedipus"],
    "starting_date": [date(2024, 11, 16), date(2024, 10, 4)],
    "closing_date": [date(2024, 12, 31), date(2025, 1, 4)],
    "venue_id": ["54939502", "125"],
    "venue_name": ["Soho Place", "Wyndham's Theatre"]
})


show_list.write_csv("show_metadata.csv")
