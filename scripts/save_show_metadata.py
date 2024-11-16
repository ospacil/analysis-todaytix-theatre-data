import polars as pl
from datetime import date

# TODO - Query the TodayTix API for this information!

show_list = pl.DataFrame({
    "show_id": ["42462", "41707", "43262"],
    "show_name": ["A Christmas Carol (ish)", "Oedipus", "Unicorn"],
    "starting_date": [date(2024, 11, 16), date(2024, 10, 4), date(2025, 2, 4)],
    "closing_date": [date(2024, 12, 31), date(2025, 1, 4), date(2025, 4, 26)],
    "venue_id": ["54939502", "125", "117"],
    "venue_name": ["Soho Place", "Wyndham's Theatre", "Garrick Theatre"]
})


show_list.write_csv("show_metadata.csv")
