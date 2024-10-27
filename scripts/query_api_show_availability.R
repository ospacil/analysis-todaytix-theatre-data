library(here)
library(httr2)
library(glue)
library(tibble)
library(purrr)
library(dplyr)
library(nanoparquet)

## == SETUP / LOAD SECRETS =====================
dat_secrets <- read.csv(here("secrets.csv"))
AFFILIATE_ID <- dat_secrets[dat_secrets$secretName == "affiliateId"]$secretValue


## == DEFINE FUNCTIONS =========================

#' Query the API for show availability information
#' @param show_id The ID of the show
#' @param date_start The start date of the availability query
#' @param date_end The end date of the availability query
#' @param affiliate_id The client AffiliateID assigned by TTG
#' @return REST API response, or NULL if error
query_api_for_show_availability <- function(show_id, date_start, date_end, affiliate_id=AFFILIATE_ID) {
  # Form the query URL
  URL_AVAILABILITY <- glue("https://inventory-service.tixuk.io/api/v4/availability/products/{show_id}/quantity/1/from/{date_start}/to/{date_end}/detailed")

  # Build the API request, including headers
  api_request <- request(URL_AVAILABILITY) |>
    req_headers(affiliateId = AFFILIATE_ID)

  # Perform the API request
  tryCatch(res <- req_perform(api_request),
    error = \(e) {print(glue("Failed to query the API for show ID {show_id}")); print(e); res <- NULL})
}


#' Parse the API response into a tibble. The function currently expects the body of the API response
#' contains partiicular fields. It will break if that changes.
#' @param api_response The REST API response
#' @return A tibble
parse_api_response <- function(api_response) {

  if (!is.null(api_response)) {
        
    # Convert body of the response to JSON
    response_content_json <- resp_body_json(api_response)

    # Extract show availability information
    response_show_availability <- response_content_json$response$availability

    # ... and then use purrr:map to convert into a tibble
    dat_show_availability <- tibble(
      performanceTime = map_chr(response_show_availability, "datetime", .default=NA_character_),
      performanceType = map_chr(response_show_availability, "performanceType", .default=NA_character_),
      availableSeatCount = map_int(response_show_availability, "availableSeatCount", .default=NA_integer_),
      largestLumpOfTickets = map_int(response_show_availability, "largestLumpOfTickets", .default=NA_integer_),
      minPrice = map_int(response_show_availability, "minPrice", .default=NA_integer_),
      maxPrice = map_int(response_show_availability, "maxPrice", .default=NA_integer_),
      discountAvailable = map_lgl(response_show_availability, "discount", .default=NA),
      currency = map_chr(response_show_availability, "currency", .default=NA_character_)
    )

    # Also extract the requested show ID as recorded in the response
    response_show_id <- response_content_json$response$show
    # Also extract the request timestamp (as recorded in the response)
    response_timestamp <- as.POSIXct(resp_date(api_response))

    # Finally, form the final tibble
    dat_show_availability <- dat_show_availability |>
      mutate(
        performanceTime = as.POSIXct(performanceTime, tz="UTC", format="%Y-%m-%dT%H:%M:%S"),
        requestTime = response_timestamp,
        showId = response_show_id
    ) 
  } else {
    res <- NULL
  }
}


## == Query the API for a number of shows =============================
# A list of shows
show_list <- tribble(
  ~show_name, ~show_id, ~date_start, ~date_end,
  "A Christmas Carol(ish)", "42462", "20241101", "20241231",
  # "Test for error", "111", "20241101", "20241231",
  "Oedipus (by Robert Icke)", "41707", "20241101", "20241231"
)

# Query the API
api_response <- pmap(
  list(show_list$show_id, show_list$date_start, show_list$date_end),
  query_api_for_show_availability
)

# Parse the API response for all shows, and assemble into a single tibble
dat_parsed <- map(api_response, parse_api_response) |>
  list_rbind() |>
  as_tibble()

# Save on disk as parquet

query_execution_date <- max(dat_parsed$requestTime) |> as.Date()

write_parquet(dat_parsed,
  glue(here("data", "show-availability-query-date-{query_execution_date}.parquet")))


# ===================================
# # This is how one should be able to read all the files back into a tibble
files_to_read <- list.files(here("data/"), pattern="*.parquet", full.names=TRUE)
dat_read <- map(files_to_read, ~read_parquet(.)) |>
  list_rbind()|> 
  as_tibble()

