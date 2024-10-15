library(httr2)
library(glue)
library(tibble)
library(purrr)
library(tidyr)
library(dplyr)
library(here)

# -- Setup -----------------------

dat_secrets <- read.csv(here("secrets.csv"))
AFFILIATE_ID <- dat_secrets[dat_secrets$secretName == "affiliateId"]$secretValue

# -- Query the API----------------
# Parameters
PRODUCT_ID <- "42462" # A Christmas Carol(ish)
DATE_START <- "20241101"
DATE_END <- "20241231"

# The query URL
URL_AVAILABILITY <- glue("https://inventory-service.tixuk.io/api/v4/availability/products/{PRODUCT_ID}/quantity/1/from/{DATE_START}/to/{DATE_END}/detailed")

# Build the API request, and perform
api_request <- request(URL_AVAILABILITY) |>
  req_headers(affiliateId = AFFILIATE_ID)

# api_response <- req_perform(api_request)

# -- Parse the response, and extract the main piece of information ---
# Save the timestamp of the request as seen in the response
request_timestamp <- as.POSIXct(api_response$headers$`X-Client-Request-DateTime`, tz="UTC", 
                                format="%Y-%m-%dT%H:%M:%S")

# Convert body of the response to JSON
response_content_json <- resp_body_json(api_response)

# First extract the requested product ID (of course this should match PRODUCT_ID)
request_product_id <- response_content_json$response$show

# And then extract the show availability information into a tibble
# First, get the list of lists ...
product_availability <- response_content_json$response$availability

# ... and then use purrr:map to convert into a tibble
dat_product_availability <- tibble(
  performanceTime = map_chr(product_availability, "datetime", .default=NA_character_),
  performanceType = map_chr(product_availability, "performanceType", .default=NA_character_),
  availableSeatCount = map_int(product_availability, "availableSeatCount", .default=NA_integer_),
  largestLumpOfTickets = map_int(product_availability, "largestLumpOfTickets", .default=NA_integer_),
  minPrice = map_int(product_availability, "minPrice", .default=NA_integer_),
  maxPrice = map_int(product_availability, "maxPrice", .default=NA_integer_),
  discountAvailable = map_lgl(product_availability, "discount", .default=NA),
  currency = map_chr(product_availability, "currency", .default=NA_character_)
)

# Finally, re-format, and add the request timestamp
pokus <- dat_product_availability |>
  mutate(
    performanceTime = as.POSIXct(performanceTime, tz="UTC", format="%Y-%m-%dT%H:%M:%S"),
    requestTime = request_timestamp,
    productId = request_product_id
  )



