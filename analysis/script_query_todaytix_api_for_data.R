library(httr2)
library(glue)
library(jsonlite)
library(tibble)
library(purrr)
library(readr)
library(tidyr)
library(dplyr)


# Parameters
PRODUCT_ID <- "41271"
DATE_START <- "20241010"
DATE_END <- "20241015"

# The query URL
URL_AVAILABILITY <- glue("https://inventory-service.tixuk.io/api/v4/availability/products/{PRODUCT_ID}/quantity/1/from/{DATE_START}/to/{DATE_END}/detailed")

# Build the API reques, and perform
api_request <- request(URL_AVAILABILITY) |>
  req_headers(affiliateId = "<SECRET>")

api_response <- req_perform(api_request)

# Parse the response, and extract the main piece of information into a nice tibble (work in progress)
content <- resp_body_json(api_response)

pokus <- content$response$availability
pokus3 <- tibble(performanceType = map_chr(pokus, "performanceType", .default=NA_character_),
                 minPrice = map_int(pokus, "minPrice", .default=NA_integer_),
                 maxPrice = map_int(pokus, "maxPrice", .default=NA_integer_))




